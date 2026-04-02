# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import json
import shutil
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .events import JobStatusEvent, LogEvent, LogLevel, ProgressEvent, StateEvent
from .models import JobSpec, JobState, JobStatus, JobStep
from .dependency_manager import (
    DependencyReporter,
    DependencyStatus,
    DependencyProgress,
    ensure_transcription_dependencies,
)
from src.config.settings_schema import (
    build_custom_subtitle_style,
    get_effective_subtitle_style,
)
from src.utils.video_namer import generate_video_name


EmitFn = Callable[[object], None]


class JobRunner:
    """
    Orquesta el pipeline usando las clases existentes pero emitiendo eventos
    UI-agnósticos para que Textual/CLI/GUI puedan observar el progreso.
    """

    def __init__(self, state_manager, emit: EmitFn, *, cli_output_dir: Optional[str] = None):
        self.state_manager = state_manager
        self.emit = emit
        self._cli_output_dir = cli_output_dir
        self._dependency_ok_cache: set[tuple[str, str, str, str]] = set()

    def run_job(self, job: JobSpec) -> JobStatus:
        status = JobStatus(progress_current=0, progress_total=len(job.video_ids) * max(1, len(job.steps)))
        status.mark_started()
        self.emit(JobStatusEvent(job_id=job.job_id, state=status.state))

        run_output_dir = self._ensure_run_output_dir(job_id=job.job_id, video_ids=job.video_ids)

        try:
            for video_id in job.video_ids:
                self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
                for step in job.steps:
                    status.label = f"{step.value} ({video_id})"
                    self.emit(
                        ProgressEvent(
                            job_id=job.job_id,
                            video_id=video_id,
                            current=status.progress_current,
                            total=status.progress_total,
                            label=status.label,
                        )
                    )
                    self._run_step(job_id=job.job_id, video_id=video_id, step=step, settings=job.settings, run_output_dir=run_output_dir)
                    status.progress_current += 1
                    self.emit(
                        ProgressEvent(
                            job_id=job.job_id,
                            video_id=video_id,
                            current=status.progress_current,
                            total=status.progress_total,
                            label=status.label,
                        )
                    )

            status.mark_finished_ok()
            self.emit(JobStatusEvent(job_id=job.job_id, state=status.state))
            return status

        except Exception as e:
            status.mark_failed(str(e))
            self.emit(LogEvent(job_id=job.job_id, level=LogLevel.ERROR, message=f"Job failed: {e}"))
            self.emit(JobStatusEvent(job_id=job.job_id, state=status.state, error=str(e)))
            return status

    def _run_step(self, *, job_id: str, video_id: str, step: JobStep, settings: Dict[str, Any], run_output_dir: Path) -> None:
        if step == JobStep.TRANSCRIBE:
            self._step_transcribe(job_id=job_id, video_id=video_id, settings=settings.get("transcribe") or {}, run_output_dir=run_output_dir)
        elif step == JobStep.GENERATE_CLIPS:
            self._step_generate_clips(job_id=job_id, video_id=video_id, settings=settings.get("clips") or {}, run_output_dir=run_output_dir)
        elif step == JobStep.EXPORT_CLIPS:
            self._step_export_clips(job_id=job_id, video_id=video_id, settings=settings.get("export") or {}, run_output_dir=run_output_dir)
        elif step == JobStep.EXPORT_SHORTS:
            self._step_export_shorts(job_id=job_id, video_id=video_id, settings=settings, run_output_dir=run_output_dir)
        elif step == JobStep.DOWNLOAD:
            raise ValueError("DOWNLOAD is not supported as a job step; use Add Videos to download and register.")
        else:
            raise ValueError(f"Unknown job step: {step}")

    def _slugify(self, value: str, *, max_len: int = 48) -> str:
        cleaned = (value or "").strip().lower()
        cleaned = cleaned.replace(" ", "_")
        cleaned = re.sub(r"[^a-z0-9._-]+", "_", cleaned)
        cleaned = re.sub(r"_+", "_", cleaned).strip("._-")
        return (cleaned or "run")[:max_len]

    def _resolve_output_dir(self) -> Path:
        """Resolve output directory with priority: CLI arg > env var > config setting > default.

        Returns:
            Resolved and validated output directory path.

        Raises:
            ValueError: If the path is invalid or not writable.
        """
        import os

        # Priority 1: CLI argument
        if self._cli_output_dir:
            output_path = Path(self._cli_output_dir)
        else:
            # Priority 2: Environment variable
            env_output_dir = os.environ.get("CLIPER_OUTPUT_DIR", "").strip()
            if env_output_dir:
                output_path = Path(env_output_dir)
            else:
                # Priority 3: Config setting (or default)
                config_output_dir = self.state_manager.get_setting("output_dir", "output/")
                output_path = Path(config_output_dir) if config_output_dir else Path("output/")

        # Resolve relative paths against cwd
        if not output_path.is_absolute():
            output_path = Path.cwd() / output_path
        output_path = output_path.resolve()

        # Create directory if it doesn't exist
        try:
            output_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise ValueError(f"Cannot create output directory '{output_path}': {e}") from e

        # Validate writability
        if not os.access(output_path, os.W_OK):
            raise ValueError(f"Output directory is not writable: {output_path}")

        return output_path

    def _ensure_run_output_dir(self, *, job_id: str, video_ids: list[str]) -> Path:
        """
        Crea estructura de directorios para el job:
        - {output_dir}/.cache/{job_id}/ para archivos intermedios (transcripts, clips metadata, SRTs)
        - {output_dir}/exports/ para videos finales (estructura plana)
        """
        # Resolve base output directory with priority resolution
        base_output_dir = self._resolve_output_dir()

        # Cache dir para archivos intermedios
        cache_dir = (base_output_dir / ".cache" / job_id).resolve()
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Exports dir para videos finales
        exports_dir = self._get_exports_dir()

        # Persist for UI / debugging.
        self.state_manager.update_job_status(job_id, {
            "cache_dir": str(cache_dir),
            "exports_dir": str(exports_dir),
        })
        return cache_dir

    def _get_exports_dir(self) -> Path:
        """Retorna el directorio de exports ({output_dir}/exports/)."""
        base_output_dir = self._resolve_output_dir()
        exports_dir = (base_output_dir / "exports").resolve()
        exports_dir.mkdir(parents=True, exist_ok=True)
        return exports_dir

    def _ensure_video_run_dir(self, *, run_output_dir: Path, video_id: str) -> Path:
        """Crea subdirectorio de cache para un video especifico."""
        video_dir = run_output_dir / video_id
        video_dir.mkdir(parents=True, exist_ok=True)
        return video_dir

    def _copy_if_exists(self, src: Path, dst: Path) -> Optional[Path]:
        if not src.exists():
            return None
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        return dst

    def _rewrite_transcript_json_paths(self, transcript_path: Path, *, audio_path: Optional[Path] = None) -> None:
        if not transcript_path.exists() or transcript_path.suffix.lower() != ".json":
            return
        try:
            data = json.loads(transcript_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(data, dict):
            return
        updated = False
        if audio_path and audio_path.exists():
            data["audio_path"] = str(audio_path)
            updated = True
        if updated:
            transcript_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _copy_exported_clip(self, src: Path, *, video_run_dir: Path) -> Optional[Path]:
        if not src.exists():
            return None
        try:
            export_idx = src.parts.index("export")
        except ValueError:
            export_idx = -1
        rel = Path(*src.parts[export_idx + 1 :]) if export_idx >= 0 else Path(src.name)
        dst = video_run_dir / "export" / rel
        return self._copy_if_exists(src, dst)

    def _get_video_path(self, video_id: str) -> str:
        path = self.state_manager.get_video_path(video_id)
        if not path:
            raise FileNotFoundError(f"Video path not registered for: {video_id}")
        return path

    def _step_transcribe(self, *, job_id: str, video_id: str, settings: Dict[str, Any], run_output_dir: Path) -> None:
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Starting transcription"))

        video_path = self._get_video_path(video_id)
        if self.state_manager.is_transcribed(video_id) and settings.get("skip_done", True):
            state = self.state_manager.get_video_state(video_id) or {}
            transcript_existing = state.get("transcript_path") or state.get("transcription_path")
            copied_transcript: Optional[Path] = None
            copied_audio: Optional[Path] = None
            if transcript_existing:
                video_run_dir = self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
                transcript_src = Path(transcript_existing)
                transcript_dst = video_run_dir / "transcribe" / transcript_src.name
                copied_transcript = self._copy_if_exists(transcript_src, transcript_dst)
                if copied_transcript:
                    self.state_manager.update_job_status(job_id, {"transcript_path": str(copied_transcript)})

            audio_src = Path("temp") / f"{Path(video_path).stem}_audio.wav"
            if audio_src.exists():
                video_run_dir = self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
                audio_dst = video_run_dir / "transcribe" / audio_src.name
                copied_audio = self._copy_if_exists(audio_src, audio_dst)
                if copied_audio:
                    self.state_manager.update_job_status(job_id, {"transcript_audio_path": str(copied_audio)})
            if copied_transcript and copied_audio:
                self._rewrite_transcript_json_paths(copied_transcript, audio_path=copied_audio)
            self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Already transcribed; skipping"))
            return

        class _JobDependencyReporter(DependencyReporter):
            def __init__(self, emit: EmitFn, job_id: str, video_id: str):
                self._emit = emit
                self._job_id = job_id
                self._video_id = video_id

            def report(self, event: DependencyProgress) -> None:
                if event.status == DependencyStatus.CHECKING:
                    self._emit(
                        LogEvent(
                            job_id=self._job_id,
                            video_id=self._video_id,
                            level=LogLevel.INFO,
                            message=f"Checking dependency: {event.description}",
                        )
                    )
                elif event.status == DependencyStatus.DOWNLOADING:
                    self._emit(
                        LogEvent(
                            job_id=self._job_id,
                            video_id=self._video_id,
                            level=LogLevel.INFO,
                            message=f"Downloading dependency: {event.description}",
                        )
                    )
                elif event.status == DependencyStatus.SKIPPED:
                    self._emit(
                        LogEvent(
                            job_id=self._job_id,
                            video_id=self._video_id,
                            level=LogLevel.INFO,
                            message=f"Dependency already installed: {event.description}",
                        )
                    )
                elif event.status == DependencyStatus.DONE:
                    self._emit(
                        LogEvent(
                            job_id=self._job_id,
                            video_id=self._video_id,
                            level=LogLevel.INFO,
                            message=f"Dependency ready: {event.description}",
                        )
                    )
                elif event.status == DependencyStatus.ERROR:
                    self._emit(
                        LogEvent(
                            job_id=self._job_id,
                            video_id=self._video_id,
                            level=LogLevel.ERROR,
                            message=f"Dependency failed: {event.description} ({event.message})",
                        )
                    )

            def is_cancelled(self) -> bool:
                return False

        device_setting = str(settings.get("device", "auto"))
        resolved_device = "cpu" if device_setting == "auto" else device_setting
        model_size = str(settings.get("model", "base"))
        compute_type = str(settings.get("compute_type", "int8"))
        language_code = settings.get("language")
        deps_cache_key = (
            model_size,
            str(language_code) if language_code else "",
            resolved_device,
            compute_type,
        )
        if deps_cache_key not in self._dependency_ok_cache:
            deps_result = ensure_transcription_dependencies(
                model_size=model_size,
                language_code=str(language_code) if language_code else None,
                device=resolved_device,
                compute_type=compute_type,
                reporter=_JobDependencyReporter(self.emit, job_id, video_id),
            )
            if not deps_result.ok:
                details = "; ".join(f"{k}: {v}" for k, v in deps_result.failed.items()) or "Unknown dependency error"
                raise RuntimeError(f"Missing required dependencies: {details}")
            self._dependency_ok_cache.add(deps_cache_key)
        else:
            self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Dependencies already ensured; skipping checks"))

        from src.transcriber import Transcriber

        transcriber = Transcriber(
            model_size=model_size,
            device=device_setting,
            compute_type=compute_type,
        )
        transcript_path_raw = transcriber.transcribe(
            video_path=video_path,
            language=language_code,
            skip_if_exists=settings.get("skip_if_exists", True),
        )
        if not transcript_path_raw:
            raise RuntimeError("Transcription failed (no transcript returned)")

        transcript_src = Path(transcript_path_raw)
        video_run_dir = self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
        transcript_dst = video_run_dir / "transcribe" / transcript_src.name
        copied_transcript = self._copy_if_exists(transcript_src, transcript_dst)
        transcript_path = str(copied_transcript or transcript_src)

        audio_src = Path("temp") / f"{Path(video_path).stem}_audio.wav"
        audio_dst = video_run_dir / "transcribe" / audio_src.name
        copied_audio = self._copy_if_exists(audio_src, audio_dst)
        if copied_transcript and copied_audio:
            self._rewrite_transcript_json_paths(copied_transcript, audio_path=copied_audio)

        self.state_manager.mark_transcribed(video_id, transcript_path)
        job_updates: Dict[str, object] = {"transcribed": True, "transcript_path": transcript_path}
        if copied_audio:
            job_updates["transcript_audio_path"] = str(copied_audio)
        self.state_manager.update_job_status(job_id, job_updates)
        self.emit(StateEvent(job_id=job_id, video_id=video_id, updates=job_updates))
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Transcription complete"))

    def _step_generate_clips(self, *, job_id: str, video_id: str, settings: Dict[str, Any], run_output_dir: Path) -> None:
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Generating clips"))

        state = self.state_manager.get_video_state(video_id) or {}
        transcript_path = state.get("transcript_path") or state.get("transcription_path")
        if not transcript_path:
            raise RuntimeError("No transcript_path found; run Transcribe first")

        if (state.get("clips_generated") or state.get("clips_metadata_path")) and settings.get("skip_done", True):
            clips_metadata_existing = state.get("clips_metadata_path")
            if clips_metadata_existing:
                video_run_dir = self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
                metadata_src = Path(clips_metadata_existing)
                metadata_dst = video_run_dir / "clips" / metadata_src.name
                copied_metadata = self._copy_if_exists(metadata_src, metadata_dst)
                if copied_metadata:
                    self.state_manager.update_job_status(job_id, {"clips_metadata_path": str(copied_metadata)})
            self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Clips already generated; skipping"))
            return

        from src.clips_generator import ClipsGenerator

        # Get clip generation settings from job settings or app settings
        app_settings = self.state_manager.load_settings()
        generator = ClipsGenerator(
            min_clip_duration=int(settings.get("min_seconds", app_settings.get("min_clip_duration", 30))),
            max_clip_duration=int(settings.get("max_seconds", app_settings.get("max_clip_duration", 90))),
        )
        clips = generator.generate_clips(
            transcript_path=transcript_path,
            min_clips=int(settings.get("min_clips", app_settings.get("min_clips", 3))),
            max_clips=int(settings.get("max_clips", app_settings.get("max_clips", 10))),
        )

        if not clips:
            raise RuntimeError("Clips generation failed (no clips returned)")

        video_run_dir = self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
        clips_metadata_path = generator.save_clips_metadata(
            clips=clips,
            video_id=video_id,
            output_path=str(video_run_dir / "clips" / f"{video_id}_clips.json"),
        )

        self.state_manager.mark_clips_generated(video_id, clips or [], clips_metadata_path=clips_metadata_path)
        self.state_manager.update_job_status(job_id, {"clips_metadata_path": clips_metadata_path})
        self.emit(
            StateEvent(
                job_id=job_id,
                video_id=video_id,
                updates={
                    "clips_generated": True,
                    "clips_count": len(clips or []),
                    "clips_metadata_path": clips_metadata_path,
                },
            )
        )
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Clips generation complete"))

    def _step_export_clips(self, *, job_id: str, video_id: str, settings: Dict[str, Any], run_output_dir: Path) -> None:
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Exporting clips"))

        state = self.state_manager.get_video_state(video_id) or {}
        clips = state.get("clips") or []
        if not clips:
            raise RuntimeError("No clips in state; run Generate Clips first")

        if state.get("clips_exported") and settings.get("skip_done", True):
            existing_paths = [Path(p) for p in (state.get("exported_clips") or []) if p]
            video_run_dir = self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
            copied: list[Path] = []
            for src in existing_paths:
                copied_path = self._copy_exported_clip(src, video_run_dir=video_run_dir)
                if copied_path:
                    copied.append(copied_path)

            mp4s = [p for p in copied if p.suffix.lower() == ".mp4" and p.exists()]
            final_mp4s = mp4s
            if not final_mp4s:
                final_mp4s = [p for p in existing_paths if p.suffix.lower() == ".mp4" and p.exists()]
            if final_mp4s:
                final = max(final_mp4s, key=lambda p: p.stat().st_mtime)
                self.state_manager.update_job_status(
                    job_id,
                    {"final_video_path": str(final.resolve()), "final_video_paths": [str(p.resolve()) for p in final_mp4s]},
                )

            self.emit(
                StateEvent(
                    job_id=job_id,
                    video_id=video_id,
                    updates={"clips_exported": True, "exported_count": len(mp4s)},
                )
            )
            self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Clips already exported; skipping"))
            return

        def _safe_int_setting(key: str, default: int = 0) -> int:
            raw_value = settings.get(key, default)
            if raw_value is None or raw_value == "":
                return default
            try:
                return int(raw_value)
            except (TypeError, ValueError):
                self.emit(
                    LogEvent(
                        job_id=job_id,
                        video_id=video_id,
                        level=LogLevel.WARNING,
                        message=f"Invalid export setting {key}={raw_value!r}; using {default}",
                    )
                )
                return default

        from src.video_exporter import VideoExporter
        from src.utils.logo import DEFAULT_BUILTIN_LOGO_PATH, resolve_logo_path

        # Get auto-naming settings
        app_settings = self.state_manager.load_settings()
        auto_name_method = app_settings.get("auto_name_method", "filename")
        auto_name_max_chars = int(app_settings.get("auto_name_max_chars", 40))
        auto_name_word_count = int(app_settings.get("auto_name_word_count", 5))

        # Generate video name from transcript
        video_path = self._get_video_path(video_id)
        transcript_path = state.get("transcript_path") or state.get("transcription_path")
        video_name = generate_video_name(
            transcript_path=transcript_path,
            original_filename=Path(video_path).name,
            method=auto_name_method,
            max_chars=auto_name_max_chars,
            word_count=auto_name_word_count,
        )
        self.state_manager.set_auto_generated_name(video_id, video_name)
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message=f"Auto-generated video name: {video_name}"))

        # Use flat exports directory
        exports_dir = self._get_exports_dir()
        exporter = VideoExporter(output_dir=str(exports_dir))

        saved_logo_path = self.state_manager.get_setting("logo_path", DEFAULT_BUILTIN_LOGO_PATH)
        resolved_logo_path = resolve_logo_path(
            user_logo_path=settings.get("logo_path"),
            saved_logo_path=saved_logo_path,
            builtin_logo_path=DEFAULT_BUILTIN_LOGO_PATH,
        )
        add_logo = bool(settings.get("add_logo", False))
        if add_logo and not resolved_logo_path:
            add_logo = False

        # Build settings dict for subtitle style helpers (app_settings already loaded above)
        effective_style = get_effective_subtitle_style(app_settings)
        custom_style = build_custom_subtitle_style(app_settings) if effective_style == "__custom__" else None

        exported_paths = exporter.export_clips(
            video_path=video_path,
            clips=clips,
            aspect_ratio=settings.get("aspect_ratio") or app_settings.get("default_aspect_ratio") or None,
            video_name=video_name,
            add_subtitles=bool(settings.get("add_subtitles", False)),
            transcript_path=transcript_path,
            subtitle_style=effective_style,
            custom_style=custom_style,
            organize_by_style=bool(settings.get("organize_by_style", False)),
            clip_styles=state.get("clip_styles"),
            trim_ms_start=_safe_int_setting("trim_ms_start", 0),
            trim_ms_end=_safe_int_setting("trim_ms_end", 0),
            enable_face_tracking=bool(settings.get("enable_face_tracking", app_settings.get("enable_face_tracking", False))),
            face_tracking_strategy=str(settings.get("face_tracking_strategy", app_settings.get("face_tracking_strategy", "keep_in_frame"))),
            face_tracking_sample_rate=int(settings.get("face_tracking_sample_rate", app_settings.get("face_tracking_sample_rate", 3))),
            add_logo=add_logo,
            logo_path=(resolved_logo_path if add_logo else None),
            logo_position=str(settings.get("logo_position", app_settings.get("logo_position", "top-right"))),
            logo_scale=float(settings.get("logo_scale", app_settings.get("logo_scale", 0.1))),
            video_crf=int(settings.get("video_crf", app_settings.get("video_crf", 23))),
            ffmpeg_threads=int(settings.get("ffmpeg_threads", app_settings.get("ffmpeg_threads", 0))),
            subtitle_max_chars_per_line=int(settings.get("subtitle_max_chars_per_line", app_settings.get("subtitle_max_chars_per_line", 42))),
            subtitle_max_duration=float(settings.get("subtitle_max_duration", app_settings.get("subtitle_max_duration", 5.0))),
            flat_output=True,
        )

        self.state_manager.mark_clips_exported(video_id, exported_paths, aspect_ratio=settings.get("aspect_ratio"))
        if exported_paths:
            self.state_manager.update_job_status(
                job_id,
                {
                    "final_video_path": str(Path(exported_paths[-1]).resolve()),
                    "final_video_paths": [str(Path(p).resolve()) for p in exported_paths],
                },
            )
        self.emit(
            StateEvent(
                job_id=job_id,
                video_id=video_id,
                updates={"clips_exported": True, "exported_count": len(exported_paths)},
            )
        )
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Export complete"))

    def _step_export_shorts(self, *, job_id: str, video_id: str, settings: Dict[str, Any], run_output_dir: Path) -> None:
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Exporting short (full video)"))

        shorts_settings = settings.get("shorts") or {}
        skip_done = bool(shorts_settings.get("skip_done", True))
        if self.state_manager.is_shorts_exported(video_id) and skip_done:
            self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Short already exported; skipping"))
            return

        video_run_dir = self._ensure_video_run_dir(run_output_dir=run_output_dir, video_id=video_id)
        state = self.state_manager.get_video_state(video_id) or {}

        # Optional: process an already-exported clip instead of the full registered video.
        input_paths = shorts_settings.get("input_paths") or {}
        input_path = None
        if isinstance(input_paths, dict):
            candidate = input_paths.get(video_id)
            if candidate:
                input_path = str(candidate)
        if not input_path:
            input_path = str(shorts_settings.get("input_path") or "").strip() or self._get_video_path(video_id)

        transcript_path = state.get("transcript_path") or state.get("transcription_path")
        if not transcript_path or not self.state_manager.is_transcribed(video_id):
            # Ensure transcription exists (reuses TRANSCRIBE skip behavior).
            self._step_transcribe(
                job_id=job_id,
                video_id=video_id,
                settings=settings.get("transcribe") or {},
                run_output_dir=run_output_dir,
            )
            state = self.state_manager.get_video_state(video_id) or {}
            transcript_path = state.get("transcript_path") or state.get("transcription_path")

        if not transcript_path:
            raise RuntimeError("No transcript_path found; run Transcribe first")

        from src.subtitle_generator import SubtitleGenerator
        from src.video_exporter import VideoExporter

        # Get auto-naming settings
        app_settings = self.state_manager.load_settings()
        auto_name_method = app_settings.get("auto_name_method", "filename")
        auto_name_max_chars = int(app_settings.get("auto_name_max_chars", 40))
        auto_name_word_count = int(app_settings.get("auto_name_word_count", 5))

        # Generate video name from transcript
        video_name = generate_video_name(
            transcript_path=transcript_path,
            original_filename=Path(input_path).name,
            method=auto_name_method,
            max_chars=auto_name_max_chars,
            word_count=auto_name_word_count,
        )
        self.state_manager.set_auto_generated_name(video_id, video_name)
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message=f"Auto-generated video name: {video_name}"))

        # SRT generation — needed for subtitles and speech-aware trim
        add_subtitles = bool(shorts_settings.get("add_subtitles", True))
        temp_dir = Path(shorts_settings.get("temp_dir") or (video_run_dir / "shorts" / "temp"))
        temp_dir.mkdir(parents=True, exist_ok=True)
        srt_path = temp_dir / f"{video_id}.srt"

        # Get subtitle formatting settings from app settings
        subtitle_max_chars_per_line = int(shorts_settings.get("max_chars_per_line", app_settings.get("subtitle_max_chars_per_line", 42)))
        subtitle_max_duration = float(shorts_settings.get("max_duration", app_settings.get("subtitle_max_duration", 5.0)))

        # Always generate SRT (used for speech-aware trimming even without burn-in)
        srt_generated = SubtitleGenerator().generate_srt_from_transcript(
            transcript_path=str(transcript_path),
            output_path=str(srt_path),
            max_chars_per_line=subtitle_max_chars_per_line,
            max_duration=subtitle_max_duration,
        )
        if not srt_generated:
            raise RuntimeError("Failed to generate SRT from transcript")

        # Use flat exports directory
        exports_dir = self._get_exports_dir()
        exporter = VideoExporter(output_dir=str(exports_dir))

        # Build settings dict for subtitle style helpers
        effective_style = get_effective_subtitle_style(app_settings)
        custom_style = build_custom_subtitle_style(app_settings) if effective_style == "__custom__" else None

        # Output filename includes video name
        output_filename = f"{video_name}_short.mp4"

        exported_path = exporter.export_full_video(
            video_path=input_path,
            video_name=video_name,
            output_filename=output_filename,
            srt_path=str(srt_path) if add_subtitles else None,
            transcript_path=str(transcript_path),
            subtitle_style=str(shorts_settings.get("subtitle_style") or effective_style),
            custom_style=custom_style,
            add_logo=bool(shorts_settings.get("add_logo", True)),
            logo_path=str(shorts_settings.get("logo_path") or app_settings.get("logo_path", "assets/logo.png")),
            logo_position=str(shorts_settings.get("logo_position") or app_settings.get("logo_position", "top-right")),
            logo_scale=(app_settings.get("logo_scale", 0.1) if shorts_settings.get("logo_scale") is None else float(shorts_settings.get("logo_scale"))),
            video_crf=int(shorts_settings.get("video_crf", app_settings.get("video_crf", 23))),
            ffmpeg_threads=int(shorts_settings.get("ffmpeg_threads", app_settings.get("ffmpeg_threads", 0))),
            subtitle_max_chars_per_line=subtitle_max_chars_per_line,
            subtitle_max_duration=subtitle_max_duration,
            flat_output=True,
            # Dead space trimming
            trim_ms_start=int(shorts_settings.get("trim_ms_start", app_settings.get("trim_ms_start", 0))),
            trim_ms_end=int(shorts_settings.get("trim_ms_end", app_settings.get("trim_ms_end", 0))),
        )

        self.state_manager.mark_shorts_exported(video_id, exported_path, srt_path=str(srt_path), input_path=input_path)
        self.emit(
            StateEvent(
                job_id=job_id,
                video_id=video_id,
                updates={"shorts_exported": True, "shorts_export_path": exported_path},
            )
        )
        self.emit(LogEvent(job_id=job_id, video_id=video_id, level=LogLevel.INFO, message="Short export complete"))
