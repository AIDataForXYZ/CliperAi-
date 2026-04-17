# -*- coding: utf-8 -*-
"""
Video Exporter - Corta videos en clips usando ffmpeg

Este módulo toma los clips generados y los exporta a archivos de video reales.
Usa ffmpeg para cortar con precisión y opcionalmente cambiar aspect ratio.
"""

import json
import os
import subprocess
from fractions import Fraction
from pathlib import Path
from typing import Dict, List, Optional
from rich.console import Console
from rich.progress import Progress, TaskID

from src.utils.logger import get_logger
from src.subtitle_generator import SubtitleGenerator
from src.reframer import FaceReframer
from src.speech_edge_clip import compute_speech_aware_boundaries

logger = get_logger(__name__)

def _safe_parse_ffprobe_r_frame_rate(r_frame_rate: object) -> float:
    """
    Safely parse ffprobe's `r_frame_rate` field into a numeric FPS value.

    Expected formats include strings like "30/1" or "30000/1001".
    Returns 0.0 if the value is missing or invalid.
    """
    if r_frame_rate is None:
        return 0.0
    if isinstance(r_frame_rate, (int, float)):
        return float(r_frame_rate)
    if not isinstance(r_frame_rate, str):
        return 0.0

    value = r_frame_rate.strip()
    if not value:
        return 0.0

    try:
        fps = float(Fraction(value))
    except Exception:
        return 0.0

    return fps if fps > 0 else 0.0


def _resolve_ffmpeg_threads(threads: int) -> int:
    """
    Resolve thread count for ffmpeg -threads parameter.

    Args:
        threads: 0=auto, positive=specific count, negative=all CPUs minus N

    Returns:
        Actual thread count (0 for auto, or positive integer)
    """
    if threads >= 0:
        return threads
    # Negative: all CPUs minus N
    cpu_count = os.cpu_count() or 4
    result = cpu_count + threads  # threads is negative, so this subtracts
    return max(1, result)  # At least 1 thread


class VideoExporter:
    """
    Exporto clips de video usando ffmpeg

    Características:
    - Corte preciso por timestamps
    - Conversión de aspect ratio (16:9 → 9:16 para redes sociales)
    - Progress tracking
    - Nombres descriptivos para clips
    """

    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.console = Console()
        self.subtitle_generator = SubtitleGenerator()

        # Verifico que ffmpeg esté instalado
        if not self._check_ffmpeg():
            raise RuntimeError(
                "ffmpeg no está instalado. "
                "Instala con: brew install ffmpeg (macOS) o apt install ffmpeg (Linux)"
            )

    def _check_ffmpeg(self) -> bool:
        """
        Verifico si ffmpeg está disponible en el sistema
        """
        try:
            result = subprocess.run(
                ["ffmpeg", "-version"], capture_output=True, text=True, check=False
            )
            return result.returncode == 0
        except FileNotFoundError:
            return False

    def export_clips(
        self,
        video_path: str,
        clips: List[Dict],
        aspect_ratio: Optional[str] = None,
        video_name: Optional[str] = None,
        add_subtitles: bool = False,
        transcript_path: Optional[str] = None,
        subtitle_style: str = "default",
        custom_style: Optional[Dict[str, str]] = None,
        organize_by_style: bool = False,
        clip_styles: Optional[Dict[int, str]] = None,
        # Face tracking parameters (PASO3)
        enable_face_tracking: bool = False,
        face_tracking_strategy: str = "keep_in_frame",
        face_tracking_sample_rate: int = 3,
        # Branding parameters (PASO4 - Logo)
        add_logo: bool = False,
        logo_path: Optional[str] = None,
        logo_position: str = "top-right",
        logo_scale: float = 0.1,
        # Speech-aware trimming: maximum silence buffer at start/end (milliseconds)
        trim_ms_start: int = 0,
        trim_ms_end: int = 0,
        # Video quality and performance
        video_crf: int = 23,
        ffmpeg_threads: int = 0,
        # Subtitle formatting
        subtitle_max_chars_per_line: int = 42,
        subtitle_max_duration: float = 5.0,
        # Output structure
        flat_output: bool = False,
        fonts_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Exporto todos los clips de un video

        Args:
            video_path: Ruta al video original
            clips: Lista de dicts con {clip_id, start_time, end_time, text_preview}
            aspect_ratio: "16:9", "9:16", "1:1", o None (mantener original)
            video_name: Nombre base para los archivos (default: nombre del video)
            add_subtitles: Si True, quema subtítulos en el video
            transcript_path: Ruta al archivo de transcripción (requerido si add_subtitles=True)
            subtitle_style: Estilo de subtítulos ("default", "bold", "yellow")
            organize_by_style: Si True, organiza clips en subcarpetas por estilo
            clip_styles: Dict mapping clip_id → style ("viral", "educational", "storytelling")
            enable_face_tracking: Si True, usa detección de rostros para reencuadre dinámico (9:16 only)
            face_tracking_strategy: "keep_in_frame" (menos movimiento) o "centered" (siempre centrado)
            face_tracking_sample_rate: Procesar cada N frames (default: 3 = 3x speedup)
            add_logo: Si True, superpone el logo en el video.
            logo_path: Ruta al archivo del logo (solo .png/.jpg/.jpeg).
            logo_position: Posición del logo ("top-right", "top-left", "bottom-right", "bottom-left").
            logo_scale: Escala del logo relativa al ancho del video (0.1 = 10%).
            trim_ms_start: Máximo silencio (ms) a conservar antes del habla (requiere transcript_path).
            trim_ms_end: Máximo silencio (ms) a conservar después del habla (requiere transcript_path).
            flat_output: Si True, escribe directamente en output_dir sin crear subcarpeta.

        Returns:
            Lista de rutas a los clips exportados
        """
        video_path = Path(video_path)

        if not video_path.exists():
            raise FileNotFoundError(f"Video no encontrado: {video_path}")

        # Nombre base para los clips
        if video_name is None:
            video_name = video_path.stem

        # Directorio de salida: plano o con subcarpeta por video
        if flat_output:
            video_output_dir = self.output_dir
        else:
            video_output_dir = self.output_dir / video_name
            video_output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Exportando clips a: {video_output_dir}")

        exported_clips = []

        resolved_logo_path = None
        if add_logo:
            from src.utils.logo import coerce_logo_file

            resolved_logo_path = coerce_logo_file(logo_path)
            if not resolved_logo_path:
                logger.warning(
                    "Logo overlay requested but no valid logo file found; skipping logo."
                )
                add_logo = False

        # Progress bar
        with Progress() as progress:
            task = progress.add_task(
                f"[cyan]Exporting {len(clips)} clips...", total=len(clips)
            )

            for clip in clips:
                # Determinar carpeta de salida según estilo (si aplica)
                clip_output_dir = video_output_dir

                if organize_by_style and clip_styles:
                    clip_id = clip["clip_id"]
                    style = clip_styles.get(clip_id, "unclassified")

                    # Crear subcarpeta por estilo
                    clip_output_dir = video_output_dir / style
                    clip_output_dir.mkdir(parents=True, exist_ok=True)

                clip_path = self._export_single_clip(
                    video_path=video_path,
                    clip=clip,
                    video_name=video_name,
                    output_dir=clip_output_dir,
                    aspect_ratio=aspect_ratio,
                    add_subtitles=add_subtitles,
                    transcript_path=transcript_path,
                    subtitle_style=subtitle_style,
                    custom_style=custom_style,
                    trim_ms_start=trim_ms_start,
                    trim_ms_end=trim_ms_end,
                    enable_face_tracking=enable_face_tracking,
                    face_tracking_strategy=face_tracking_strategy,
                    face_tracking_sample_rate=face_tracking_sample_rate,
                    add_logo=add_logo,
                    logo_path=resolved_logo_path,
                    logo_position=logo_position,
                    logo_scale=logo_scale,
                    video_crf=video_crf,
                    ffmpeg_threads=ffmpeg_threads,
                    subtitle_max_chars_per_line=subtitle_max_chars_per_line,
                    subtitle_max_duration=subtitle_max_duration,
                    fonts_dir=fonts_dir,
                )

                if clip_path:
                    exported_clips.append(str(clip_path))

                progress.update(task, advance=1)

        return exported_clips

    def export_full_video(
        self,
        *,
        video_path: str,
        video_name: Optional[str] = None,
        output_filename: str = "short.mp4",
        srt_path: Optional[str] = None,
        transcript_path: Optional[str] = None,
        subtitle_style: str = "default",
        custom_style: Optional[Dict[str, str]] = None,
        add_logo: bool = False,
        logo_path: Optional[str] = "assets/logo.png",
        logo_position: str = "top-right",
        logo_scale: float = 0.1,
        video_crf: int = 23,
        ffmpeg_threads: int = 0,
        subtitle_max_chars_per_line: int = 42,
        subtitle_max_duration: float = 5.0,
        grouping_preset: str = "smart",
        pause_threshold: Optional[float] = None,
        flat_output: bool = False,
        fonts_dir: Optional[str] = None,
        # Speech-aware trimming: maximum silence buffer at start/end (milliseconds)
        trim_ms_start: int = 0,
        trim_ms_end: int = 0,
    ) -> str:
        """
        Exporto un video completo aplicando (opcionalmente) subtítulos y logo.

        Este flujo se usa para "shorts-only": exporta un único archivo (opcionalmente
        con subtítulos/logo) y puede aplicar recorte "speech-aware" si hay transcript.

        Args:
            flat_output: Si True, escribe directamente en output_dir sin crear subcarpeta.
        """
        video_path_p = Path(video_path)
        if not video_path_p.exists():
            raise FileNotFoundError(f"Video no encontrado: {video_path_p}")

        if video_name is None:
            video_name = video_path_p.stem

        # Directorio de salida: plano o con subcarpeta por video
        if flat_output:
            video_output_dir = self.output_dir
            output_path = video_output_dir / output_filename
        else:
            video_output_dir = self.output_dir / video_name
            video_output_dir.mkdir(parents=True, exist_ok=True)
            output_path = video_output_dir / output_filename

        srt_file = Path(srt_path) if srt_path else None
        has_subtitles = bool(srt_file and srt_file.exists())
        resolved_logo_path: Optional[str] = None
        if add_logo:
            from src.utils.logo import coerce_logo_file

            resolved_logo_path = coerce_logo_file(logo_path)
            if not resolved_logo_path:
                logger.warning("Logo overlay requested but no valid logo file found; skipping logo.")
        has_logo = bool(add_logo and resolved_logo_path)

        # Calculate trim parameters (speech-aware)
        trim_args: List[str] = []
        duration_args: List[str] = []
        temp_srt_path: Optional[Path] = None

        trim_window_start = 0.0
        trim_window_end: Optional[float] = None
        total_duration: Optional[float] = None

        if transcript_path and (trim_ms_start > 0 or trim_ms_end > 0):
            # Get video duration to define the window end.
            video_info = self.get_video_info(str(video_path_p))
            total_duration = float(video_info.get("duration", 0) or 0)
            if total_duration and total_duration > 0:
                candidate_start, candidate_end = compute_speech_aware_boundaries(
                    transcript_path=transcript_path,
                    clip_start=0.0,
                    clip_end=total_duration,
                    trim_ms_start=trim_ms_start,
                    trim_ms_end=trim_ms_end,
                )
                if candidate_end > candidate_start:
                    if candidate_start > 0 or candidate_end < total_duration:
                        trim_window_start, trim_window_end = candidate_start, candidate_end
                        logger.info(
                            f"Speech-aware trim for full video: "
                            f"0.000-{total_duration:.3f} -> {trim_window_start:.3f}-{trim_window_end:.3f}"
                        )

        if trim_window_end is not None:
            if trim_window_start > 0:
                trim_args = ["-ss", str(trim_window_start)]
            effective_duration = trim_window_end - trim_window_start
            if effective_duration > 0:
                duration_args = ["-t", str(effective_duration)]
            else:
                trim_args = []
                duration_args = []
                trim_window_start = 0.0
                trim_window_end = None

        # If we trimmed the start/end and subtitles are enabled, regenerate SRT for the trimmed window
        # so subtitles remain clip-relative to the exported output.
        if (
            has_subtitles
            and transcript_path
            and trim_window_end is not None
        ):
            temp_srt_path = video_output_dir / f"{output_path.stem}_trimmed.srt"
            generated = self.subtitle_generator.generate_srt_for_clip(
                transcript_path=transcript_path,
                clip_start=trim_window_start,
                clip_end=float(trim_window_end),
                output_path=str(temp_srt_path),
                max_chars_per_line=subtitle_max_chars_per_line,
                max_duration=subtitle_max_duration,
                grouping_preset=grouping_preset,
                pause_threshold=pause_threshold,
            )
            if generated and temp_srt_path.exists():
                srt_file = temp_srt_path
                has_subtitles = True
            else:
                logger.warning("Failed to regenerate trimmed SRT; subtitles may be desynced if trimming occurred.")

        # Use a two-step process when both logo and subtitles are enabled to avoid subtitle duplication bugs.
        needs_two_steps = has_logo and has_subtitles
        temp_path_step1 = video_output_dir / f"{output_path.stem}_step1_temp.mp4"

        try:
            if needs_two_steps:
                logger.info("Applying logo first, then subtitles in a second step (shorts export).")

                logo_chains, logo_out = self._get_logo_overlay_filter(
                    video_stream="[0:v]",
                    logo_stream="[1:v]",
                    position=logo_position,
                    scale=logo_scale,
                )
                resolved_threads = _resolve_ffmpeg_threads(ffmpeg_threads)
                # Build command with trim args before -i for fast seeking
                cmd1 = ["ffmpeg"]
                cmd1.extend(trim_args)  # -ss before -i for fast seeking
                cmd1.extend(["-i", str(video_path_p)])
                cmd1.extend(["-i", str(resolved_logo_path)])
                cmd1.extend(duration_args)  # -t after inputs
                cmd1.extend([
                    "-filter_complex",
                    ";".join(logo_chains),
                    "-map",
                    logo_out,
                    "-map",
                    "0:a?",
                    "-sn",
                    "-c:v",
                    "libx264",
                    "-c:a",
                    "aac",
                    "-preset",
                    "fast",
                    "-crf",
                    str(video_crf),
                    "-threads",
                    str(resolved_threads),
                    "-y",
                    str(temp_path_step1),
                ])
                result1 = subprocess.run(cmd1, capture_output=True, text=True, check=False)
                if result1.returncode != 0:
                    raise RuntimeError(f"Error exporting short (step 1): {result1.stderr}")

                subtitle_filter = self._get_subtitle_filter(str(srt_file), subtitle_style, custom_style, fonts_dir=fonts_dir)
                cmd2 = [
                    "ffmpeg",
                    "-i",
                    str(temp_path_step1),
                    "-vf",
                    subtitle_filter,
                    "-c:v",
                    "libx264",
                    "-preset",
                    "fast",
                    "-crf",
                    str(video_crf),
                    "-pix_fmt",
                    "yuv420p",
                    "-movflags",
                    "+faststart",
                    "-threads",
                    str(resolved_threads),
                    "-c:a",
                    "copy",
                    "-y",
                    str(output_path),
                ]
                result2 = subprocess.run(cmd2, capture_output=True, text=True, check=False)
                if result2.returncode != 0:
                    raise RuntimeError(f"Error exporting short (step 2): {result2.stderr}")

                return str(output_path)

            # Single-step path (logo only, subtitles only, or neither).
            # Build command with trim args
            cmd = ["ffmpeg"]
            cmd.extend(trim_args)  # -ss before -i for fast seeking

            if has_logo:
                logo_chains, logo_out = self._get_logo_overlay_filter(
                    video_stream="[0:v]",
                    logo_stream="[1:v]",
                    position=logo_position,
                    scale=logo_scale,
                )
                cmd.extend(["-i", str(video_path_p)])
                cmd.extend(["-i", str(resolved_logo_path)])
                cmd.extend(duration_args)  # -t after inputs
                cmd.extend([
                    "-filter_complex",
                    ";".join(logo_chains),
                    "-map",
                    logo_out,
                ])
            elif has_subtitles:
                subtitle_filter = self._get_subtitle_filter(str(srt_file), subtitle_style, custom_style, fonts_dir=fonts_dir)
                cmd.extend(["-i", str(video_path_p)])
                cmd.extend(duration_args)
                cmd.extend(["-vf", subtitle_filter, "-map", "0:v"])
            else:
                cmd.extend(["-i", str(video_path_p)])
                cmd.extend(duration_args)
                cmd.extend(["-map", "0:v"])

            resolved_threads = _resolve_ffmpeg_threads(ffmpeg_threads)
            cmd.extend(
                [
                    "-map",
                    "0:a?",
                    "-sn",
                    "-c:v",
                    "libx264",
                    "-c:a",
                    "aac",
                    "-preset",
                    "fast",
                    "-crf",
                    str(video_crf),
                    "-threads",
                    str(resolved_threads),
                    "-y",
                    str(output_path),
                ]
            )

            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                raise RuntimeError(f"Error exporting short: {result.stderr}")

            return str(output_path)

        finally:
            if temp_path_step1.exists():
                temp_path_step1.unlink()
            if temp_srt_path and temp_srt_path.exists():
                temp_srt_path.unlink()

    def _escape_ffmpeg_filter_path(self, path: str) -> str:
        """
        Escapa una ruta para usarse dentro de un string de filtro de ffmpeg.

        Nota: esto está pensado para filtros como `subtitles=...` y `movie=...`.
        """
        # Order matters: escape backslashes first.
        return path.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")

    def _export_single_clip(
        self,
        video_path: Path,
        clip: Dict,
        video_name: str,
        output_dir: Path,
        aspect_ratio: Optional[str] = None,
        add_subtitles: bool = False,
        transcript_path: Optional[str] = None,
        subtitle_style: str = "default",
        custom_style: Optional[Dict[str, str]] = None,
        enable_face_tracking: bool = False,
        face_tracking_strategy: str = "keep_in_frame",
        face_tracking_sample_rate: int = 3,
        add_logo: bool = False,
        logo_path: Optional[str] = None,
        logo_position: str = "top-right",
        logo_scale: float = 0.1,
        # Speech-edge trimming parameters
        trim_ms_start: int = 0,
        trim_ms_end: int = 0,
        # Video quality and performance
        video_crf: int = 23,
        ffmpeg_threads: int = 0,
        # Subtitle formatting
        subtitle_max_chars_per_line: int = 42,
        subtitle_max_duration: float = 5.0,
        fonts_dir: Optional[str] = None,
    ) -> Optional[Path]:
        clip_id = clip["clip_id"]
        start_time = float(clip["start_time"])
        end_time = float(clip["end_time"])

        # Speech-aware trimming: keep up to N ms of silence around speech edges.
        if transcript_path and (trim_ms_start > 0 or trim_ms_end > 0):
            new_start, new_end = compute_speech_aware_boundaries(
                transcript_path=transcript_path,
                clip_start=start_time,
                clip_end=end_time,
                trim_ms_start=trim_ms_start,
                trim_ms_end=trim_ms_end,
            )
            if new_start != start_time or new_end != end_time:
                logger.info(
                    f"Speech-aware trim for clip {clip_id}: "
                    f"{start_time:.3f}-{end_time:.3f} -> {new_start:.3f}-{new_end:.3f}"
                )
            start_time, end_time = new_start, new_end

        # Safety: Ensure we don't create negative or zero duration clips
        if end_time <= start_time:
            logger.warning(f"Clip {clip_id} would have zero/negative duration after trim; keeping original window")
            start_time = float(clip["start_time"])
            end_time = float(clip["end_time"])

        duration = end_time - start_time

        output_filename = f"{clip_id}.mp4"
        output_path = output_dir / output_filename

        # Define paths for temporary files
        temp_path_step1 = output_dir / f"{clip_id}_step1_temp.mp4"
        temp_reframed_path = output_dir / f"{clip_id}_reframed_temp.mp4"

        subtitle_file = None
        if add_subtitles and transcript_path:
            subtitle_filename = f"{clip_id}.srt"
            subtitle_file = output_dir / subtitle_filename
            self.subtitle_generator.generate_srt_for_clip(
                transcript_path=transcript_path,
                clip_start=start_time,
                clip_end=end_time,
                output_path=str(subtitle_file),
                max_chars_per_line=subtitle_max_chars_per_line,
                max_duration=subtitle_max_duration,
            )

        video_to_process = video_path

        if enable_face_tracking and aspect_ratio == "9:16":
            logger.info(
                f"Face tracking enabled for clip {clip_id} (strategy: {face_tracking_strategy})"
            )
            try:
                reframer = FaceReframer(
                    frame_sample_rate=face_tracking_sample_rate,
                    strategy=face_tracking_strategy,
                )
                reframer.reframe_video(
                    input_path=str(video_path),
                    output_path=str(temp_reframed_path),
                    target_resolution=(1080, 1920),
                    start_time=start_time,
                    end_time=end_time,
                )
                video_to_process = temp_reframed_path
                aspect_ratio = None
                logger.info(f"Face tracking completed for clip {clip_id}")
            except Exception as e:
                logger.warning(
                    f"Face tracking failed for clip {clip_id}: {e}, falling back to static crop."
                )
                video_to_process = video_path

        # Architectural Decision: Use a two-step process when both logo and subtitles are enabled.
        needs_two_steps = (
            add_logo
            and bool(logo_path)
            and add_subtitles
            and subtitle_file
            and subtitle_file.exists()
        )

        # Determine the output target for the first command
        first_step_output = temp_path_step1 if needs_two_steps else output_path

        try:
            # --- STEP 1: Process all filters EXCEPT subtitles ---
            inputs, filter_chains = [], []
            using_face_tracking = (
                video_to_process == temp_reframed_path and temp_reframed_path.exists()
            )
            video_input_idx, audio_input_idx = (0, 1) if using_face_tracking else (0, 0)

            if using_face_tracking:
                inputs.extend(["-i", str(video_to_process)])
                inputs.extend(["-ss", str(start_time), "-t", str(duration), "-i", str(video_path)])
            else:
                inputs.extend(["-ss", str(start_time), "-t", str(duration), "-i", str(video_path)])

            logo_input_idx = -1
            if add_logo and logo_path:
                inputs.extend(["-i", str(logo_path)])
                logo_input_idx = audio_input_idx + 1
                logger.info(f"Adding logo from {logo_path}")

            last_video_stream = f"[{video_input_idx}:v]"

            # Add filters that can be chained simply
            simple_filters = []
            if aspect_ratio and not using_face_tracking:
                aspect_filter = self._get_aspect_ratio_filter(aspect_ratio)
                if aspect_filter:
                    simple_filters.append(aspect_filter)

            # If we are NOT doing two steps, add subtitles here
            if not needs_two_steps and add_subtitles and subtitle_file and subtitle_file.exists():
                subtitle_filter = self._get_subtitle_filter(str(subtitle_file), subtitle_style, custom_style, fonts_dir=fonts_dir)
                simple_filters.append(subtitle_filter)

            cmd = ["ffmpeg"] + inputs

            # If a logo is present, we must use filter_complex
            if logo_input_idx != -1:
                # Apply simple filters first, if any
                if simple_filters:
                    filter_chains.append(f"{last_video_stream}{','.join(simple_filters)}[v_filtered]")
                    last_video_stream = "[v_filtered]"

                logo_stream = f"[{logo_input_idx}:v]"
                logo_chains, last_video_stream = self._get_logo_overlay_filter(
                    video_stream=last_video_stream,
                    logo_stream=logo_stream,
                    position=logo_position,
                    scale=logo_scale,
                )
                filter_chains.extend(logo_chains)

                cmd.extend(["-filter_complex", ";".join(filter_chains), "-map", last_video_stream])

            elif simple_filters:
                cmd.extend(["-vf", ",".join(simple_filters), "-map", f"{video_input_idx}:v"])
            else:
                cmd.extend(["-map", f"{video_input_idx}:v"])

            # BUGFIX: Add -sn flag when doing two-step processing to discard any subtitle streams
            # This prevents FFmpeg from preserving subtitle metadata that would cause duplication in Step 2
            if needs_two_steps:
                cmd.extend(["-sn"])  # Discard subtitle streams

            resolved_threads = _resolve_ffmpeg_threads(ffmpeg_threads)
            cmd.extend(
                [
                    "-map",
                    f"{audio_input_idx}:a?",
                    "-c:v",
                    "libx264",
                    "-c:a",
                    "aac",
                    "-preset",
                    "fast",
                    "-crf",
                    str(video_crf),
                    "-threads",
                    str(resolved_threads),
                    "-y",
                    str(first_step_output),
                ]
            )

            result1 = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result1.returncode != 0:
                logger.error(
                    f"Error in video processing (Step 1) for clip {clip_id}: {result1.stderr}"
                )
                return None

            # --- STEP 2: Add subtitles if required in a separate, safe step ---
            if needs_two_steps:
                logger.info("Applying subtitles in a second step to avoid duplication bug.")
                subtitle_filter = self._get_subtitle_filter(str(subtitle_file), subtitle_style, custom_style, fonts_dir=fonts_dir)
                cmd2 = [
                    "ffmpeg",
                    "-i",
                    str(first_step_output),
                    "-vf",
                    subtitle_filter,
                    "-c:v",
                    "libx264",
                    "-preset",
                    "fast",
                    "-crf",
                    str(video_crf),
                    "-pix_fmt",
                    "yuv420p",
                    "-movflags",
                    "+faststart",
                    "-threads",
                    str(resolved_threads),
                    "-c:a",
                    "copy",
                    "-y",
                    str(output_path),
                ]

                result2 = subprocess.run(cmd2, capture_output=True, text=True, check=False)
                if result2.returncode != 0:
                    logger.error(
                        f"Error adding subtitles (Step 2) for clip {clip_id}: {result2.stderr}"
                    )
                    first_step_output.replace(output_path)  # Fallback to the version without subtitles
                    return output_path

            logger.info(f"✓ Exported clip {clip_id}: {output_path.name}")
            return output_path

        finally:
            # Cleanup all temporary files
            if temp_path_step1.exists():
                temp_path_step1.unlink()
            if temp_reframed_path and temp_reframed_path.exists():
                temp_reframed_path.unlink()

    def _get_logo_overlay_filter(
        self,
        *,
        video_stream: str,
        logo_stream: str,
        position: str = "top-right",
        scale: float = 0.1,
    ) -> tuple[list[str], str]:
        """
        Genera filtros FFmpeg para escalar y superponer un logo.

        Args:
            video_stream: Label del stream de video (por ej. "[0:v]" o "[v_filtered]").
            logo_stream: Label del stream del logo (por ej. "[1:v]").
            position: Posición del logo ("top-right", "top-left", "bottom-right", "bottom-left").
            scale: Escala del logo relativa al ancho del video (0.1 = 10%).

        Returns:
            (filter_chains, output_stream_label)
        """
        positions = {
            "top-right": "W-w-20:20",
            "top-left": "20:20",
            "bottom-right": "W-w-20:H-h-20",
            "bottom-left": "20:H-h-20",
        }
        pos = positions.get(position, positions["top-right"])

        # 1) Escalo el logo relativo al ancho del video (main_w) y preservo aspecto con h=-1
        # 2) Superpongo el logo escalado en la posición elegida
        logo_scaled = "[logo_scaled]"
        video_for_overlay = "[video_for_overlay]"
        output = "[v_out]"

        filter_chains = [
            f"{logo_stream}{video_stream}scale2ref=w=main_w*{scale}:h=-1{logo_scaled}{video_for_overlay}",
            f"{video_for_overlay}{logo_scaled}overlay={pos}{output}",
        ]
        return filter_chains, output

    def _get_aspect_ratio_filter(self, aspect_ratio: str) -> Optional[str]:
        """
        Genero el filtro de ffmpeg para cambiar aspect ratio

        Estrategia: Crop inteligente + scale
        - 16:9 → 9:16: Crop vertical y resize
        - 16:9 → 1:1: Crop a cuadrado

        Args:
            aspect_ratio: "9:16", "1:1", etc.

        Returns:
            String de filtro para ffmpeg, o None si no se reconoce
        """
        if aspect_ratio == "9:16":
            # Vertical (para Instagram Reels, TikTok, YouTube Shorts)
            # Crop al centro y resize a 1080x1920
            return "crop=ih*9/16:ih,scale=1080:1920"

        elif aspect_ratio == "1:1":
            # Cuadrado (para Instagram post)
            # Crop al centro y resize a 1080x1080
            return "crop=ih:ih,scale=1080:1080"

        elif aspect_ratio == "16:9":
            # Horizontal estándar (ya suele ser así, pero por si acaso)
            return "scale=1920:1080"

        else:
            logger.warning(f"Aspect ratio '{aspect_ratio}' no reconocido, manteniendo original")
            return None

    def _get_subtitle_filter(
        self,
        subtitle_path: str,
        style: str = "default",
        custom_style: Optional[Dict[str, str]] = None,
        fonts_dir: Optional[str] = None,
    ) -> str:
        """
        Genero el filtro de ffmpeg para quemar subtítulos en el video

        Args:
            subtitle_path: Ruta al archivo SRT
            style: Estilo de subtítulos ("default", "bold", "yellow", "tiktok", "__custom__")
            custom_style: Dict with custom style properties (used when style="__custom__")

        Returns:
            String de filtro para ffmpeg
        """
        subtitle_path_escaped = self._escape_ffmpeg_filter_path(subtitle_path)

        # Estilos predefinidos para subtítulos
        # TODOS con texto AMARILLO para máxima visibilidad
        styles = {
            "default": {
                "FontName": "Arial",
                "FontSize": "18",
                "PrimaryColour": "&H0000FFFF",  # AMARILLO
                "OutlineColour": "&H00000000",  # Negro
                "Outline": "2",
                "Shadow": "1",
                "Bold": "0",
            },
            "bold": {
                "FontName": "Arial",
                "FontSize": "22",
                "PrimaryColour": "&H0000FFFF",  # AMARILLO
                "OutlineColour": "&H00000000",
                "Outline": "2",
                "Shadow": "1",
                "Bold": "-1",
            },
            "yellow": {
                "FontName": "Arial",
                "FontSize": "20",
                "PrimaryColour": "&H0000FFFF",  # AMARILLO
                "OutlineColour": "&H00000000",
                "Outline": "2",
                "Shadow": "1",
                "Bold": "-1",
            },
            "tiktok": {
                "FontName": "Arial",
                "FontSize": "20",
                "PrimaryColour": "&H0000FFFF",  # AMARILLO
                "OutlineColour": "&H00000000",
                "Outline": "2",
                "Shadow": "2",
                "Bold": "-1",
                "Alignment": "10",  # Centro arriba
            },
            "small": {
                "FontName": "Arial",
                "FontSize": "10",
                "PrimaryColour": "&H0000FFFF",  # AMARILLO
                "OutlineColour": "&H00000000",
                "Outline": "1",
                "Shadow": "1",
                "Bold": "0",
                "Alignment": "6",  # Centro medio-arriba
                "MarginV": "100",
            },
            "tiny": {
                "FontName": "Arial",
                "FontSize": "8",
                "PrimaryColour": "&H0000FFFF",  # AMARILLO
                "OutlineColour": "&H00000000",
                "Outline": "1",
                "Shadow": "0",
                "Bold": "0",
                "Alignment": "6",  # Centro medio-arriba
                "MarginV": "100",
            },
        }

        # Use custom_style if provided and style is "__custom__"
        if style == "__custom__" and custom_style:
            selected_style = custom_style
        else:
            selected_style = styles.get(style, styles["default"])

        # Construyo el filtro subtitles con el estilo
        # subtitles filter quema los subtítulos directamente en el video
        # Wrapeamos el path con comillas simples para manejar espacios
        if fonts_dir:
            fonts_dir_escaped = self._escape_ffmpeg_filter_path(fonts_dir)
            subtitle_filter = f"subtitles='{subtitle_path_escaped}':fontsdir='{fonts_dir_escaped}':force_style='"
        else:
            subtitle_filter = f"subtitles='{subtitle_path_escaped}':force_style='"
        subtitle_filter += f"FontName={selected_style['FontName']},"
        subtitle_filter += f"FontSize={selected_style['FontSize']},"
        subtitle_filter += f"PrimaryColour={selected_style['PrimaryColour']},"
        subtitle_filter += f"OutlineColour={selected_style['OutlineColour']},"
        subtitle_filter += f"Outline={selected_style['Outline']},"
        subtitle_filter += f"Shadow={selected_style['Shadow']},"
        subtitle_filter += f"Bold={selected_style['Bold']}"

        if "Alignment" in selected_style:
            subtitle_filter += f",Alignment={selected_style['Alignment']}"

        if "MarginV" in selected_style:
            subtitle_filter += f",MarginV={selected_style['MarginV']}"

        subtitle_filter += "'"

        return subtitle_filter

    def get_video_info(self, video_path: str) -> Dict:
        """
        Obtengo información del video usando ffprobe

        Returns:
            Dict con duration, width, height, fps, etc.
        """
        cmd = [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(video_path),
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            data = json.loads(result.stdout)

            # Extraigo info relevante del video stream
            video_stream = next(
                (s for s in data.get("streams", []) if s["codec_type"] == "video"),
                None,
            )

            if not video_stream:
                return {}

            return {
                "duration": float(data["format"].get("duration", 0)),
                "width": video_stream.get("width"),
                "height": video_stream.get("height"),
                "fps": _safe_parse_ffprobe_r_frame_rate(video_stream.get("r_frame_rate")),
                "codec": video_stream.get("codec_name"),
            }

        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            return {}
