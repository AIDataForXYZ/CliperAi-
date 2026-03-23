import asyncio
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

pytest.importorskip("textual")


def _create_fixture_video_mp4(path: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        pytest.skip("ffmpeg not available; skipping TUI video integration test")

    path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        ffmpeg,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=320x240:d=1",
        "-c:v",
        "mpeg4",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        str(path),
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        pytest.skip(f"ffmpeg could not generate mp4 fixture (missing encoder?): {e.stderr or e}")


async def _wait_until(pilot, predicate, *, timeout: float = 5.0, step: float = 0.05) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if predicate():
            return
        if loop.time() >= deadline:
            raise AssertionError("Timed out waiting for UI state")
        await pilot.pause(step)


async def _dismiss_dependency_modal_if_present(pilot, app) -> None:
    """Startup may auto-open the dependency modal when deps are missing."""

    def has_dep_modal() -> bool:
        try:
            app.screen.query_one("#dep_modal")
            return True
        except Exception:
            return False

    await pilot.pause(0.2)
    if has_dep_modal():
        await pilot.press("escape")
        await _wait_until(pilot, lambda: not has_dep_modal())


def test_tui_video_features_single_video(tmp_path: Path, monkeypatch) -> None:
    async def run() -> None:
        # Isolate all state in a temporary project directory.
        monkeypatch.chdir(tmp_path)

        import src.utils.state_manager as state_manager_module

        state_manager_module._state_manager_instance = None
        settings_file = tmp_path / "config" / "app_settings.json"
        settings_file.parent.mkdir(parents=True, exist_ok=True)
        # Pre-set wizard_completed to skip the setup wizard during tests
        settings_file.write_text(json.dumps({"_wizard_completed": True}), encoding="utf-8")
        state_manager_module._state_manager_init_kwargs = {"app_root": tmp_path, "settings_file": settings_file}

        from src.core.events import LogEvent, LogLevel, StateEvent
        from src.core.models import JobStatus, JobStep
        import src.tui.app as tui_app_module
        from src.utils.logo import DEFAULT_BUILTIN_LOGO_PATH

        fixture_path = tmp_path / "dec 9th promo video.mp4"
        _create_fixture_video_mp4(fixture_path)
        video_id = fixture_path.stem

        class FakeJobRunner:
            def __init__(self, state_manager, emit, *, cli_output_dir=None):
                self.state_manager = state_manager
                self.emit = emit

            def run_job(self, spec):
                status = JobStatus(progress_current=0, progress_total=max(1, len(spec.video_ids) * len(spec.steps)))
                status.mark_started()

                def slugify(value: str, max_len: int = 48) -> str:
                    cleaned = (value or "").strip().lower().replace(" ", "_")
                    cleaned = re.sub(r"[^a-z0-9._-]+", "_", cleaned)
                    cleaned = re.sub(r"_+", "_", cleaned).strip("._-")
                    return (cleaned or "run")[:max_len]

                run_output_dir = Path("output") / "runs" / f"{slugify(video_id)}_{spec.job_id}"
                run_output_dir.mkdir(parents=True, exist_ok=True)
                self.state_manager.update_job_status(spec.job_id, {"run_output_dir": str(run_output_dir)})

                for video_id in spec.video_ids:
                    for step in spec.steps:
                        if step == JobStep.TRANSCRIBE:
                            transcript_path = run_output_dir / video_id / "transcribe" / f"{video_id}_transcript.json"
                            transcript_path.parent.mkdir(parents=True, exist_ok=True)
                            transcript_path.write_text('{"segments":[{"start":0,"end":1,"text":"dummy"}]}', encoding="utf-8")
                            self.state_manager.mark_transcribed(video_id, str(transcript_path))
                            self.emit(StateEvent(job_id=spec.job_id, video_id=video_id, updates={"transcribed": True}))
                        elif step == JobStep.GENERATE_CLIPS:
                            clips = [{"start": 0, "end": 1, "title": "clip-1"}]
                            clips_metadata_path = run_output_dir / video_id / "clips" / f"{video_id}.json"
                            clips_metadata_path.parent.mkdir(parents=True, exist_ok=True)
                            clips_metadata_path.write_text("[]", encoding="utf-8")
                            self.state_manager.mark_clips_generated(video_id, clips, clips_metadata_path=str(clips_metadata_path))
                            self.emit(
                                StateEvent(
                                    job_id=spec.job_id,
                                    video_id=video_id,
                                    updates={"clips_generated": True, "clips_count": 1},
                                )
                            )
                        elif step == JobStep.EXPORT_CLIPS:
                            export_dir = run_output_dir / video_id / "export"
                            export_dir.mkdir(parents=True, exist_ok=True)
                            exported_path = export_dir / "clip1.mp4"
                            exported_path.write_bytes(b"")
                            self.state_manager.mark_clips_exported(video_id, [str(exported_path)])
                            self.state_manager.update_job_status(spec.job_id, {"final_video_path": str(exported_path)})
                            self.emit(
                                StateEvent(
                                    job_id=spec.job_id,
                                    video_id=video_id,
                                    updates={"clips_exported": True, "exported_count": 1},
                                )
                            )
                        elif step == JobStep.EXPORT_SHORTS:
                            output_dir = Path("output") / "shorts" / video_id
                            output_dir.mkdir(parents=True, exist_ok=True)
                            exported_path = output_dir / "short.mp4"
                            exported_path.write_bytes(b"")
                            self.state_manager.mark_shorts_exported(video_id, str(exported_path))
                            self.emit(StateEvent(job_id=spec.job_id, video_id=video_id, updates={"shorts_exported": True}))

                        status.progress_current += 1

                status.mark_finished_ok()
                self.emit(LogEvent(job_id=spec.job_id, level=LogLevel.INFO, message="Fake job completed"))
                return status

        monkeypatch.setattr(tui_app_module, "JobRunner", FakeJobRunner)

        app = tui_app_module.CliperTUI()
        # Avoid background startup dependency checks in the test harness.
        app._startup_dep_check_done = True
        app._startup_wizard_check_done = True

        async with app.run_test(size=(120, 40)) as pilot:
            # Start from a known settings baseline.
            app.state_manager.settings = {}
            app.state_manager.set_setting("logo_path", DEFAULT_BUILTIN_LOGO_PATH)

            # Settings entrypoint (hotkey) + Save persists.
            await _dismiss_dependency_modal_if_present(pilot, app)
            await pilot.press("s")

            from textual.widgets import Button, DataTable, Input, RichLog, Static

            def has_settings_logo_input() -> bool:
                try:
                    app.screen.query_one("#setting_logo_path", Input)
                    return True
                except Exception:
                    return False

            await _wait_until(pilot, has_settings_logo_input)

            modal = app.screen

            # Invalid logo paths should show a clear error and avoid persisting.
            baseline_logo = app.state_manager.get_setting("logo_path", DEFAULT_BUILTIN_LOGO_PATH)
            invalid_logo = tmp_path / "custom_logo.gif"
            invalid_logo.write_bytes(b"")
            app.screen.query_one("#setting_logo_path", Input).value = str(invalid_logo)
            # Force a validation pass (programmatic value changes may not emit Input.Changed).
            modal._validate_and_save()  # type: ignore[attr-defined]

            def has_logo_error() -> bool:
                try:
                    err = app.screen.query_one("#setting_logo_path_error", Static)
                    renderable = getattr(err, "renderable", getattr(err, "_renderable", ""))
                    return bool(err.display) and bool(str(renderable).strip())
                except Exception:
                    return False

            await _wait_until(pilot, has_logo_error)
            assert app.state_manager.get_setting("logo_path", DEFAULT_BUILTIN_LOGO_PATH) == baseline_logo

            custom_logo = tmp_path / "custom_logo.png"
            custom_logo.write_bytes(b"\x89PNG\r\n\x1a\n")
            app.screen.query_one("#setting_logo_path", Input).value = str(custom_logo)
            modal._validate_and_save()  # type: ignore[attr-defined]

            await pilot.press("escape")
            await _wait_until(pilot, lambda: not has_settings_logo_input())
            assert app.state_manager.get_setting("logo_path") == str(custom_logo)
            persisted = json.loads(settings_file.read_text(encoding="utf-8"))
            assert persisted.get("logo_path") == str(custom_logo)

            # Settings entrypoint (button) persists valid updates.
            app.screen.query_one("#settings", Button).press()
            await _wait_until(pilot, has_settings_logo_input)
            modal = app.screen

            other_logo = tmp_path / "other_logo.png"
            other_logo.write_bytes(b"\x89PNG\r\n\x1a\n")
            app.screen.query_one("#setting_logo_path", Input).value = str(other_logo)
            modal._validate_and_save()  # type: ignore[attr-defined]
            await pilot.press("escape")

            await _wait_until(pilot, lambda: not has_settings_logo_input())
            assert app.state_manager.get_setting("logo_path") == str(other_logo)
            persisted = json.loads(settings_file.read_text(encoding="utf-8"))
            assert persisted.get("logo_path") == str(other_logo)

            # Repro reported crash path: `a` to add videos should not throw.
            await pilot.press("a")

            def has_paths_input() -> bool:
                try:
                    app.screen.query_one("#paths", Input)
                    return True
                except Exception:
                    return False

            await _wait_until(pilot, has_paths_input)

            paths_input = app.screen.query_one("#paths", Input)
            if hasattr(pilot, "type"):
                await pilot.type(str(fixture_path))
            else:
                paths_input.value = str(fixture_path)

            app.screen.query_one("#add", Button).press()
            await _wait_until(pilot, lambda: not has_paths_input())

            library = app.screen.query_one("#library", DataTable)
            await _wait_until(pilot, lambda: library.row_count == 1)

            jobs_table = app.screen.query_one("#jobs", DataTable)
            await _wait_until(pilot, lambda: jobs_table.row_count == 0)
            assert video_id in (app.state_manager.get_all_videos() or {})

            logs = app.screen.query_one("#logs", RichLog)
            if hasattr(logs, "export_text"):
                exported = logs.export_text()
                assert "NoActiveWorker" not in exported
                assert "Traceback" not in exported

            # Selection workflow
            await pilot.press("space")
            await _wait_until(pilot, lambda: len(app.selected_video_ids) == 1)
            assert video_id in app.selected_video_ids

            # Enqueue & complete each supported job workflow.
            await pilot.press("t")
            await _wait_until(pilot, lambda: len(app.state_manager.list_jobs()) == 1)
            await _wait_until(pilot, lambda: jobs_table.row_count == 1)
            await _wait_until(
                pilot,
                lambda: list(app.state_manager.list_jobs().values())[-1].get("status", {}).get("state") == "succeeded",
            )
            await _wait_until(pilot, lambda: bool((app.state_manager.get_video_state(video_id) or {}).get("transcribed")))

            await pilot.press("c")
            await _wait_until(pilot, lambda: len(app.state_manager.list_jobs()) == 2)
            await _wait_until(pilot, lambda: jobs_table.row_count == 2)
            await _wait_until(
                pilot,
                lambda: list(app.state_manager.list_jobs().values())[-1].get("status", {}).get("state") == "succeeded",
            )
            await _wait_until(pilot, lambda: bool((app.state_manager.get_video_state(video_id) or {}).get("clips_generated")))

            await pilot.press("p")
            await _wait_until(pilot, lambda: len(app.state_manager.list_jobs()) == 3)
            await _wait_until(pilot, lambda: jobs_table.row_count == 3)
            await _wait_until(
                pilot,
                lambda: list(app.state_manager.list_jobs().values())[-1].get("status", {}).get("state") == "succeeded",
            )
            await _wait_until(pilot, lambda: bool((app.state_manager.get_video_state(video_id) or {}).get("shorts_exported")))

            await pilot.press("e")
            await _wait_until(pilot, lambda: len(app.state_manager.list_jobs()) == 4)
            await _wait_until(pilot, lambda: jobs_table.row_count == 4)
            await _wait_until(
                pilot,
                lambda: list(app.state_manager.list_jobs().values())[-1].get("status", {}).get("state") == "succeeded",
            )
            await _wait_until(pilot, lambda: bool((app.state_manager.get_video_state(video_id) or {}).get("clips_exported")))

            # Refresh workflow should keep state consistent.
            await pilot.press("r")
            await _wait_until(pilot, lambda: app.screen.query_one("#library", DataTable).row_count == 1)
            await _wait_until(pilot, lambda: app.screen.query_one("#jobs", DataTable).row_count == 4)

            # Basic health assertion: no job should have failed or recorded an error.
            jobs = app.state_manager.list_jobs().values()
            assert len(list(jobs)) == 4
            for job in app.state_manager.list_jobs().values():
                st = (job or {}).get("status") or {}
                assert st.get("state") == "succeeded"
                assert not st.get("error")
                assert st.get("run_output_dir")
                assert Path(st["run_output_dir"]).exists()

            # Basic artifact assertion: fake export wrote at least one file.
            runs_dir = Path("output") / "runs"
            assert runs_dir.exists()
            assert len([p for p in runs_dir.iterdir() if p.is_dir()]) == 3
            assert any(runs_dir.rglob("*.mp4"))

            # Quit workflow
            await pilot.press("q")

    asyncio.run(run())


def test_tui_shift_p_logo_selection(tmp_path: Path, monkeypatch) -> None:
    async def run() -> None:
        monkeypatch.chdir(tmp_path)

        import src.utils.state_manager as state_manager_module

        state_manager_module._state_manager_instance = None
        settings_file = tmp_path / "config" / "app_settings.json"
        settings_file.parent.mkdir(parents=True, exist_ok=True)
        settings_file.write_text(json.dumps({"_wizard_completed": True}), encoding="utf-8")
        state_manager_module._state_manager_init_kwargs = {"app_root": tmp_path, "settings_file": settings_file}

        from src.core.events import LogEvent, LogLevel
        from src.core.models import JobStatus, JobStep
        import src.tui.app as tui_app_module

        class FakeJobRunner:
            def __init__(self, state_manager, emit, *, cli_output_dir=None):
                self.state_manager = state_manager
                self.emit = emit

            def run_job(self, spec):
                status = JobStatus(progress_current=0, progress_total=max(1, len(spec.video_ids) * len(spec.steps)))
                status.mark_started()
                for _video_id in spec.video_ids:
                    for _step in spec.steps:
                        status.progress_current += 1
                status.mark_finished_ok()
                self.emit(LogEvent(job_id=spec.job_id, level=LogLevel.INFO, message="Fake job completed"))
                return status

        monkeypatch.setattr(tui_app_module, "JobRunner", FakeJobRunner)

        app = tui_app_module.CliperTUI()
        # Avoid background startup dependency checks in the test harness.
        app._startup_dep_check_done = True
        app._startup_wizard_check_done = True

        async with app.run_test(size=(120, 40)) as pilot:
            from textual.widgets import Button, DataTable, Input

            def move_table_cursor_to_row(table: DataTable, row: int) -> None:
                try:
                    table.move_cursor(row=row, column=0, animate=False)
                    return
                except TypeError:
                    try:
                        table.move_cursor(row=row, column=0)
                        return
                    except Exception:
                        pass
                except Exception:
                    pass

                try:
                    table.cursor_coordinate = (row, 0)  # type: ignore[attr-defined]
                    return
                except Exception:
                    pass

                try:
                    table.cursor_row = row  # type: ignore[assignment]
                except Exception:
                    pass

            # Keep this test self-contained without requiring ffmpeg.
            fixture_path = tmp_path / "video.mp4"
            fixture_path.write_bytes(b"")
            video_id = fixture_path.stem

            # Ensure multiple valid logo options exist (built-in + saved).
            custom_logo = tmp_path / "saved_logo.png"
            custom_logo.write_bytes(b"\x89PNG\r\n\x1a\n")
            app.state_manager.set_setting("logo_path", str(custom_logo))

            # Add & select the video.
            await _dismiss_dependency_modal_if_present(pilot, app)
            await pilot.press("a")

            def has_paths_input() -> bool:
                try:
                    app.screen.query_one("#paths", Input)
                    return True
                except Exception:
                    return False

            await _wait_until(pilot, has_paths_input)
            paths_input = app.screen.query_one("#paths", Input)
            if hasattr(pilot, "type"):
                await pilot.type(str(fixture_path))
            else:
                paths_input.value = str(fixture_path)
            app.screen.query_one("#add", Button).press()
            await _wait_until(pilot, lambda: not has_paths_input())

            library = app.screen.query_one("#library", DataTable)
            await _wait_until(pilot, lambda: library.row_count == 1)

            await pilot.press("space")
            await _wait_until(pilot, lambda: len(app.selected_video_ids) == 1)
            assert video_id in app.selected_video_ids

            # Shift+P: choose built-in logo override for this run.
            await pilot.press("P")

            def has_custom_shorts_modal() -> bool:
                try:
                    app.screen.query_one("#custom_shorts_modal")
                    return True
                except Exception:
                    return False

            await _wait_until(pilot, has_custom_shorts_modal)

            logo_table = app.screen.query_one("#logo_table", DataTable)
            assert bool(getattr(logo_table, "display", True))
            move_table_cursor_to_row(logo_table, 1)  # "Built-in" option (row 0 is "Use default")
            app.screen.query_one("#process", Button).press()
            await _wait_until(pilot, lambda: not has_custom_shorts_modal())

            await _wait_until(
                pilot,
                lambda: list(app.state_manager.list_jobs().values())[-1].get("status", {}).get("state") == "succeeded",
            )
            jobs = list(app.state_manager.list_jobs().values())
            assert len(jobs) == 1
            spec_settings = (jobs[-1].get("spec") or {}).get("settings") or {}
            assert (spec_settings.get("shorts") or {}).get("logo_path") == "assets/logo.png"

            # Shift+P again: keep "Use default" selected -> no override in job settings.
            await pilot.press("P")
            await _wait_until(pilot, has_custom_shorts_modal)
            logo_table = app.screen.query_one("#logo_table", DataTable)
            move_table_cursor_to_row(logo_table, 0)
            app.screen.query_one("#process", Button).press()
            await _wait_until(pilot, lambda: not has_custom_shorts_modal())

            await _wait_until(
                pilot,
                lambda: list(app.state_manager.list_jobs().values())[-1].get("status", {}).get("state") == "succeeded",
            )
            jobs = list(app.state_manager.list_jobs().values())
            assert len(jobs) == 2
            spec_settings = (jobs[-1].get("spec") or {}).get("settings") or {}
            assert "logo_path" not in (spec_settings.get("shorts") or {})

    asyncio.run(run())
