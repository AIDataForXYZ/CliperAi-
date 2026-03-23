import asyncio
import json
from pathlib import Path

import pytest

pytest.importorskip("textual")


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

    # Give the startup worker a moment to mount the modal.
    await pilot.pause(0.2)
    if has_dep_modal():
        await pilot.press("escape")
        await _wait_until(pilot, lambda: not has_dep_modal())


def test_settings_modal_layout_common_sizes(tmp_path: Path, monkeypatch) -> None:
    async def run(size: tuple[int, int]) -> None:
        monkeypatch.chdir(tmp_path)

        import src.utils.state_manager as state_manager_module

        state_manager_module._state_manager_instance = None
        settings_file = tmp_path / "config" / "app_settings.json"
        settings_file.parent.mkdir(parents=True, exist_ok=True)
        # Pre-set wizard_completed to skip the setup wizard during tests
        settings_file.write_text(json.dumps({"_wizard_completed": True}), encoding="utf-8")
        state_manager_module._state_manager_init_kwargs = {"app_root": tmp_path, "settings_file": settings_file}

        import src.tui.app as tui_app_module

        app = tui_app_module.CliperTUI()
        # Avoid background startup dependency checks in the test harness.
        app._startup_dep_check_done = True
        app._startup_wizard_check_done = True

        async with app.run_test(size=size) as pilot:
            from textual.widgets import Input, Static

            await _dismiss_dependency_modal_if_present(pilot, app)
            await app.action_settings()

            def has_logo_input() -> bool:
                try:
                    app.screen.query_one("#setting_logo_path", Input)
                    return True
                except Exception:
                    return False

            await _wait_until(pilot, has_logo_input)

            modal = app.screen

            # Invalid extension: should show a clear error and avoid persisting.
            baseline_logo = app.state_manager.get_setting("logo_path")
            invalid_logo = tmp_path / "logo.gif"
            invalid_logo.write_bytes(b"")
            app.screen.query_one("#setting_logo_path", Input).value = str(invalid_logo)
            # Force a validation pass (programmatic value changes may not emit Input.Changed).
            modal._validate_and_save()  # type: ignore[attr-defined]

            def has_error() -> bool:
                try:
                    err = app.screen.query_one("#setting_logo_path_error", Static)
                    return bool(err.display) and bool(err.content.strip())
                except Exception:
                    return False

            await _wait_until(pilot, has_error)
            assert app.state_manager.get_setting("logo_path") == baseline_logo

            # Valid PNG: should enable Save and persist.
            custom_logo = tmp_path / "logo.png"
            custom_logo.write_bytes(b"\x89PNG\r\n\x1a\n")
            app.screen.query_one("#setting_logo_path", Input).value = str(custom_logo)
            modal._validate_and_save()  # type: ignore[attr-defined]

            await pilot.press("escape")
            await _wait_until(pilot, lambda: not has_logo_input())

            persisted = json.loads(settings_file.read_text(encoding="utf-8"))
            assert persisted.get("logo_path") == str(custom_logo)

    # Smoke-check common terminal sizes for the new modal layout.
    for size in [(80, 24), (100, 30), (120, 40)]:
        asyncio.run(run(size))
