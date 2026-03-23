# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Type, TypeVar

T = TypeVar("T")


def _validate_type(value: Any, expected_type: Type[T]) -> T:
    """
    Small compatibility wrapper for pydantic v1/v2 typed validation.

    Applies explicit coercions for str/bool before delegating to pydantic so
    that behaviour is consistent across pydantic v1 and v2:
      - str:  None → "", int/float → str(value)
      - bool: None → False
    """
    # Pre-process simple types that Pydantic v2 won't coerce automatically
    if expected_type is str:
        return ("" if value is None else str(value))  # type: ignore[return-value]
    if expected_type is bool and value is None:
        return False  # type: ignore[return-value]

    try:
        from pydantic import TypeAdapter  # type: ignore
    except (ModuleNotFoundError, ImportError):
        TypeAdapter = None  # type: ignore[assignment]

    if TypeAdapter is not None:
        try:
            return TypeAdapter(expected_type).validate_python(value)  # type: ignore[no-any-return]
        except Exception as e:
            msg = str(e).strip() or "Invalid value"
            raise ValueError(msg) from None

    try:
        from pydantic import parse_obj_as  # type: ignore
    except (ModuleNotFoundError, ImportError):
        parse_obj_as = None  # type: ignore[assignment]

    if parse_obj_as is not None:
        try:
            return parse_obj_as(expected_type, value)  # type: ignore[no-any-return]
        except Exception as e:
            msg = str(e).strip() or "Invalid value"
            raise ValueError(msg) from None

    # Fallback when pydantic isn't available (e.g., minimal TUI env).
    if expected_type is str:
        return ("" if value is None else str(value))  # type: ignore[return-value]
    if expected_type is bool:
        if isinstance(value, bool):
            return value  # type: ignore[return-value]
        if value is None:
            return False  # type: ignore[return-value]
        if isinstance(value, (int, float)):
            return bool(value)  # type: ignore[return-value]
        lowered = str(value).strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True  # type: ignore[return-value]
        if lowered in {"0", "false", "no", "n", "off", ""}:
            return False  # type: ignore[return-value]
        raise ValueError(f"Invalid boolean value: {value!r}")
    if expected_type is int:
        return int(value)  # type: ignore[return-value]
    if expected_type is float:
        return float(value)  # type: ignore[return-value]
    if isinstance(value, expected_type):
        return value  # type: ignore[return-value]
    raise ValueError(f"Expected {expected_type.__name__}, got {type(value).__name__}")


@dataclass(frozen=True)
class SettingGroup:
    key: str
    title: str
    description: str = ""


@dataclass(frozen=True)
class SettingDefinition:
    key: str
    group: str
    label: str
    python_type: Type[Any]
    default: Any
    help_text: str = ""
    placeholder: str = ""
    is_secret: bool = False
    normalize: Optional[Callable[[Any], Any]] = None

    def validate_and_normalize(self, value: Any) -> Any:
        typed = _validate_type(value, self.python_type)
        if self.normalize is None:
            return typed
        return self.normalize(typed)

    def validate_from_text(self, raw: str) -> Any:
        text = "" if raw is None else str(raw)
        if self.python_type is str:
            return self.validate_and_normalize(text)

        stripped = text.strip()
        if stripped == "":
            return self.validate_and_normalize(self.default)

        if self.python_type is bool:
            lowered = stripped.lower()
            if lowered in {"1", "true", "yes", "y", "on"}:
                return self.validate_and_normalize(True)
            if lowered in {"0", "false", "no", "n", "off"}:
                return self.validate_and_normalize(False)
            raise ValueError("Must be a boolean (true/false)")

        if self.python_type is int:
            try:
                return self.validate_and_normalize(int(stripped))
            except ValueError as e:
                raise ValueError("Must be an integer") from e

        if self.python_type is float:
            try:
                return self.validate_and_normalize(float(stripped))
            except ValueError as e:
                raise ValueError("Must be a number") from e

        return self.validate_and_normalize(stripped)


def _normalize_logo_path(value: str) -> str:
    from pathlib import Path

    from src.utils.logo import DEFAULT_BUILTIN_LOGO_PATH, coerce_logo_file, normalize_logo_setting_value

    candidate = (value or "").strip() or DEFAULT_BUILTIN_LOGO_PATH
    suffix = Path(candidate).suffix.lower()
    if suffix not in {".png", ".jpg", ".jpeg"}:
        raise ValueError("Logo must be an existing .png, .jpg, or .jpeg image file")
    if coerce_logo_file(candidate) is None:
        raise ValueError("Logo file not found or invalid (must be an existing .png, .jpg, or .jpeg image file)")
    return normalize_logo_setting_value(candidate)


# --- Branding normalizers ---


def _normalize_logo_position(value: str) -> str:
    allowed = {"top-right", "top-left", "bottom-right", "bottom-left"}
    v = value.strip().lower()
    if v not in allowed:
        raise ValueError(f"Must be one of: {', '.join(sorted(allowed))}")
    return v


def _normalize_logo_scale(value: float) -> float:
    if value < 0.01 or value > 1.0:
        raise ValueError("Must be between 0.01 and 1.0")
    return value


# --- Clip generation normalizers ---


def _normalize_positive_int(value: int) -> int:
    if value < 1:
        raise ValueError("Must be a positive integer (>= 1)")
    return value


def _normalize_non_negative_int(value: int) -> int:
    if value < 0:
        raise ValueError("Must be non-negative (>= 0)")
    return value


# --- Subtitle normalizers ---

SUBTITLE_PRESETS = {"default", "bold", "yellow", "tiktok", "small", "tiny"}


def _normalize_subtitle_mode(value: str) -> str:
    v = value.strip().lower()
    if v not in {"preset", "custom"}:
        raise ValueError("Must be 'preset' or 'custom'")
    return v


def _normalize_subtitle_preset(value: str) -> str:
    v = value.strip().lower()
    if v not in SUBTITLE_PRESETS:
        raise ValueError(f"Must be one of: {', '.join(sorted(SUBTITLE_PRESETS))}")
    return v


NAMED_COLORS = {"yellow", "white", "black", "red", "green", "blue", "cyan", "magenta"}


def _normalize_subtitle_color(value: str) -> str:
    v = value.strip().lower()
    if v in NAMED_COLORS:
        return v
    # Validate hex format #RRGGBB
    if v.startswith("#") and len(v) == 7:
        try:
            int(v[1:], 16)
            return v
        except ValueError:
            pass
    raise ValueError(f"Must be a color name ({', '.join(sorted(NAMED_COLORS))}) or #RRGGBB hex code")


def _normalize_font_size(value: int) -> int:
    if value < 8 or value > 72:
        raise ValueError("Font size must be between 8 and 72")
    return value


def _normalize_outline_width(value: int) -> int:
    if value < 0 or value > 10:
        raise ValueError("Outline width must be between 0 and 10")
    return value


def _normalize_shadow(value: int) -> int:
    if value < 0 or value > 5:
        raise ValueError("Shadow depth must be between 0 and 5")
    return value


def _normalize_positive_float(value: float) -> float:
    if value <= 0:
        raise ValueError("Must be a positive number")
    return value


# --- Export normalizers ---


def _normalize_aspect_ratio(value: str) -> str:
    v = value.strip()
    if v == "":
        return ""  # None/original
    allowed = {"16:9", "9:16", "1:1", "4:3", "3:4"}
    if v not in allowed:
        raise ValueError(f"Must be one of: {', '.join(sorted(allowed))} or blank for original")
    return v


def _normalize_crf(value: int) -> int:
    if value < 0 or value > 51:
        raise ValueError("CRF must be between 0 and 51")
    return value


def _normalize_face_tracking_strategy(value: str) -> str:
    v = value.strip().lower()
    if v not in {"keep_in_frame", "centered"}:
        raise ValueError("Must be 'keep_in_frame' or 'centered'")
    return v


def _normalize_sample_rate(value: int) -> int:
    if value < 1 or value > 30:
        raise ValueError("Sample rate must be between 1 and 30")
    return value


def _normalize_ffmpeg_threads(value: int) -> int:
    # 0 = auto-detect, positive = specific thread count, negative = all minus N
    if value < -16 or value > 64:
        raise ValueError("Threads must be between -16 and 64 (0=auto, negative=all minus N)")
    return value


# --- Output/naming normalizers ---


def _normalize_auto_name_method(value: str) -> str:
    """Normaliza el metodo de auto-nombrado de videos."""
    v = value.strip().lower()
    allowed = {"filename", "first_words", "llm_summary"}
    if v not in allowed:
        raise ValueError(f"Must be one of: {', '.join(sorted(allowed))}")
    return v


def _normalize_auto_name_word_count(value: int) -> int:
    """Normaliza el conteo de palabras para first_words."""
    if value < 1 or value > 15:
        raise ValueError("Word count must be between 1 and 15")
    return value


def _normalize_auto_name_max_chars(value: int) -> int:
    """Normaliza el maximo de caracteres para nombres."""
    if value < 10 or value > 100:
        raise ValueError("Max chars must be between 10 and 100")
    return value


def _normalize_output_dir(value: str) -> str:
    """Normaliza y valida el directorio de salida.

    - Handles both relative and absolute paths
    - Creates directory if it doesn't exist
    - Validates writability
    """
    from pathlib import Path
    import os

    path_str = (value or "").strip() or "output/"
    path = Path(path_str)

    # Resolve relative paths against cwd
    if not path.is_absolute():
        path = Path.cwd() / path
    path = path.resolve()

    # Create directory if it doesn't exist
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise ValueError(f"Cannot create output directory '{path}': {e}") from e

    # Validate writability
    if not os.access(path, os.W_OK):
        raise ValueError(f"Output directory is not writable: {path}")

    return str(path)


APP_SETTING_GROUPS: Tuple[SettingGroup, ...] = (
    SettingGroup(key="branding", title="Branding", description="Logo and watermark settings."),
    SettingGroup(key="clip_generation", title="Clip Generation", description="Duration and trimming settings for clip detection."),
    SettingGroup(key="subtitles", title="Subtitles", description="Subtitle styling and formatting."),
    SettingGroup(key="export", title="Export", description="Video export quality and processing options."),
    SettingGroup(key="output", title="Output", description="Video naming and output directory settings."),
)


APP_SETTINGS: Tuple[SettingDefinition, ...] = (
    # --- Branding settings ---
    SettingDefinition(
        key="logo_path",
        group="branding",
        label="Logo image file (.png/.jpg):",
        python_type=str,
        default="assets/logo.png",
        placeholder="assets/logo.png or /abs/path/logo.png",
        help_text="Used for video watermarking where supported. Must be a PNG or JPG file.",
        normalize=_normalize_logo_path,
    ),
    SettingDefinition(
        key="logo_position",
        group="branding",
        label="Logo position:",
        python_type=str,
        default="top-right",
        placeholder="top-right, top-left, bottom-right, bottom-left",
        help_text="Corner position for the logo overlay.",
        normalize=_normalize_logo_position,
    ),
    SettingDefinition(
        key="logo_scale",
        group="branding",
        label="Logo scale (0.01 - 1.0):",
        python_type=float,
        default=0.1,
        placeholder="0.1",
        help_text="Size of logo relative to video width (0.1 = 10%).",
        normalize=_normalize_logo_scale,
    ),
    # --- Clip generation settings ---
    SettingDefinition(
        key="min_clip_duration",
        group="clip_generation",
        label="Minimum clip duration (seconds):",
        python_type=int,
        default=30,
        placeholder="30",
        help_text="Shortest allowed clip length in seconds.",
        normalize=_normalize_positive_int,
    ),
    SettingDefinition(
        key="max_clip_duration",
        group="clip_generation",
        label="Maximum clip duration (seconds):",
        python_type=int,
        default=90,
        placeholder="90",
        help_text="Longest allowed clip length in seconds.",
        normalize=_normalize_positive_int,
    ),
    SettingDefinition(
        key="min_clips",
        group="clip_generation",
        label="Minimum number of clips:",
        python_type=int,
        default=3,
        placeholder="3",
        help_text="Minimum clips to generate per video.",
        normalize=_normalize_positive_int,
    ),
    SettingDefinition(
        key="max_clips",
        group="clip_generation",
        label="Maximum number of clips:",
        python_type=int,
        default=10,
        placeholder="10",
        help_text="Maximum clips to generate per video.",
        normalize=_normalize_positive_int,
    ),
    SettingDefinition(
        key="trim_ms_start",
        group="clip_generation",
        label="Max silence at start (ms):",
        python_type=int,
        default=1000,
        placeholder="1000",
        help_text="Maximum silence before speech. Trims only if exceeded (0=disabled).",
        normalize=_normalize_non_negative_int,
    ),
    SettingDefinition(
        key="trim_ms_end",
        group="clip_generation",
        label="Max silence at end (ms):",
        python_type=int,
        default=1000,
        placeholder="1000",
        help_text="Maximum silence after speech. Trims only if exceeded (0=disabled).",
        normalize=_normalize_non_negative_int,
    ),
    # --- Subtitle settings ---
    SettingDefinition(
        key="subtitle_style_mode",
        group="subtitles",
        label="Subtitle style mode:",
        python_type=str,
        default="preset",
        placeholder="preset or custom",
        help_text="Use 'preset' for built-in styles or 'custom' to define your own.",
        normalize=_normalize_subtitle_mode,
    ),
    SettingDefinition(
        key="subtitle_preset",
        group="subtitles",
        label="Preset style:",
        python_type=str,
        default="default",
        placeholder="default, bold, yellow, tiktok, small, tiny",
        help_text="Built-in subtitle style (only used when mode is 'preset').",
        normalize=_normalize_subtitle_preset,
    ),
    SettingDefinition(
        key="subtitle_font_family",
        group="subtitles",
        label="Font family:",
        python_type=str,
        default="Arial",
        placeholder="Arial",
        help_text="Font family for custom subtitles (used when mode is 'custom').",
    ),
    SettingDefinition(
        key="subtitle_font_size",
        group="subtitles",
        label="Font size:",
        python_type=int,
        default=18,
        placeholder="18",
        help_text="Font size in points for custom subtitles (used when mode is 'custom').",
        normalize=_normalize_font_size,
    ),
    SettingDefinition(
        key="subtitle_primary_color",
        group="subtitles",
        label="Primary color:",
        python_type=str,
        default="yellow",
        placeholder="yellow, white, red, #RRGGBB",
        help_text="Text color for custom subtitles (name or hex, used when mode is 'custom').",
        normalize=_normalize_subtitle_color,
    ),
    SettingDefinition(
        key="subtitle_outline_color",
        group="subtitles",
        label="Outline color:",
        python_type=str,
        default="black",
        placeholder="black, white, #RRGGBB",
        help_text="Outline/border color for custom subtitles (used when mode is 'custom').",
        normalize=_normalize_subtitle_color,
    ),
    SettingDefinition(
        key="subtitle_outline_width",
        group="subtitles",
        label="Outline width:",
        python_type=int,
        default=2,
        placeholder="2",
        help_text="Outline thickness 0-10 (used when mode is 'custom').",
        normalize=_normalize_outline_width,
    ),
    SettingDefinition(
        key="subtitle_shadow",
        group="subtitles",
        label="Shadow depth:",
        python_type=int,
        default=1,
        placeholder="1",
        help_text="Shadow depth 0-5 (used when mode is 'custom').",
        normalize=_normalize_shadow,
    ),
    SettingDefinition(
        key="subtitle_bold",
        group="subtitles",
        label="Bold:",
        python_type=bool,
        default=False,
        placeholder="true or false",
        help_text="Enable bold text for custom subtitles (used when mode is 'custom').",
    ),
    SettingDefinition(
        key="subtitle_max_chars_per_line",
        group="subtitles",
        label="Max characters per line:",
        python_type=int,
        default=42,
        placeholder="42",
        help_text="Maximum characters before line wrap.",
        normalize=_normalize_positive_int,
    ),
    SettingDefinition(
        key="subtitle_max_duration",
        group="subtitles",
        label="Max subtitle duration (seconds):",
        python_type=float,
        default=5.0,
        placeholder="5.0",
        help_text="Maximum duration a single subtitle stays on screen.",
        normalize=_normalize_positive_float,
    ),
    # --- Export settings ---
    SettingDefinition(
        key="default_aspect_ratio",
        group="export",
        label="Default aspect ratio:",
        python_type=str,
        default="",
        placeholder="16:9, 9:16, 1:1 or blank for original",
        help_text="Default aspect ratio for exports. Leave blank to keep original.",
        normalize=_normalize_aspect_ratio,
    ),
    SettingDefinition(
        key="video_crf",
        group="export",
        label="Video quality (CRF):",
        python_type=int,
        default=23,
        placeholder="23",
        help_text="Constant Rate Factor 0-51. Lower = better quality, larger file. 18-23 recommended.",
        normalize=_normalize_crf,
    ),
    SettingDefinition(
        key="ffmpeg_threads",
        group="export",
        label="FFmpeg threads:",
        python_type=int,
        default=0,
        placeholder="0",
        help_text="Thread count: 0=auto, 7=use 7 threads, -2=all CPUs minus 2.",
        normalize=_normalize_ffmpeg_threads,
    ),
    SettingDefinition(
        key="enable_face_tracking",
        group="export",
        label="Enable face tracking:",
        python_type=bool,
        default=False,
        placeholder="true or false",
        help_text="Use face detection for dynamic reframing (9:16 only).",
    ),
    SettingDefinition(
        key="face_tracking_strategy",
        group="export",
        label="Face tracking strategy:",
        python_type=str,
        default="keep_in_frame",
        placeholder="keep_in_frame or centered",
        help_text="'keep_in_frame' for less movement, 'centered' to always center face.",
        normalize=_normalize_face_tracking_strategy,
    ),
    SettingDefinition(
        key="face_tracking_sample_rate",
        group="export",
        label="Face tracking sample rate:",
        python_type=int,
        default=3,
        placeholder="3",
        help_text="Process every Nth frame (higher = faster but less smooth).",
        normalize=_normalize_sample_rate,
    ),
    # --- Output settings ---
    SettingDefinition(
        key="output_dir",
        group="output",
        label="Output directory:",
        python_type=str,
        default="output/",
        placeholder="output/ or /absolute/path/to/output",
        help_text="Base directory for exported videos. Relative paths are resolved from current working directory.",
        normalize=_normalize_output_dir,
    ),
    SettingDefinition(
        key="auto_name_method",
        group="output",
        label="Auto-name method:",
        python_type=str,
        default="filename",
        placeholder="filename, first_words, llm_summary",
        help_text="How to generate video names: 'filename'=original, 'first_words'=from transcript, 'llm_summary'=AI generated.",
        normalize=_normalize_auto_name_method,
    ),
    SettingDefinition(
        key="auto_name_max_chars",
        group="output",
        label="Max name length:",
        python_type=int,
        default=40,
        placeholder="40",
        help_text="Maximum characters in auto-generated video names.",
        normalize=_normalize_auto_name_max_chars,
    ),
    SettingDefinition(
        key="auto_name_word_count",
        group="output",
        label="Word count (first_words):",
        python_type=int,
        default=5,
        placeholder="5",
        help_text="Number of words to extract from transcript (only for first_words method).",
        normalize=_normalize_auto_name_word_count,
    ),
)


def iter_app_setting_groups() -> Sequence[SettingGroup]:
    return list(APP_SETTING_GROUPS)


def iter_app_settings() -> Sequence[SettingDefinition]:
    return list(APP_SETTINGS)


def get_app_setting_definition(key: str) -> Optional[SettingDefinition]:
    for setting in APP_SETTINGS:
        if setting.key == key:
            return setting
    return None


def get_app_settings_defaults() -> Dict[str, Any]:
    return {s.key: s.default for s in APP_SETTINGS}


def validate_and_normalize_app_settings(settings: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, str]]:
    """
    Validates known keys and returns:
    - validated settings dict with schema defaults merged
    - per-key error strings for any invalid persisted values

    Unknown keys are preserved as-is.
    """
    merged: Dict[str, Any] = dict(get_app_settings_defaults())
    merged.update(settings or {})

    errors: Dict[str, str] = {}
    for definition in APP_SETTINGS:
        if definition.key not in merged:
            merged[definition.key] = definition.default
            continue
        try:
            merged[definition.key] = definition.validate_and_normalize(merged.get(definition.key))
        except Exception as e:
            errors[definition.key] = str(e).strip() or "Invalid value"
            merged[definition.key] = definition.default
    return merged, errors


def list_app_settings_by_group() -> Dict[str, List[SettingDefinition]]:
    grouped: Dict[str, List[SettingDefinition]] = {}
    for definition in APP_SETTINGS:
        grouped.setdefault(definition.group, []).append(definition)
    return grouped


# --- Custom subtitle style helpers ---

# ASS color format: &H00BBGGRR (alpha, blue, green, red)
COLOR_MAP: Dict[str, str] = {
    "yellow": "&H0000FFFF",
    "white": "&H00FFFFFF",
    "black": "&H00000000",
    "red": "&H000000FF",
    "green": "&H0000FF00",
    "blue": "&H00FF0000",
    "cyan": "&H00FFFF00",
    "magenta": "&H00FF00FF",
}


def _hex_to_ass_color(hex_color: str) -> str:
    """Convert #RRGGBB to ASS format &H00BBGGRR."""
    r = int(hex_color[1:3], 16)
    g = int(hex_color[3:5], 16)
    b = int(hex_color[5:7], 16)
    return f"&H00{b:02X}{g:02X}{r:02X}"


def build_custom_subtitle_style(settings: Dict[str, Any]) -> Dict[str, str]:
    """
    Build an ffmpeg subtitle style dict from settings.

    Returns a dict compatible with video_exporter's styles format:
    {
        "FontName": "Arial",
        "FontSize": "18",
        "PrimaryColour": "&H0000FFFF",
        "OutlineColour": "&H00000000",
        "Outline": "2",
        "Shadow": "1",
        "Bold": "0",
    }
    """
    primary = settings.get("subtitle_primary_color", "yellow")
    if primary.startswith("#"):
        primary_color = _hex_to_ass_color(primary)
    else:
        primary_color = COLOR_MAP.get(primary, COLOR_MAP["yellow"])

    outline = settings.get("subtitle_outline_color", "black")
    if outline.startswith("#"):
        outline_color = _hex_to_ass_color(outline)
    else:
        outline_color = COLOR_MAP.get(outline, COLOR_MAP["black"])

    return {
        "FontName": settings.get("subtitle_font_family", "Arial"),
        "FontSize": str(settings.get("subtitle_font_size", 18)),
        "PrimaryColour": primary_color,
        "OutlineColour": outline_color,
        "Outline": str(settings.get("subtitle_outline_width", 2)),
        "Shadow": str(settings.get("subtitle_shadow", 1)),
        "Bold": "-1" if settings.get("subtitle_bold", False) else "0",
    }


def get_effective_subtitle_style(settings: Dict[str, Any]) -> str:
    """
    Returns either the preset name or '__custom__' to signal custom mode.
    """
    mode = settings.get("subtitle_style_mode", "preset")
    if mode == "custom":
        return "__custom__"
    return settings.get("subtitle_preset", "default")
