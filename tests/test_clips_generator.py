# -*- coding: utf-8 -*-
"""
Tests for ClipsGenerator - Automatic clip segmentation using ClipsAI
"""

import json
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

import pytest

clipsai = pytest.importorskip("clipsai", reason="clipsai not importable (missing system lib, e.g. libGL)")
from src.clips_generator import ClipsGenerator, generate_clips_from_transcript


# ============================================================================
# MOCK FIXTURES
# ============================================================================


@pytest.fixture
def mock_clip():
    """Create a mock Clip object with start_time and end_time attributes."""
    clip = MagicMock()
    clip.start_time = 0.0
    clip.end_time = 45.0
    return clip


@pytest.fixture
def mock_clip_finder():
    """Create a mock ClipFinder that returns configurable clips."""
    with patch("src.clips_generator.ClipFinder") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_transcription():
    """Create a mock Transcription class."""
    with patch("src.clips_generator.Transcription") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance
        yield mock_class


@pytest.fixture
def transcript_with_words() -> Dict:
    """WhisperX transcript with word-level timestamps."""
    return {
        "video_id": "test_video_001",
        "language": "es",
        "segments": [
            {
                "start": 0.0,
                "end": 5.0,
                "text": "Bienvenidos al programa de hoy",
                "words": [
                    {"word": "Bienvenidos", "start": 0.0, "end": 1.0},
                    {"word": "al", "start": 1.1, "end": 1.3},
                    {"word": "programa", "start": 1.4, "end": 2.2},
                    {"word": "de", "start": 2.3, "end": 2.5},
                    {"word": "hoy", "start": 2.6, "end": 5.0},
                ],
            },
            {
                "start": 5.5,
                "end": 10.0,
                "text": "Vamos a hablar de inteligencia artificial",
                "words": [
                    {"word": "Vamos", "start": 5.5, "end": 6.0},
                    {"word": "a", "start": 6.1, "end": 6.2},
                    {"word": "hablar", "start": 6.3, "end": 6.8},
                    {"word": "de", "start": 6.9, "end": 7.0},
                    {"word": "inteligencia", "start": 7.1, "end": 8.0},
                    {"word": "artificial", "start": 8.1, "end": 10.0},
                ],
            },
            {
                "start": 35.0,
                "end": 40.0,
                "text": "Este es un segmento adicional",
                "words": [
                    {"word": "Este", "start": 35.0, "end": 35.5},
                    {"word": "es", "start": 35.6, "end": 35.8},
                    {"word": "un", "start": 35.9, "end": 36.1},
                    {"word": "segmento", "start": 36.2, "end": 37.5},
                    {"word": "adicional", "start": 37.6, "end": 40.0},
                ],
            },
            {
                "start": 60.0,
                "end": 65.0,
                "text": "Llegamos al final del video",
                "words": [
                    {"word": "Llegamos", "start": 60.0, "end": 60.8},
                    {"word": "al", "start": 60.9, "end": 61.1},
                    {"word": "final", "start": 61.2, "end": 62.0},
                    {"word": "del", "start": 62.1, "end": 62.4},
                    {"word": "video", "start": 62.5, "end": 65.0},
                ],
            },
        ],
    }


@pytest.fixture
def transcript_without_words() -> Dict:
    """WhisperX transcript without word-level timestamps (segment-only)."""
    return {
        "video_id": "test_video_002",
        "language": "en",
        "segments": [
            {"start": 0.0, "end": 5.0, "text": "Hello world"},
            {"start": 5.5, "end": 10.0, "text": "This is a test"},
        ],
    }


@pytest.fixture
def long_transcript() -> Dict:
    """Long transcript for testing fixed time clip generation."""
    segments = []
    for i in range(20):
        start = i * 10.0
        end = start + 9.5
        segments.append({
            "start": start,
            "end": end,
            "text": f"Segment number {i + 1} with some content",
            "words": [
                {"word": f"Segment", "start": start, "end": start + 1.0},
                {"word": "number", "start": start + 1.1, "end": start + 2.0},
                {"word": str(i + 1), "start": start + 2.1, "end": start + 3.0},
            ],
        })
    return {
        "video_id": "long_video",
        "language": "en",
        "segments": segments,
    }


# ============================================================================
# TEST: ClipsGenerator.__init__()
# ============================================================================


class TestClipsGeneratorInit:
    """Tests for ClipsGenerator initialization."""

    def test_init_default_parameters(self, mock_clip_finder):
        """ClipsGenerator initializes with default duration values."""
        generator = ClipsGenerator()

        assert generator.min_clip_duration == 30
        assert generator.max_clip_duration == 90

    def test_init_custom_parameters(self, mock_clip_finder):
        """ClipsGenerator accepts custom min/max duration parameters."""
        generator = ClipsGenerator(min_clip_duration=15, max_clip_duration=120)

        assert generator.min_clip_duration == 15
        assert generator.max_clip_duration == 120

    def test_init_creates_clip_finder(self):
        """ClipsGenerator creates ClipFinder with correct parameters."""
        with patch("src.clips_generator.ClipFinder") as mock_class:
            generator = ClipsGenerator(min_clip_duration=20, max_clip_duration=60)

            mock_class.assert_called_once_with(
                min_clip_duration=20,
                max_clip_duration=60
            )


# ============================================================================
# TEST: _load_transcript()
# ============================================================================


class TestLoadTranscript:
    """Tests for transcript loading functionality."""

    def test_load_transcript_valid_json(
        self, tmp_project_dir, mock_clip_finder, transcript_with_words
    ):
        """_load_transcript loads valid JSON transcript successfully."""
        # Create transcript file
        transcript_path = tmp_project_dir / "temp" / "test_transcript.json"
        transcript_path.write_text(
            json.dumps(transcript_with_words), encoding="utf-8"
        )

        generator = ClipsGenerator()
        result = generator._load_transcript(str(transcript_path))

        assert result is not None
        assert result["video_id"] == "test_video_001"
        assert result["language"] == "es"
        assert len(result["segments"]) == 4

    def test_load_transcript_missing_file(self, tmp_project_dir, mock_clip_finder):
        """_load_transcript returns None for non-existent file."""
        generator = ClipsGenerator()
        result = generator._load_transcript("/nonexistent/path/transcript.json")

        assert result is None

    def test_load_transcript_invalid_json(self, tmp_project_dir, mock_clip_finder):
        """_load_transcript returns None for invalid JSON."""
        # Create invalid JSON file
        transcript_path = tmp_project_dir / "temp" / "invalid.json"
        transcript_path.write_text("{ invalid json content }", encoding="utf-8")

        generator = ClipsGenerator()
        result = generator._load_transcript(str(transcript_path))

        assert result is None

    def test_load_transcript_empty_file(self, tmp_project_dir, mock_clip_finder):
        """_load_transcript handles empty file gracefully."""
        transcript_path = tmp_project_dir / "temp" / "empty.json"
        transcript_path.write_text("", encoding="utf-8")

        generator = ClipsGenerator()
        result = generator._load_transcript(str(transcript_path))

        assert result is None


# ============================================================================
# TEST: _convert_to_clipsai_format()
# ============================================================================


class TestConvertToClipsaiFormat:
    """Tests for WhisperX to ClipsAI format conversion."""

    def test_convert_with_words(
        self, mock_clip_finder, mock_transcription, transcript_with_words
    ):
        """_convert_to_clipsai_format processes segments with word-level timestamps."""
        generator = ClipsGenerator()
        result = generator._convert_to_clipsai_format(transcript_with_words)

        assert result is not None
        # Verify Transcription was called with a dict containing char_info
        mock_transcription.assert_called_once()
        call_args = mock_transcription.call_args[0][0]
        assert "char_info" in call_args
        assert "source_software" in call_args
        assert call_args["source_software"] == "whisperx"
        assert call_args["language"] == "es"

    def test_convert_without_words(
        self, mock_clip_finder, mock_transcription, transcript_without_words
    ):
        """_convert_to_clipsai_format processes segments without word timestamps."""
        generator = ClipsGenerator()
        result = generator._convert_to_clipsai_format(transcript_without_words)

        assert result is not None
        mock_transcription.assert_called_once()
        call_args = mock_transcription.call_args[0][0]
        # Characters should be extracted from segment text
        assert len(call_args["char_info"]) > 0

    def test_convert_empty_segments(self, mock_clip_finder, mock_transcription):
        """_convert_to_clipsai_format returns None for empty segments."""
        generator = ClipsGenerator()
        result = generator._convert_to_clipsai_format({"segments": []})

        assert result is None

    def test_convert_no_segments_key(self, mock_clip_finder, mock_transcription):
        """_convert_to_clipsai_format returns None when segments key is missing."""
        generator = ClipsGenerator()
        result = generator._convert_to_clipsai_format({})

        assert result is None

    def test_convert_default_language(self, mock_clip_finder, mock_transcription):
        """_convert_to_clipsai_format uses 'en' as default language."""
        transcript_no_lang = {
            "segments": [
                {"start": 0.0, "end": 5.0, "text": "Test"}
            ]
        }
        generator = ClipsGenerator()
        generator._convert_to_clipsai_format(transcript_no_lang)

        call_args = mock_transcription.call_args[0][0]
        assert call_args["language"] == "en"


# ============================================================================
# TEST: _get_text_for_timerange()
# ============================================================================


class TestGetTextForTimerange:
    """Tests for extracting text within a time range."""

    def test_get_text_overlapping_segments(
        self, mock_clip_finder, transcript_with_words
    ):
        """_get_text_for_timerange extracts text from overlapping segments."""
        generator = ClipsGenerator()
        result = generator._get_text_for_timerange(
            transcript_with_words, start_time=0.0, end_time=12.0
        )

        assert "Bienvenidos al programa de hoy" in result
        assert "Vamos a hablar de inteligencia artificial" in result

    def test_get_text_single_segment(self, mock_clip_finder, transcript_with_words):
        """_get_text_for_timerange extracts text from a single segment."""
        generator = ClipsGenerator()
        result = generator._get_text_for_timerange(
            transcript_with_words, start_time=0.0, end_time=5.0
        )

        assert "Bienvenidos al programa de hoy" in result
        # Second segment should not be included (starts at 5.5)
        assert "Vamos" not in result

    def test_get_text_no_overlap(self, mock_clip_finder, transcript_with_words):
        """_get_text_for_timerange returns empty string for non-overlapping range."""
        generator = ClipsGenerator()
        result = generator._get_text_for_timerange(
            transcript_with_words, start_time=20.0, end_time=30.0
        )

        assert result == ""

    def test_get_text_partial_overlap_start(
        self, mock_clip_finder, transcript_with_words
    ):
        """_get_text_for_timerange includes segment with partial overlap at start."""
        generator = ClipsGenerator()
        # Range starts in the middle of segment 2 (5.5-10.0)
        result = generator._get_text_for_timerange(
            transcript_with_words, start_time=7.0, end_time=12.0
        )

        assert "Vamos a hablar de inteligencia artificial" in result

    def test_get_text_partial_overlap_end(
        self, mock_clip_finder, transcript_with_words
    ):
        """_get_text_for_timerange includes segment with partial overlap at end."""
        generator = ClipsGenerator()
        # Range ends in the middle of segment 1 (0.0-5.0)
        result = generator._get_text_for_timerange(
            transcript_with_words, start_time=0.0, end_time=3.0
        )

        assert "Bienvenidos al programa de hoy" in result

    def test_get_text_empty_segments(self, mock_clip_finder):
        """_get_text_for_timerange handles transcript with no segments."""
        generator = ClipsGenerator()
        result = generator._get_text_for_timerange(
            {"segments": []}, start_time=0.0, end_time=10.0
        )

        assert result == ""

    def test_get_text_boundary_conditions(
        self, mock_clip_finder, transcript_with_words
    ):
        """_get_text_for_timerange handles exact boundary conditions."""
        generator = ClipsGenerator()
        # Query exactly at segment boundary
        result = generator._get_text_for_timerange(
            transcript_with_words, start_time=5.0, end_time=5.5
        )

        # First segment ends at 5.0, second starts at 5.5
        # Neither should be included with strict < and > comparison
        assert result == ""


# ============================================================================
# TEST: _format_time()
# ============================================================================


class TestFormatTime:
    """Tests for time formatting utility."""

    def test_format_time_zero(self, mock_clip_finder):
        """_format_time formats zero seconds correctly."""
        generator = ClipsGenerator()
        assert generator._format_time(0.0) == "00:00"

    def test_format_time_seconds_only(self, mock_clip_finder):
        """_format_time formats seconds under one minute."""
        generator = ClipsGenerator()
        assert generator._format_time(45.5) == "00:45"

    def test_format_time_minutes_and_seconds(self, mock_clip_finder):
        """_format_time formats minutes and seconds correctly."""
        generator = ClipsGenerator()
        assert generator._format_time(125.5) == "02:05"

    def test_format_time_exact_minute(self, mock_clip_finder):
        """_format_time formats exact minutes correctly."""
        generator = ClipsGenerator()
        assert generator._format_time(60.0) == "01:00"

    def test_format_time_large_value(self, mock_clip_finder):
        """_format_time handles large values correctly."""
        generator = ClipsGenerator()
        assert generator._format_time(3661.0) == "61:01"


# ============================================================================
# TEST: generate_clips() with mocked ClipFinder
# ============================================================================


class TestGenerateClips:
    """Tests for the main clip generation workflow."""

    def test_generate_clips_success(
        self, tmp_project_dir, mock_clip_finder, transcript_with_words
    ):
        """generate_clips returns formatted clips when ClipFinder finds clips."""
        # Setup transcript file
        transcript_path = tmp_project_dir / "temp" / "transcript.json"
        transcript_path.write_text(
            json.dumps(transcript_with_words), encoding="utf-8"
        )

        # Create mock clips
        mock_clip1 = MagicMock()
        mock_clip1.start_time = 0.0
        mock_clip1.end_time = 45.0

        mock_clip2 = MagicMock()
        mock_clip2.start_time = 45.0
        mock_clip2.end_time = 90.0

        mock_clip_finder.find_clips.return_value = [mock_clip1, mock_clip2]

        with patch("src.clips_generator.Transcription"):
            generator = ClipsGenerator()
            result = generator.generate_clips(str(transcript_path))

        assert result is not None
        assert len(result) == 2
        assert result[0]["clip_id"] == 1
        assert result[0]["start_time"] == 0.0
        assert result[0]["end_time"] == 45.0
        assert result[0]["duration"] == 45.0
        assert result[0]["method"] == "clipsai"

    def test_generate_clips_missing_transcript(
        self, tmp_project_dir, mock_clip_finder
    ):
        """generate_clips returns None when transcript file is missing."""
        generator = ClipsGenerator()
        result = generator.generate_clips("/nonexistent/transcript.json")

        assert result is None

    def test_generate_clips_fallback_to_fixed_time(
        self, tmp_project_dir, mock_clip_finder, long_transcript
    ):
        """generate_clips falls back to fixed time clips when ClipFinder returns empty."""
        transcript_path = tmp_project_dir / "temp" / "transcript.json"
        transcript_path.write_text(
            json.dumps(long_transcript), encoding="utf-8"
        )

        # ClipFinder returns empty list
        mock_clip_finder.find_clips.return_value = []

        with patch("src.clips_generator.Transcription"):
            generator = ClipsGenerator(min_clip_duration=30, max_clip_duration=60)
            result = generator.generate_clips(str(transcript_path))

        assert result is not None
        # Should have clips generated with fixed time method
        for clip in result:
            assert clip["method"] == "fixed_time"

    def test_generate_clips_respects_max_clips(
        self, tmp_project_dir, mock_clip_finder, transcript_with_words
    ):
        """generate_clips respects max_clips parameter."""
        transcript_path = tmp_project_dir / "temp" / "transcript.json"
        transcript_path.write_text(
            json.dumps(transcript_with_words), encoding="utf-8"
        )

        # Create many mock clips
        mock_clips = []
        for i in range(10):
            clip = MagicMock()
            clip.start_time = i * 30.0
            clip.end_time = (i + 1) * 30.0
            mock_clips.append(clip)

        mock_clip_finder.find_clips.return_value = mock_clips

        with patch("src.clips_generator.Transcription"):
            generator = ClipsGenerator()
            result = generator.generate_clips(str(transcript_path), max_clips=3)

        assert len(result) == 3

    def test_generate_clips_warns_below_min_clips(
        self, tmp_project_dir, mock_clip_finder, transcript_with_words
    ):
        """generate_clips logs warning when fewer clips than min_clips are found."""
        transcript_path = tmp_project_dir / "temp" / "transcript.json"
        transcript_path.write_text(
            json.dumps(transcript_with_words), encoding="utf-8"
        )

        mock_clip = MagicMock()
        mock_clip.start_time = 0.0
        mock_clip.end_time = 45.0
        mock_clip_finder.find_clips.return_value = [mock_clip]

        with patch("src.clips_generator.Transcription"):
            generator = ClipsGenerator()
            result = generator.generate_clips(
                str(transcript_path), min_clips=5, max_clips=10
            )

        # Should still return the clips found
        assert result is not None
        assert len(result) == 1

    def test_generate_clips_includes_text_preview(
        self, tmp_project_dir, mock_clip_finder, transcript_with_words
    ):
        """generate_clips includes text_preview and full_text in clip data."""
        transcript_path = tmp_project_dir / "temp" / "transcript.json"
        transcript_path.write_text(
            json.dumps(transcript_with_words), encoding="utf-8"
        )

        mock_clip = MagicMock()
        mock_clip.start_time = 0.0
        mock_clip.end_time = 12.0
        mock_clip_finder.find_clips.return_value = [mock_clip]

        with patch("src.clips_generator.Transcription"):
            generator = ClipsGenerator()
            result = generator.generate_clips(str(transcript_path))

        assert "text_preview" in result[0]
        assert "full_text" in result[0]
        assert "Bienvenidos" in result[0]["full_text"]


# ============================================================================
# TEST: _generate_fixed_time_clips()
# ============================================================================


class TestGenerateFixedTimeClips:
    """Tests for fixed duration clip generation fallback."""

    def test_fixed_time_clips_basic(self, mock_clip_finder, long_transcript):
        """_generate_fixed_time_clips divides video into fixed duration clips."""
        generator = ClipsGenerator()
        result = generator._generate_fixed_time_clips(
            long_transcript, clip_duration=60, max_clips=5
        )

        assert result is not None
        assert len(result) <= 5
        for clip in result:
            assert clip["method"] == "fixed_time"
            assert clip["duration"] >= 30  # Minimum duration requirement

    def test_fixed_time_clips_empty_segments(self, mock_clip_finder):
        """_generate_fixed_time_clips returns None for empty segments."""
        generator = ClipsGenerator()
        result = generator._generate_fixed_time_clips(
            {"segments": []}, clip_duration=60
        )

        assert result is None

    def test_fixed_time_clips_respects_max_clips(
        self, mock_clip_finder, long_transcript
    ):
        """_generate_fixed_time_clips respects max_clips parameter."""
        generator = ClipsGenerator()
        result = generator._generate_fixed_time_clips(
            long_transcript, clip_duration=30, max_clips=3
        )

        assert len(result) <= 3

    def test_fixed_time_clips_skips_short_clips(self, mock_clip_finder):
        """_generate_fixed_time_clips skips clips shorter than 30 seconds."""
        # Create a transcript with total duration less than 60s
        short_transcript = {
            "segments": [
                {"start": 0.0, "end": 25.0, "text": "Short segment"}
            ]
        }

        generator = ClipsGenerator()
        result = generator._generate_fixed_time_clips(
            short_transcript, clip_duration=60
        )

        # Should return None because the only possible clip is < 30s
        assert result is None

    def test_fixed_time_clips_includes_text(
        self, mock_clip_finder, long_transcript
    ):
        """_generate_fixed_time_clips extracts text for each clip."""
        generator = ClipsGenerator()
        result = generator._generate_fixed_time_clips(
            long_transcript, clip_duration=60, max_clips=2
        )

        assert result is not None
        for clip in result:
            assert "text_preview" in clip
            assert "full_text" in clip


# ============================================================================
# TEST: save_clips_metadata() and load_clips_metadata()
# ============================================================================


class TestClipsMetadataPersistence:
    """Tests for saving and loading clip metadata."""

    def test_save_clips_metadata_default_path(
        self, tmp_project_dir, mock_clip_finder
    ):
        """save_clips_metadata saves to default temp/{video_id}_clips.json."""
        generator = ClipsGenerator()
        clips = [
            {"clip_id": 1, "start_time": 0.0, "end_time": 45.0, "duration": 45.0}
        ]

        result_path = generator.save_clips_metadata(clips, "test_video_001")

        assert result_path is not None
        assert Path(result_path).exists()

        # Verify contents
        with open(result_path, "r", encoding="utf-8") as f:
            saved_data = json.load(f)

        assert saved_data["video_id"] == "test_video_001"
        assert saved_data["num_clips"] == 1
        assert len(saved_data["clips"]) == 1

    def test_save_clips_metadata_custom_path(
        self, tmp_project_dir, mock_clip_finder
    ):
        """save_clips_metadata saves to custom output path."""
        generator = ClipsGenerator()
        clips = [{"clip_id": 1, "duration": 45.0}]
        custom_path = str(tmp_project_dir / "custom" / "clips.json")

        result_path = generator.save_clips_metadata(
            clips, "test_video", output_path=custom_path
        )

        assert result_path == custom_path
        assert Path(custom_path).exists()

    def test_save_clips_metadata_includes_duration_settings(
        self, tmp_project_dir, mock_clip_finder
    ):
        """save_clips_metadata includes min/max duration settings."""
        generator = ClipsGenerator(min_clip_duration=20, max_clip_duration=120)
        clips = [{"clip_id": 1}]

        result_path = generator.save_clips_metadata(clips, "test_video")

        with open(result_path, "r", encoding="utf-8") as f:
            saved_data = json.load(f)

        assert saved_data["min_clip_duration"] == 20
        assert saved_data["max_clip_duration"] == 120

    def test_load_clips_metadata_success(self, tmp_project_dir, mock_clip_finder):
        """load_clips_metadata loads previously saved metadata."""
        generator = ClipsGenerator()
        clips = [
            {"clip_id": 1, "start_time": 0.0, "end_time": 45.0},
            {"clip_id": 2, "start_time": 45.0, "end_time": 90.0},
        ]

        saved_path = generator.save_clips_metadata(clips, "test_video")
        loaded_data = generator.load_clips_metadata(saved_path)

        assert loaded_data is not None
        assert loaded_data["video_id"] == "test_video"
        assert len(loaded_data["clips"]) == 2

    def test_load_clips_metadata_missing_file(
        self, tmp_project_dir, mock_clip_finder
    ):
        """load_clips_metadata returns None for non-existent file."""
        generator = ClipsGenerator()
        result = generator.load_clips_metadata("/nonexistent/clips.json")

        assert result is None

    def test_save_load_roundtrip(self, tmp_project_dir, mock_clip_finder):
        """save and load clips_metadata preserves all clip data."""
        generator = ClipsGenerator(min_clip_duration=25, max_clip_duration=100)
        original_clips = [
            {
                "clip_id": 1,
                "start_time": 0.0,
                "end_time": 45.0,
                "duration": 45.0,
                "text_preview": "Preview text",
                "full_text": "Full text content",
                "method": "clipsai",
            },
            {
                "clip_id": 2,
                "start_time": 45.0,
                "end_time": 90.0,
                "duration": 45.0,
                "text_preview": "Second preview",
                "full_text": "Second full text",
                "method": "fixed_time",
            },
        ]

        saved_path = generator.save_clips_metadata(original_clips, "roundtrip_test")
        loaded_data = generator.load_clips_metadata(saved_path)

        assert loaded_data["video_id"] == "roundtrip_test"
        assert loaded_data["num_clips"] == 2
        assert loaded_data["min_clip_duration"] == 25
        assert loaded_data["max_clip_duration"] == 100

        for i, clip in enumerate(loaded_data["clips"]):
            assert clip["clip_id"] == original_clips[i]["clip_id"]
            assert clip["start_time"] == original_clips[i]["start_time"]
            assert clip["end_time"] == original_clips[i]["end_time"]
            assert clip["method"] == original_clips[i]["method"]


# ============================================================================
# TEST: generate_clips_from_transcript() helper function
# ============================================================================


class TestGenerateClipsFromTranscript:
    """Tests for the convenience helper function."""

    def test_helper_function_basic(
        self, tmp_project_dir, mock_clip_finder, transcript_with_words
    ):
        """generate_clips_from_transcript creates generator and calls generate_clips."""
        transcript_path = tmp_project_dir / "temp" / "transcript.json"
        transcript_path.write_text(
            json.dumps(transcript_with_words), encoding="utf-8"
        )

        mock_clip = MagicMock()
        mock_clip.start_time = 0.0
        mock_clip.end_time = 45.0
        mock_clip_finder.find_clips.return_value = [mock_clip]

        with patch("src.clips_generator.Transcription"):
            result = generate_clips_from_transcript(str(transcript_path))

        assert result is not None
        assert len(result) >= 1

    def test_helper_function_custom_parameters(
        self, tmp_project_dir, transcript_with_words
    ):
        """generate_clips_from_transcript passes custom parameters correctly."""
        transcript_path = tmp_project_dir / "temp" / "transcript.json"
        transcript_path.write_text(
            json.dumps(transcript_with_words), encoding="utf-8"
        )

        with patch("src.clips_generator.ClipFinder") as mock_finder_class:
            mock_instance = MagicMock()
            mock_instance.find_clips.return_value = []
            mock_finder_class.return_value = mock_instance

            with patch("src.clips_generator.Transcription"):
                generate_clips_from_transcript(
                    str(transcript_path),
                    min_clips=2,
                    max_clips=5,
                    min_duration=15,
                    max_duration=120,
                )

            # Verify ClipFinder was created with custom duration parameters
            mock_finder_class.assert_called_once_with(
                min_clip_duration=15,
                max_clip_duration=120
            )

    def test_helper_function_returns_none_on_error(self, tmp_project_dir):
        """generate_clips_from_transcript returns None when transcript is missing."""
        with patch("src.clips_generator.ClipFinder"):
            result = generate_clips_from_transcript("/nonexistent/path.json")

        assert result is None
