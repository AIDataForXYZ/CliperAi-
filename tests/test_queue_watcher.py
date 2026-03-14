"""Tests for the queue watcher bridge."""

import json
import os
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# We need the queue schema from shorts-analytics
QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    status          TEXT NOT NULL DEFAULT 'processing',
    source_video    TEXT,
    filename        TEXT,
    platforms       TEXT DEFAULT '[]',
    uploaded_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    uploaded_by     INTEGER,
    clip_path       TEXT,
    caption         TEXT,
    clip_metadata   TEXT DEFAULT '{}',
    parent_id       INTEGER,
    reviewed_at     DATETIME,
    reviewed_by     INTEGER,
    review_notes    TEXT,
    integration_id  TEXT,
    channel         TEXT,
    content_type    TEXT,
    posted_at       DATETIME,
    postiz_post_id  TEXT,
    error           TEXT
);
"""


@pytest.fixture
def queue_db(tmp_path):
    """Create a temporary queue database."""
    db_path = str(tmp_path / "test_queue.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(QUEUE_SCHEMA)
    conn.close()
    return db_path


@pytest.fixture
def sample_video(tmp_path):
    """Create a fake video file."""
    video = tmp_path / "test_video.mp4"
    video.write_bytes(b"fake video content")
    return str(video)


@pytest.fixture
def clips_dir(tmp_path):
    """Create a clips output directory."""
    d = tmp_path / "clips"
    d.mkdir()
    return str(d)


def insert_processing_job(db_path: str, source_video: str, platforms: list[str] = None) -> int:
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "INSERT INTO queue (status, source_video, filename, platforms) VALUES (?, ?, ?, ?)",
        ("processing", source_video, os.path.basename(source_video), json.dumps(platforms or ["tiktok"])),
    )
    conn.commit()
    item_id = cur.lastrowid
    conn.close()
    return item_id


def get_item(db_path: str, item_id: int) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM queue WHERE id = ?", (item_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_by_status(db_path: str, status: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM queue WHERE status = ?", (status,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


class TestQueueDBOperations:
    def test_get_processing_jobs(self, queue_db, sample_video):
        from src.queue_watcher import get_processing_jobs

        assert get_processing_jobs(queue_db) == []

        insert_processing_job(queue_db, sample_video)
        insert_processing_job(queue_db, sample_video)

        jobs = get_processing_jobs(queue_db)
        assert len(jobs) == 2
        assert all(j["status"] == "processing" for j in jobs)

    def test_update_queue_status(self, queue_db, sample_video):
        from src.queue_watcher import update_queue_status

        item_id = insert_processing_job(queue_db, sample_video)
        update_queue_status(queue_db, item_id, "failed", error="something broke")

        item = get_item(queue_db, item_id)
        assert item["status"] == "failed"
        assert item["error"] == "something broke"

    def test_add_clip_to_queue(self, queue_db, sample_video):
        from src.queue_watcher import add_clip_to_queue

        parent_id = insert_processing_job(queue_db, sample_video, ["tiktok", "youtube"])

        clip_id = add_clip_to_queue(
            queue_db, parent_id, "/clips/clip1.mp4", "Great caption!",
            {"duration": 30}, '["tiktok", "youtube"]',
        )

        clip = get_item(queue_db, clip_id)
        assert clip["status"] == "review"
        assert clip["parent_id"] == parent_id
        assert clip["caption"] == "Great caption!"
        assert clip["clip_path"] == "/clips/clip1.mp4"


class TestProcessJob:
    def test_fails_when_video_not_found(self, queue_db, clips_dir):
        from src.queue_watcher import process_job

        item_id = insert_processing_job(queue_db, "/nonexistent/video.mp4")
        item = get_item(queue_db, item_id)

        process_job(queue_db, item, clips_dir)

        result = get_item(queue_db, item_id)
        assert result["status"] == "failed"
        assert "not found" in result["error"]

    @patch("src.queue_watcher.JobRunner")
    def test_marks_failed_on_job_error(self, MockRunner, queue_db, sample_video, clips_dir):
        from src.queue_watcher import process_job
        from src.core.models import JobStatus, JobState

        # Simulate job failure
        mock_status = JobStatus()
        mock_status.mark_failed("FFmpeg crashed")
        MockRunner.return_value.run_job.return_value = mock_status

        item_id = insert_processing_job(queue_db, sample_video)
        item = get_item(queue_db, item_id)

        process_job(queue_db, item, clips_dir)

        result = get_item(queue_db, item_id)
        assert result["status"] == "failed"
        assert "FFmpeg" in result["error"]

    def test_successful_processing_creates_review_clips(
        self, queue_db, sample_video, clips_dir, tmp_path,
    ):
        from src.queue_watcher import process_job
        from src.core.models import JobStatus, JobState

        # Create fake exported clips
        clip1 = Path(clips_dir) / "clip_001.mp4"
        clip2 = Path(clips_dir) / "clip_002.mp4"
        clip1.write_bytes(b"clip1")
        clip2.write_bytes(b"clip2")

        # Create fake transcript
        transcript = tmp_path / "transcript.json"
        transcript.write_text(json.dumps({
            "segments": [
                {"start": 0, "end": 10, "text": "first clip caption text"},
                {"start": 15, "end": 25, "text": "second clip caption text"},
            ]
        }))

        # Mock successful job
        mock_status = JobStatus()
        mock_status.mark_started()
        mock_status.mark_finished_ok()

        mock_state = {
            "exported_clips": [str(clip1), str(clip2)],
            "clips": [
                {"start_time": 0, "end_time": 10, "duration": 10},
                {"start_time": 15, "end_time": 25, "duration": 10},
            ],
            "transcript_path": str(transcript),
        }

        with patch("src.queue_watcher.JobRunner") as MockRunner, \
             patch("src.queue_watcher.StateManager") as MockSM:

            MockRunner.return_value.run_job.return_value = mock_status
            mock_sm_instance = MockSM.return_value
            mock_sm_instance.get_video_state.return_value = mock_state
            mock_sm_instance.register_video = MagicMock()

            item_id = insert_processing_job(queue_db, sample_video, ["tiktok", "instagram"])
            item = get_item(queue_db, item_id)

            process_job(queue_db, item, clips_dir)

        # Source should be marked as clipped
        source = get_item(queue_db, item_id)
        assert source["status"] == "clipped"

        # Two clips should be in review
        review_items = get_by_status(queue_db, "review")
        assert len(review_items) == 2

        assert review_items[0]["parent_id"] == item_id
        assert "first clip" in review_items[0]["caption"]
        assert review_items[0]["clip_path"] == str(clip1)

        assert "second clip" in review_items[1]["caption"]


class TestCaptionGeneration:
    def test_fallback_to_transcript_excerpt(self, tmp_path):
        from src.queue_watcher import generate_captions_for_clips

        transcript = tmp_path / "transcript.json"
        transcript.write_text(json.dumps({
            "segments": [
                {"start": 0, "end": 5, "text": "Hello world"},
                {"start": 5, "end": 10, "text": "this is a test"},
                {"start": 15, "end": 20, "text": "second clip text"},
            ]
        }))

        clips = [
            {"start_time": 0, "end_time": 10},
            {"start_time": 15, "end_time": 20},
        ]

        # CopysGenerator won't be available in tests, so it falls back
        captions = generate_captions_for_clips(clips, str(transcript))
        assert len(captions) == 2
        assert "Hello world" in captions[0]
        assert "second clip" in captions[1]

    def test_fallback_when_no_transcript(self):
        from src.queue_watcher import generate_captions_for_clips

        clips = [{"start_time": 0, "end_time": 10}]
        captions = generate_captions_for_clips(clips, "/nonexistent/transcript.json")
        assert captions[0] == "Clip 1"


class TestRunOnce:
    def test_returns_zero_when_no_jobs(self, queue_db, clips_dir):
        from src.queue_watcher import run_once
        assert run_once(queue_db, clips_dir) == 0

    @patch("src.queue_watcher.process_job")
    def test_processes_all_pending(self, mock_process, queue_db, sample_video, clips_dir):
        from src.queue_watcher import run_once

        insert_processing_job(queue_db, sample_video)
        insert_processing_job(queue_db, sample_video)

        count = run_once(queue_db, clips_dir)
        assert count == 2
        assert mock_process.call_count == 2
