"""
Queue watcher: bridges the shorts-analytics SQLite queue with CliperAi's JobRunner.

Polls for status='processing' jobs, runs the AI pipeline, writes clips back as
status='review' with captions.

Usage:
    uv run python -m src.queue_watcher --queue-db /home/dev/shorts-analytics/queue.db
    uv run python -m src.queue_watcher --queue-db /home/dev/shorts-analytics/queue.db --watch 30
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from src.core.events import JobStatusEvent, LogEvent, LogLevel, ProgressEvent
from src.core.job_runner import JobRunner
from src.core.models import JobSpec, JobState, JobStep
from src.utils.state_manager import StateManager
from src.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_CLIPS_DIR = os.getenv(
    "CLIPER_OUTPUT_DIR",
    os.path.join(os.path.dirname(__file__), "..", "output", "exports"),
)

IDLE_TIMEOUT_SECONDS = int(os.getenv("CLIPER_IDLE_TIMEOUT", "600"))  # 10 min


# --- SQLite queue operations (reads from shorts-analytics DB) ---

@contextmanager
def get_queue_db(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def get_processing_jobs(db_path: str) -> list[dict]:
    with get_queue_db(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM queue WHERE status = 'processing' ORDER BY uploaded_at ASC"
        ).fetchall()
        return [dict(r) for r in rows]


def update_queue_status(db_path: str, item_id: int, status: str, **extra):
    with get_queue_db(db_path) as conn:
        sets = ["status = ?"]
        vals = [status]
        for k, v in extra.items():
            sets.append(f"{k} = ?")
            vals.append(v)
        vals.append(item_id)
        conn.execute(f"UPDATE queue SET {', '.join(sets)} WHERE id = ?", vals)


def add_clip_to_queue(
    db_path: str,
    parent_id: int,
    clip_path: str,
    caption: str,
    metadata: dict,
    platforms: str,
) -> int:
    with get_queue_db(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO queue (status, clip_path, caption, clip_metadata, parent_id, platforms) VALUES (?, ?, ?, ?, ?, ?)",
            ("review", clip_path, caption, json.dumps(metadata), parent_id, platforms),
        )
        return cur.lastrowid


# --- Event handler ---

class QueueEventHandler:
    """Collects events from JobRunner for logging."""

    def __init__(self):
        self.events: list = []

    def emit(self, event):
        self.events.append(event)
        if isinstance(event, LogEvent):
            level = event.level.value if hasattr(event.level, "value") else str(event.level)
            logger.info(f"[{level}] {event.message}")
        elif isinstance(event, ProgressEvent):
            logger.info(f"[progress] {event.label} ({event.current}/{event.total})")
        elif isinstance(event, JobStatusEvent):
            state = event.state.value if hasattr(event.state, "value") else str(event.state)
            logger.info(f"[status] {state}" + (f" error={event.error}" if event.error else ""))


# --- Caption generation ---

def generate_captions_for_clips(clips: list[dict], transcript_path: str) -> dict[int, str]:
    """
    Generate captions for clips using CliperAi's copys_generator.
    Returns {clip_index: caption_text}.
    Falls back to transcript excerpt if Gemini is not available.
    """
    captions = {}

    try:
        from src.copys_generator import CopysGenerator
        generator = CopysGenerator()

        for i, clip in enumerate(clips):
            start = clip.get("start_time", 0)
            end = clip.get("end_time", 0)
            try:
                result = generator.generate_copy(
                    transcript_path=transcript_path,
                    start_time=start,
                    end_time=end,
                )
                if result and hasattr(result, "caption"):
                    captions[i] = result.caption
                elif isinstance(result, str):
                    captions[i] = result
                elif isinstance(result, dict):
                    captions[i] = result.get("caption", result.get("text", ""))
            except Exception as e:
                logger.warning(f"Caption generation failed for clip {i}: {e}")
    except ImportError:
        logger.warning("CopysGenerator not available, using transcript excerpts")
    except Exception as e:
        logger.warning(f"Caption generation setup failed: {e}")

    # Fallback: extract text from transcript for clips without captions
    if len(captions) < len(clips):
        try:
            with open(transcript_path, "r") as f:
                transcript_data = json.load(f)
            segments = transcript_data.get("segments", [])
            for i, clip in enumerate(clips):
                if i not in captions:
                    start = clip.get("start_time", 0)
                    end = clip.get("end_time", 0)
                    text_parts = []
                    for seg in segments:
                        seg_start = seg.get("start", 0)
                        seg_end = seg.get("end", 0)
                        if seg_start >= start and seg_end <= end:
                            text_parts.append(seg.get("text", ""))
                    excerpt = " ".join(text_parts).strip()
                    captions[i] = excerpt[:200] if excerpt else f"Clip {i + 1}"
        except Exception:
            for i in range(len(clips)):
                if i not in captions:
                    captions[i] = f"Clip {i + 1}"

    return captions


# --- Main process ---

def process_job(db_path: str, item: dict, clips_dir: str):
    """Process a single queue item through CliperAi's pipeline."""
    item_id = item["id"]
    source_video = item["source_video"]
    platforms = item.get("platforms", "[]")

    if not source_video or not os.path.exists(source_video):
        update_queue_status(db_path, item_id, "failed", error=f"Source video not found: {source_video}")
        return

    logger.info(f"Processing #{item_id}: {source_video}")

    # Setup CliperAi state manager in a temp directory for this job
    work_dir = Path(clips_dir) / f".work_{item_id}"
    work_dir.mkdir(parents=True, exist_ok=True)

    state_manager = StateManager(
        state_file=str(work_dir / "state.json"),
        settings_file=str(work_dir / "settings.json"),
    )

    # Register the video
    video_id = Path(source_video).stem
    state_manager.register_video(
        video_id=video_id,
        filename=Path(source_video).name,
        video_path=str(Path(source_video).resolve()),
    )

    # Create job spec
    job_id = f"queue_{item_id}_{uuid.uuid4().hex[:8]}"
    job = JobSpec(
        job_id=job_id,
        video_ids=[video_id],
        steps=[JobStep.TRANSCRIBE, JobStep.GENERATE_CLIPS, JobStep.EXPORT_CLIPS],
        settings={
            "transcribe": {"device": "auto", "model": "base"},
            "clips": {},
            "export": {
                "aspect_ratio": "9:16",
                "add_subtitles": True,
            },
        },
    )

    # Run pipeline
    handler = QueueEventHandler()
    runner = JobRunner(state_manager, handler.emit, cli_output_dir=clips_dir)

    try:
        status = runner.run_job(job)
    except Exception as e:
        update_queue_status(db_path, item_id, "failed", error=str(e))
        logger.error(f"Job failed for #{item_id}: {e}")
        return

    if status.state == JobState.FAILED:
        update_queue_status(db_path, item_id, "failed", error=status.error or "Unknown error")
        logger.error(f"Job failed for #{item_id}: {status.error}")
        return

    # Collect exported clips
    video_state = state_manager.get_video_state(video_id) or {}
    exported_paths = video_state.get("exported_clips", [])
    clips_data = video_state.get("clips", [])
    transcript_path = video_state.get("transcript_path") or video_state.get("transcription_path", "")

    if not exported_paths:
        # Try to find clips in the exports dir
        exports_dir = Path(clips_dir)
        exported_paths = sorted(str(p) for p in exports_dir.glob(f"*{video_id}*.mp4"))

    if not exported_paths:
        update_queue_status(db_path, item_id, "failed", error="No clips exported")
        return

    # Generate captions
    captions = {}
    if transcript_path and os.path.exists(transcript_path):
        captions = generate_captions_for_clips(clips_data, transcript_path)

    # Insert each clip into the queue as 'review'
    for i, clip_path in enumerate(exported_paths):
        caption = captions.get(i, f"Clip {i + 1}")
        clip_meta = clips_data[i] if i < len(clips_data) else {}
        metadata = {
            "duration": clip_meta.get("duration", 0),
            "start_time": clip_meta.get("start_time", 0),
            "end_time": clip_meta.get("end_time", 0),
            "clip_index": i,
            "total_clips": len(exported_paths),
        }

        clip_id = add_clip_to_queue(db_path, item_id, clip_path, caption, metadata, platforms)
        logger.info(f"  Added clip #{clip_id} to review queue: {Path(clip_path).name}")

    # Mark source as done (change from 'processing' to something that indicates completion)
    update_queue_status(db_path, item_id, "clipped")
    logger.info(f"Finished #{item_id}: {len(exported_paths)} clips added to review queue")


def run_once(db_path: str, clips_dir: str) -> int:
    """Process all pending jobs. Returns number processed."""
    jobs = get_processing_jobs(db_path)
    if not jobs:
        return 0

    logger.info(f"Found {len(jobs)} processing job(s)")
    for job in jobs:
        process_job(db_path, job, clips_dir)

    return len(jobs)


def main():
    parser = argparse.ArgumentParser(description="CliperAi queue watcher")
    parser.add_argument("--queue-db", required=True, help="Path to shorts-analytics queue.db")
    parser.add_argument("--clips-dir", default=DEFAULT_CLIPS_DIR, help="Output directory for clips")
    parser.add_argument("--watch", type=int, metavar="SECONDS", help="Poll interval in seconds")
    args = parser.parse_args()

    db_path = os.path.abspath(args.queue_db)
    clips_dir = os.path.abspath(args.clips_dir)
    os.makedirs(clips_dir, exist_ok=True)

    if not os.path.exists(db_path):
        logger.error(f"Queue DB not found: {db_path}")
        sys.exit(1)

    logger.info(f"Queue DB: {db_path}")
    logger.info(f"Clips dir: {clips_dir}")

    if args.watch:
        logger.info(f"Watching every {args.watch}s (idle timeout: {IDLE_TIMEOUT_SECONDS}s)")
        last_activity = time.time()
        while True:
            processed = run_once(db_path, clips_dir)
            if processed > 0:
                last_activity = time.time()
            elif time.time() - last_activity > IDLE_TIMEOUT_SECONDS:
                logger.info(f"Idle for {IDLE_TIMEOUT_SECONDS}s, shutting down")
                break
            time.sleep(args.watch)
    else:
        run_once(db_path, clips_dir)


if __name__ == "__main__":
    main()
