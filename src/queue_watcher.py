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
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras

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


# --- PostgreSQL queue operations (shared DB with shorts-analytics) ---

@contextmanager
def get_queue_db(db_url: str):
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def get_processing_jobs(db_url: str) -> list[dict]:
    with get_queue_db(db_url) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM queue WHERE status = 'processing' ORDER BY uploaded_at ASC"
            )
            return [dict(r) for r in cur.fetchall()]


def update_queue_status(db_url: str, item_id: int, status: str, **extra):
    with get_queue_db(db_url) as conn:
        with conn.cursor() as cur:
            sets = ["status = %s"]
            vals = [status]
            for k, v in extra.items():
                sets.append(f"{k} = %s")
                vals.append(v)
            vals.append(item_id)
            cur.execute(f"UPDATE queue SET {', '.join(sets)} WHERE id = %s", vals)


def add_clip_to_queue(
    db_url: str,
    parent_id: int,
    clip_path: str,
    caption: str,
    metadata: dict,
    platforms: str,
) -> int:
    with get_queue_db(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO queue (status, clip_path, caption, clip_metadata, parent_id, platforms) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
                ("review", clip_path, caption, json.dumps(metadata), parent_id, platforms),
            )
            return cur.fetchone()[0]


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

def _is_short_enhance(item: dict) -> tuple[bool, dict]:
    """Check if this item is a short needing enhancement. Returns (is_short, enhance_opts)."""
    meta_raw = item.get("clip_metadata") or "{}"
    try:
        meta = json.loads(meta_raw) if isinstance(meta_raw, str) else (meta_raw or {})
    except (json.JSONDecodeError, TypeError):
        meta = {}
    return bool(meta.get("enhance_short")), meta


def process_job(db_url: str, item: dict, clips_dir: str):
    """Process a single queue item through CliperAi's pipeline."""
    item_id = item["id"]
    source_video = item["source_video"]
    platforms = item.get("platforms", "[]")

    if not source_video or not os.path.exists(source_video):
        update_queue_status(db_url, item_id, "failed", error=f"Source video not found: {source_video}")
        return

    is_short, enhance_opts = _is_short_enhance(item)
    logger.info(f"Processing #{item_id}: {source_video}" + (" [short enhance]" if is_short else ""))

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

    # Create job spec — shorts use EXPORT_SHORTS, long-form uses full pipeline
    job_id = f"queue_{item_id}_{uuid.uuid4().hex[:8]}"

    if is_short:
        # Build shorts-specific settings from enhance options stored in clip_metadata
        shorts_settings: dict = {
            "add_logo": bool(enhance_opts.get("add_logo", False)),
            "skip_done": False,
            "add_subtitles": bool(enhance_opts.get("add_subtitles", False)),
        }
        if enhance_opts.get("logo_position"):
            shorts_settings["logo_position"] = enhance_opts["logo_position"]
        if enhance_opts.get("logo_scale"):
            shorts_settings["logo_scale"] = float(enhance_opts["logo_scale"])
        if enhance_opts.get("subtitle_preset"):
            shorts_settings["subtitle_style"] = enhance_opts["subtitle_preset"]
        if enhance_opts.get("trim_silence"):
            shorts_settings["trim_ms_start"] = int(enhance_opts.get("trim_ms_start", 1000))
            shorts_settings["trim_ms_end"] = int(enhance_opts.get("trim_ms_end", 1000))
        if enhance_opts.get("video_crf"):
            shorts_settings["video_crf"] = int(enhance_opts["video_crf"])

        job = JobSpec(
            job_id=job_id,
            video_ids=[video_id],
            steps=[JobStep.TRANSCRIBE, JobStep.EXPORT_SHORTS],
            settings={
                "transcribe": {"device": "auto", "model": "base"},
                "shorts": shorts_settings,
            },
        )
    else:
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
        update_queue_status(db_url, item_id, "failed", error=str(e))
        logger.error(f"Job failed for #{item_id}: {e}")
        return

    if status.state == JobState.FAILED:
        update_queue_status(db_url, item_id, "failed", error=status.error or "Unknown error")
        logger.error(f"Job failed for #{item_id}: {status.error}")
        return

    # Collect results
    video_state = state_manager.get_video_state(video_id) or {}

    if is_short:
        # Short enhance: single enhanced video replaces the original
        enhanced_path = video_state.get("shorts_export_path")
        if not enhanced_path:
            # Fallback: look for the export in clips_dir
            exports_dir = Path(clips_dir)
            candidates = sorted(str(p) for p in exports_dir.glob(f"*{video_id}*_short.mp4"))
            enhanced_path = candidates[0] if candidates else None

        if not enhanced_path:
            update_queue_status(db_url, item_id, "failed", error="Short enhancement produced no output")
            return

        # Update the original item directly: set clip_path to enhanced video, move to review
        update_queue_status(db_url, item_id, "review", clip_path=enhanced_path)
        logger.info(f"Finished #{item_id}: short enhanced → review ({Path(enhanced_path).name})")
    else:
        # Long-form: multiple clips
        exported_paths = video_state.get("exported_clips", [])
        clips_data = video_state.get("clips", [])
        transcript_path = video_state.get("transcript_path") or video_state.get("transcription_path", "")

        if not exported_paths:
            # Try to find clips in the exports dir
            exports_dir = Path(clips_dir)
            exported_paths = sorted(str(p) for p in exports_dir.glob(f"*{video_id}*.mp4"))

        if not exported_paths:
            update_queue_status(db_url, item_id, "failed", error="No clips exported")
            return

        # Generate captions (GENERATE_CAPTIONS step)
        captions = {}
        logger.info(f"[{JobStep.GENERATE_CAPTIONS.value}] Generating captions for {len(exported_paths)} clip(s)")
        if transcript_path and os.path.exists(transcript_path):
            try:
                captions = generate_captions_for_clips(clips_data, transcript_path)
            except Exception as e:
                logger.error(f"Caption generation failed for #{item_id}: {e} — using fallback labels")
        else:
            logger.warning(f"No transcript found for #{item_id}, clips will have auto-labelled captions")

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

            clip_id = add_clip_to_queue(db_url, item_id, clip_path, caption, metadata, platforms)
            logger.info(f"  Added clip #{clip_id} to review queue: {Path(clip_path).name}")

        # Mark source as done (change from 'processing' to something that indicates completion)
        update_queue_status(db_url, item_id, "clipped")
        logger.info(f"Finished #{item_id}: {len(exported_paths)} clips added to review queue")


def run_once(db_url: str, clips_dir: str) -> int:
    """Process all pending jobs. Returns number processed."""
    jobs = get_processing_jobs(db_url)
    if not jobs:
        return 0

    logger.info(f"Found {len(jobs)} processing job(s)")
    for job in jobs:
        try:
            process_job(db_url, job, clips_dir)
        except Exception as exc:
            item_id = job.get("id")
            logger.error(f"Unhandled error processing job #{item_id}: {exc}")
            if item_id:
                try:
                    update_queue_status(db_url, item_id, "failed", error=f"Unhandled error: {exc}")
                except Exception:
                    pass

    return len(jobs)


def main():
    parser = argparse.ArgumentParser(description="CliperAi queue watcher")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""),
                        help="PostgreSQL connection URL (or set DATABASE_URL env var)")
    parser.add_argument("--clips-dir", default=DEFAULT_CLIPS_DIR, help="Output directory for clips")
    parser.add_argument("--watch", type=int, metavar="SECONDS", help="Poll interval in seconds")
    args = parser.parse_args()

    db_url = args.database_url
    if not db_url:
        logger.error("No database URL. Set --database-url or DATABASE_URL env var.")
        sys.exit(1)

    clips_dir = os.path.abspath(args.clips_dir)
    os.makedirs(clips_dir, exist_ok=True)

    logger.info(f"Database: {db_url.split('@')[-1] if '@' in db_url else '(configured)'}")
    logger.info(f"Clips dir: {clips_dir}")

    if args.watch:
        logger.info(f"Watching every {args.watch}s (idle timeout: {IDLE_TIMEOUT_SECONDS}s)")
        last_activity = time.time()
        while True:
            processed = run_once(db_url, clips_dir)
            if processed > 0:
                last_activity = time.time()
            elif time.time() - last_activity > IDLE_TIMEOUT_SECONDS:
                logger.info(f"Idle for {IDLE_TIMEOUT_SECONDS}s, shutting down")
                break
            time.sleep(args.watch)
    else:
        run_once(db_url, clips_dir)


if __name__ == "__main__":
    main()
