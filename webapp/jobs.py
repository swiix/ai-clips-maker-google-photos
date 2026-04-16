"""
Background job queue for clip pipeline runs.
"""

from __future__ import annotations

import logging
import json
import os
import queue
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import sqlite3

from ai_clips_maker.media.audiovideo_file import AudioVideoFile
from ai_clips_maker.pipeline.crop_select import safe_dir_slug

from webapp import db as dbmod

if TYPE_CHECKING:
    from webapp.settings import Settings

_task_queue: queue.Queue[int | None] = queue.Queue()
_worker_started = False
_worker_lock = threading.Lock()
logger = logging.getLogger(__name__)
_DURATION_TAG_RE = re.compile(r"_(\d+(?:d\d+)?)s_to_(\d+(?:d\d+)?)s_")


def enqueue_job_id(job_id: int) -> None:
    _task_queue.put(job_id)


def start_worker(conn_factory, settings: "Settings") -> None:
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        _worker_started = True

    def loop() -> None:
        while True:
            job_id = _task_queue.get()
            if job_id is None:
                break
            conn = conn_factory()
            try:
                _run_one_job(conn, settings, job_id)
            except Exception as exc:
                # Logging can fail with some third-party lazy imports; never block DB status update.
                try:
                    logger.error("Job %s crashed: %s", job_id, exc)
                except Exception:
                    pass
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'failed',
                        phase = 'failed',
                        phase_message = COALESCE(phase_message, ?),
                        progress = COALESCE(progress, 1.0),
                        error = COALESCE(error, ?),
                        updated_at = ?
                    WHERE id = ? AND status = 'running'
                    """,
                    (
                        "Worker crashed before error reporting.",
                        "Worker crashed before error reporting.",
                        time.time(),
                        job_id,
                    ),
                )
                conn.commit()
            finally:
                conn.close()
            _task_queue.task_done()

    threading.Thread(target=loop, name="clip-worker", daemon=True).start()


def stop_worker() -> None:
    _task_queue.put(None)


def _run_one_job(conn: sqlite3.Connection, settings: Settings, job_id: int) -> None:
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return
    media_item_id = row["media_item_id"]
    job_type = str(row["job_type"] or "clip_pipeline")
    dbmod.upsert_job(
        conn,
        media_item_id,
        job_type=job_type,
        status="running",
        phase="prepare",
        phase_message="Job wird vorbereitet",
        progress=0.02,
    )

    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    settings.output_dir.mkdir(parents=True, exist_ok=True)

    ext = Path(row["filename"] or "video.mp4").suffix or ".mp4"
    cache_name = f"{media_item_id}{ext}"
    cache_path = settings.cache_dir / cache_name
    if cache_path.is_file() and not _is_valid_cached_av(cache_path):
        try:
            cache_path.unlink()
        except Exception:
            pass

    if not cache_path.is_file():
        dbmod.upsert_job(
            conn,
            media_item_id,
            job_type=job_type,
            status="running",
            phase="download",
            phase_message="Video wird geladen",
            progress=0.10,
        )
        if not row["base_url"]:
            dbmod.upsert_job(
                conn,
                media_item_id,
                job_type=job_type,
                status="failed",
                phase="failed",
                phase_message="Download-URL fehlt",
                progress=1.0,
                error="Missing base_url for download",
            )
            return
        from webapp.google_photos import download_media_base_url, ensure_fresh_credentials

        try:
            creds = ensure_fresh_credentials(settings)
            if creds is None or not creds.token:
                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="failed",
                    phase="failed",
                    phase_message="Google Auth fehlt",
                    progress=1.0,
                    error="Google auth missing. Please reconnect and retry.",
                )
                return
            last_emit_time = 0.0
            last_percent = -1

            def on_download_progress(downloaded: int, total: int | None) -> None:
                nonlocal last_emit_time, last_percent
                now = time.time()
                percent = None
                if total and total > 0:
                    percent = int(max(0, min(100, round(downloaded * 100 / total))))

                should_emit = False
                if percent is not None and percent != last_percent:
                    should_emit = True
                elif now - last_emit_time >= 1.0:
                    should_emit = True
                if not should_emit:
                    return

                if percent is not None:
                    job_progress = 0.10 + (0.12 * (percent / 100.0))
                    msg = f"Video wird geladen ({percent}%)"
                    last_percent = percent
                else:
                    mb = downloaded / (1024 * 1024)
                    job_progress = 0.11
                    msg = f"Video wird geladen ({mb:.1f} MB)"

                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="running",
                    phase="download",
                    phase_message=msg,
                    progress=job_progress,
                )
                last_emit_time = now

            download_media_base_url(
                row["base_url"],
                cache_path,
                access_token=creds.token,
                progress_callback=on_download_progress,
            )
            if not _is_valid_cached_av(cache_path):
                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="failed",
                    phase="failed",
                    phase_message="Ungueltige Mediendatei",
                    progress=1.0,
                    error=(
                        "Downloaded file is not a valid audio+video source. "
                        "Please re-open Picker and select the video again."
                    ),
                )
                return
        except Exception as exc:
            dbmod.upsert_job(
                conn,
                media_item_id,
                job_type=job_type,
                status="failed",
                phase="failed",
                phase_message="Download fehlgeschlagen",
                progress=1.0,
                error=f"download: {exc}",
            )
            return
    if job_type == "clip_pipeline":
        run_phase = "pipeline"
        run_phase_msg = "Pipeline wird gestartet"
    elif job_type == "openai_speech_trim":
        run_phase = "transcribe"
        run_phase_msg = "OpenAI Transkription"
    elif job_type == "silence_remove":
        run_phase = "detect_silence"
        run_phase_msg = "Stille wird erkannt"
    else:
        run_phase = "pipeline"
        run_phase_msg = "Pipeline wird gestartet"

    dbmod.upsert_job(
        conn,
        media_item_id,
        job_type=job_type,
        status="running",
        phase=run_phase,
        phase_message=run_phase_msg,
        progress=0.22,
    )
    processing_input = cache_path
    if _is_noise_reduction_enabled(row["job_options"]):
        dbmod.upsert_job(
            conn,
            media_item_id,
            job_type=job_type,
            status="running",
            phase="noise_reduce",
            phase_message="Noise Reduction wird angewendet",
            progress=0.28,
        )
        nr_path = settings.cache_dir / f"{media_item_id}_nr{ext}"
        try:
            _apply_noise_reduction_video(cache_path, nr_path)
            if _is_valid_cached_av(nr_path):
                processing_input = nr_path
            else:
                raise RuntimeError("Noise-reduced file is invalid.")
        except Exception as exc:
            dbmod.upsert_job(
                conn,
                media_item_id,
                job_type=job_type,
                status="failed",
                phase="failed",
                phase_message="Noise Reduction fehlgeschlagen",
                progress=1.0,
                error=str(exc),
            )
            return

    slug = safe_dir_slug(Path(row["filename"] or "video").stem)
    short_id = media_item_id[:12] if len(media_item_id) > 12 else media_item_id
    run_prefix = f"{slug}_{short_id}_{uuid.uuid4().hex[:6]}"
    output_dir = settings.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    meta = {
        "mediaItemId": media_item_id,
        "filename": row["filename"],
        "baseUrl": row["base_url"],
        "productUrl": row["product_url"],
        "creationTime": row["creation_time"],
    }

    try:
        if job_type == "openai_speech_trim":
            from webapp.openai_speech_trim import trim_video_to_openai_speech

            api_key = settings.openai_api_key or os.environ.get("OPENAI_API_KEY", "")
            if not api_key:
                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="failed",
                    phase="failed",
                    phase_message="OpenAI API Key fehlt",
                    progress=1.0,
                    error="Missing OPENAI_API_KEY (set in .env or environment).",
                )
                return

            dbmod.upsert_job(
                conn,
                media_item_id,
                job_type=job_type,
                status="running",
                phase="transcribe",
                phase_message="Audio wird extrahiert und an OpenAI gesendet",
                progress=0.40,
            )
            model = settings.openai_transcription_model or "whisper-1"
            usd_per_min = float(settings.openai_whisper_usd_per_minute)
            merge_gap_sec = 0.35
            min_segment_sec = 0.04
            try:
                options = json.loads(row["job_options"] or "{}")
                if options.get("cut_merge_gap_sec") is not None:
                    merge_gap_sec = max(0.01, float(options["cut_merge_gap_sec"]))
                elif options.get("openai_merge_gap_sec") is not None:
                    merge_gap_sec = max(0.01, float(options["openai_merge_gap_sec"]))
                if options.get("cut_min_duration_sec") is not None:
                    min_segment_sec = max(0.01, float(options["cut_min_duration_sec"]))
                elif options.get("openai_min_segment_sec") is not None:
                    min_segment_sec = max(0.01, float(options["openai_min_segment_sec"]))
            except (json.JSONDecodeError, TypeError, ValueError):
                merge_gap_sec = 0.35
                min_segment_sec = 0.04
            result = trim_video_to_openai_speech(
                str(processing_input),
                str(output_dir),
                run_prefix,
                api_key,
                model=model,
                usd_per_minute=usd_per_min,
                merge_gap_sec=merge_gap_sec,
                min_segment_sec=min_segment_sec,
            )
            out_name = Path(result["video_path"]).name
            dbmod.upsert_job(
                conn,
                media_item_id,
                job_type=job_type,
                status="done",
                phase="done",
                phase_message=f"Fertig ({out_name})",
                progress=1.0,
                output_dir=str(output_dir),
                error=None,
            )
            try:
                o_secs = float(str(result.get("input_audio_seconds") or "0") or 0.0)
            except (TypeError, ValueError):
                o_secs = 0.0
            try:
                out_secs = float(str(result.get("output_video_seconds") or "0") or 0.0)
            except (TypeError, ValueError):
                out_secs = 0.0
            try:
                o_cost = float(str(result.get("estimated_cost_usd") or "0") or 0.0)
            except (TypeError, ValueError):
                o_cost = 0.0
            dbmod.set_job_run_metrics(
                conn,
                media_item_id,
                outputs_created=1,
                openai_input_seconds=o_secs if o_secs > 0 else None,
                openai_cost_usd=o_cost if o_cost > 0 else None,
                cut_input_seconds=o_secs if o_secs > 0 else None,
                cut_output_seconds=out_secs if out_secs > 0 else None,
            )
        elif job_type == "silence_remove":
            from webapp.silence_remover import remove_silence_selected_profiles

            dbmod.upsert_job(
                conn,
                media_item_id,
                job_type=job_type,
                status="running",
                phase="cut_merge",
                phase_message="Stille wird entfernt",
                progress=0.55,
            )
            selected_profiles: list[str] = []
            cut_merge_gap_sec: float | None = None
            cut_min_duration_sec: float | None = None
            try:
                options = json.loads(row["job_options"] or "{}")
                if options.get("cut_merge_gap_sec") is not None:
                    cut_merge_gap_sec = max(0.01, float(options["cut_merge_gap_sec"]))
                if options.get("cut_min_duration_sec") is not None:
                    cut_min_duration_sec = max(0.01, float(options["cut_min_duration_sec"]))
                trim_method = str(options.get("trim_method") or "").strip()
                profiles = options.get("profiles")
                if trim_method == "silence_all":
                    selected_profiles = ["conservative", "balanced", "aggressive"]
                elif trim_method in (
                    "silence_conservative",
                    "silence_balanced",
                    "silence_aggressive",
                ):
                    selected_profiles = [trim_method.replace("silence_", "")]
                elif isinstance(profiles, list):
                    selected_profiles = [
                        str(p)
                        for p in profiles
                        if str(p) in {"conservative", "balanced", "aggressive"}
                    ]
            except Exception:
                selected_profiles = []
            if not selected_profiles:
                selected_profiles = ["balanced"]
            rendered = remove_silence_selected_profiles(
                str(processing_input),
                str(output_dir),
                run_prefix,
                selected_profiles,
                override_merge_gap_sec=cut_merge_gap_sec,
                override_min_keep_sec=cut_min_duration_sec,
            )
            best_before: float | None = None
            best_after: float | None = None
            for item in rendered:
                parsed = _parse_duration_from_name(item.get("output_path") if isinstance(item, dict) else getattr(item, "output_path", ""))
                if parsed is None:
                    continue
                before, after = parsed
                if best_after is None or after < best_after:
                    best_before = before
                    best_after = after
            dbmod.upsert_job(
                conn,
                media_item_id,
                job_type=job_type,
                status="done",
                phase="done",
                phase_message=f"Fertig ({len(rendered)} Profile erstellt)",
                progress=1.0,
                output_dir=str(output_dir),
                error=None,
            )
            dbmod.set_job_run_metrics(
                conn,
                media_item_id,
                outputs_created=len(rendered),
                openai_input_seconds=None,
                openai_cost_usd=None,
                cut_input_seconds=best_before,
                cut_output_seconds=best_after,
            )
        else:
            token = settings.pyannote_token or __import__("os").environ.get("HF_TOKEN", "")
            if not token:
                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="failed",
                    phase="failed",
                    phase_message="Token fehlt",
                    progress=1.0,
                    error="Missing PYANNOTE_TOKEN (or HF_TOKEN) for diarization",
                )
                return
            from ai_clips_maker.pipeline.export_clips import run_clips_pipeline

            def on_progress(phase: str, message: str, progress: float | None) -> None:
                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="running" if phase not in ("failed", "done") else phase,
                    phase=phase,
                    phase_message=message,
                    progress=progress,
                )

            result = run_clips_pipeline(
                str(processing_input),
                str(output_dir),
                token,
                source_metadata=meta,
                output_prefix=run_prefix,
                status_callback=on_progress,
            )
            if result.error:
                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="failed",
                    phase="failed",
                    phase_message="Pipeline fehlgeschlagen",
                    progress=1.0,
                    error=result.error,
                    output_dir=str(output_dir),
                )
            else:
                n_clips = len(result.clips) if getattr(result, "clips", None) else 0
                dbmod.upsert_job(
                    conn,
                    media_item_id,
                    job_type=job_type,
                    status="done",
                    phase="done",
                    phase_message="Verarbeitung abgeschlossen",
                    progress=1.0,
                    output_dir=str(output_dir),
                    error=None,
                )
                dbmod.set_job_run_metrics(
                    conn,
                    media_item_id,
                    outputs_created=n_clips,
                    openai_input_seconds=None,
                    openai_cost_usd=None,
                    cut_input_seconds=None,
                    cut_output_seconds=None,
                )
    except Exception as exc:
        try:
            logger.error("Pipeline error for %s: %s", media_item_id, exc)
        except Exception:
            pass
        dbmod.upsert_job(
            conn,
            media_item_id,
            job_type=job_type,
            status="failed",
            phase="failed",
            phase_message="Unerwarteter Fehler",
            progress=1.0,
            error=str(exc),
            output_dir=str(output_dir),
        )


def queue_pending_on_startup(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id FROM jobs WHERE status = 'queued' ORDER BY id"
    ).fetchall()
    for r in rows:
        _task_queue.put(r["id"])


def mark_stale_running_jobs_failed(conn: sqlite3.Connection) -> int:
    """
    Convert stale running jobs to failed after unclean shutdowns or worker crashes.
    """
    rows = conn.execute(
        "SELECT media_item_id FROM jobs WHERE status = 'running'"
    ).fetchall()
    for r in rows:
        dbmod.upsert_job(
            conn,
            r["media_item_id"],
            status="failed",
            phase="failed",
            phase_message="Worker unterbrochen",
            progress=1.0,
            error="Worker interrupted or crashed. Please retry job.",
        )
    return len(rows)


def _is_valid_cached_av(path: Path) -> bool:
    """
    Ensure cached source is truly audio+video, not thumbnail/image payload.
    """
    try:
        av = AudioVideoFile(str(path))
        av.assert_exists()
        return True
    except Exception:
        return False


def _parse_duration_from_name(path: str | Path) -> tuple[float, float] | None:
    if path is None:
        return None
    name = Path(path).name
    m = _DURATION_TAG_RE.search(name)
    if not m:
        return None
    try:
        before = float(m.group(1).replace("d", "."))
        after = float(m.group(2).replace("d", "."))
    except ValueError:
        return None
    if before <= 0 or after < 0:
        return None
    return before, after


def _is_noise_reduction_enabled(options_raw: str | None) -> bool:
    try:
        options = json.loads(options_raw or "{}")
    except (json.JSONDecodeError, TypeError, ValueError):
        return False
    value = options.get("noise_reduction", False)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _apply_noise_reduction_video(input_video: Path, output_video: Path) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(input_video),
        "-af",
        "highpass=f=70,lowpass=f=8000,afftdn=nf=-25",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        str(output_video),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip() or str(proc.returncode)
        raise RuntimeError(f"ffmpeg noise reduction failed: {detail}")
