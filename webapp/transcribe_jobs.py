from __future__ import annotations

import logging
import queue
import re
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from webapp import db as dbmod

if TYPE_CHECKING:
    import sqlite3
    from webapp.settings import Settings

logger = logging.getLogger(__name__)

_job_queue: queue.Queue[int | None] = queue.Queue()
_started = False
_lock = threading.Lock()

_OPENAI_TRANSCRIPTIONS_URL = "https://api.openai.com/v1/audio/transcriptions"
_CHUNK_SECONDS = 20 * 60
_SILENCE_RE = re.compile(r"silence_(start|end):\s*([0-9]+(?:\.[0-9]+)?)")


def start_worker(conn_factory, settings: "Settings") -> None:
    global _started
    with _lock:
        if _started:
            return
        _started = True

    def loop() -> None:
        while True:
            job_id = _job_queue.get()
            if job_id is None:
                break
            conn = conn_factory()
            try:
                _run_job(conn, settings, job_id)
            except Exception as exc:
                logger.exception("Transcription job %s crashed: %s", job_id, exc)
                dbmod.update_transcription_job(
                    conn,
                    job_id,
                    status="failed",
                    phase="failed",
                    progress=1.0,
                    error=f"Unexpected crash: {exc}",
                )
            finally:
                conn.close()
                _job_queue.task_done()

    threading.Thread(target=loop, name="transcription-worker", daemon=True).start()


def stop_worker() -> None:
    _job_queue.put(None)


def enqueue(job_id: int) -> None:
    _job_queue.put(job_id)


def _probe_duration_seconds(path: Path) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(path),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        return 0.0
    try:
        return max(0.0, float((proc.stdout or "").strip()))
    except (TypeError, ValueError):
        return 0.0


def _extract_chunk_mp3(input_path: Path, chunk_path: Path, start_sec: float, dur_sec: float) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-ss",
        f"{start_sec:.3f}",
        "-t",
        f"{dur_sec:.3f}",
        "-i",
        str(input_path),
        "-vn",
        "-ac",
        "1",
        "-ar",
        "16000",
        "-b:a",
        "32k",
        str(chunk_path),
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "ffmpeg chunk extraction failed").strip())


def _detect_silence_markers(input_path: Path) -> list[tuple[str, float]]:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "info",
        "-i",
        str(input_path),
        "-af",
        "silencedetect=noise=-35dB:d=0.35",
        "-f",
        "null",
        "-",
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    payload = f"{proc.stderr or ''}\n{proc.stdout or ''}"
    markers: list[tuple[str, float]] = []
    for m in _SILENCE_RE.finditer(payload):
        kind = m.group(1)
        try:
            ts = float(m.group(2))
        except (TypeError, ValueError):
            continue
        markers.append((kind, ts))
    markers.sort(key=lambda x: x[1])
    return markers


def _build_silence_aware_chunks(total: float, markers: list[tuple[str, float]]) -> list[tuple[float, float]]:
    if total <= 0:
        return [(0.0, float(_CHUNK_SECONDS))]
    silence_starts = [ts for kind, ts in markers if kind == "start" and 0 < ts < total]
    chunks: list[tuple[float, float]] = []
    cursor = 0.0
    min_chunk = 8 * 60.0
    max_chunk = 25 * 60.0
    prefer = float(_CHUNK_SECONDS)
    while cursor < total:
        remaining = total - cursor
        if remaining <= max_chunk:
            chunks.append((cursor, remaining))
            break
        min_end = cursor + min_chunk
        prefer_end = cursor + prefer
        max_end = cursor + max_chunk
        candidates = [s for s in silence_starts if min_end <= s <= max_end]
        if candidates:
            # Pick nearest silence boundary to preferred target length.
            cut = min(candidates, key=lambda s: abs(s - prefer_end))
        else:
            # Hard fallback only when no silence was found in a safe window.
            cut = min(max_end, total)
        dur = max(1.0, cut - cursor)
        chunks.append((cursor, dur))
        cursor = cut
    return chunks


def _transcribe_chunk_text(
    *,
    api_key: str,
    model: str,
    language: str | None,
    audio_path: Path,
    timeout_sec: float = 1800.0,
) -> str:
    data = {
        "model": model,
        "response_format": "text",
    }
    lang = (language or "").strip()
    if lang:
        data["language"] = lang
    with httpx.Client(timeout=timeout_sec) as client:
        with audio_path.open("rb") as fh:
            resp = client.post(
                _OPENAI_TRANSCRIPTIONS_URL,
                headers={"Authorization": f"Bearer {api_key}"},
                data=data,
                files={"file": (audio_path.name, fh, "audio/mpeg")},
            )
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = (resp.text or "").strip() or f"HTTP {resp.status_code}"
        raise RuntimeError(f"OpenAI transcription failed: {detail}") from exc
    return (resp.text or "").strip()


def queue_pending_on_startup(conn: "sqlite3.Connection") -> None:
    rows = conn.execute(
        "SELECT id FROM transcription_jobs WHERE status = 'queued' ORDER BY id"
    ).fetchall()
    for row in rows:
        enqueue(int(row["id"]))


def _run_job(conn: "sqlite3.Connection", settings: "Settings", job_id: int) -> None:
    row = dbmod.get_transcription_job(conn, job_id)
    if not row:
        return
    input_path = Path(str(row["input_path"])).expanduser().resolve()
    if not input_path.is_file():
        dbmod.update_transcription_job(
            conn,
            job_id,
            status="failed",
            phase="failed",
            progress=1.0,
            error="Input file not found",
        )
        return
    api_key = (settings.openai_api_key or "").strip()
    if not api_key:
        dbmod.update_transcription_job(
            conn,
            job_id,
            status="failed",
            phase="failed",
            progress=1.0,
            error="Missing OPENAI_API_KEY",
        )
        return

    duration = _probe_duration_seconds(input_path)
    dbmod.update_transcription_job(
        conn,
        job_id,
        status="running",
        phase="prepare",
        progress=0.02,
        duration_seconds=duration if duration > 0 else None,
        error=None,
    )

    output_dir = settings.output_dir / "transcriptions"
    output_dir.mkdir(parents=True, exist_ok=True)
    base_name = Path(str(row["filename"] or input_path.name)).stem
    out_txt = output_dir / f"{base_name}_job{job_id}.txt"

    total = duration if duration > 0 else float(_CHUNK_SECONDS)
    dbmod.update_transcription_job(
        conn,
        job_id,
        status="running",
        phase="detect_silence",
        progress=0.04,
        error=None,
    )
    markers = _detect_silence_markers(input_path)
    chunks = _build_silence_aware_chunks(total, markers)

    texts: list[str] = []
    model = str(row.get("model") or settings.openai_transcription_model or "whisper-1")
    language = row.get("language")
    for idx, (chunk_start, chunk_dur) in enumerate(chunks, start=1):
        dbmod.update_transcription_job(
            conn,
            job_id,
            status="running",
            phase=f"transcribe_chunk_{idx}",
            progress=0.08 + (0.86 * ((idx - 1) / max(1, len(chunks)))),
            error=None,
        )
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            chunk_path = Path(tmp.name)
        try:
            _extract_chunk_mp3(input_path, chunk_path, chunk_start, chunk_dur)
            text = _transcribe_chunk_text(
                api_key=api_key,
                model=model,
                language=language,
                audio_path=chunk_path,
            )
        finally:
            try:
                chunk_path.unlink(missing_ok=True)
            except OSError:
                pass
        header = f"[Chunk {idx}/{len(chunks)} | {int(chunk_start)}s - {int(chunk_start + chunk_dur)}s]"
        texts.append(f"{header}\n{text}".strip())

    out_txt.write_text("\n\n".join(texts).strip() + "\n", encoding="utf-8")
    dbmod.update_transcription_job(
        conn,
        job_id,
        status="done",
        phase="done",
        progress=1.0,
        output_txt_path=str(out_txt),
        error=None,
    )
