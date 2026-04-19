"""
Trim video to speech-only segments using OpenAI audio transcription (Whisper API).
"""

from __future__ import annotations

import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional, Sequence

import httpx

from webapp.silence_remover import (
    duration_before_after_tag,
    output_duration_from_keep_segments,
    probe_duration_seconds,
    render_keep_segments_video,
)

logger = logging.getLogger(__name__)

OPENAI_TRANSCRIPTIONS_URL = "https://api.openai.com/v1/audio/transcriptions"


def _extract_audio_mp3(video_path: str, mp3_path: str) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        video_path,
        "-vn",
        "-acodec",
        "libmp3lame",
        "-q:a",
        "4",
        mp3_path,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"ffmpeg audio extract failed: {(proc.stderr or proc.stdout or '').strip() or proc.returncode}"
        )


def merge_transcript_segments(
    segments: list[dict[str, Any]],
    *,
    max_gap_sec: float = 0.35,
    min_duration_sec: float = 0.04,
) -> list[tuple[float, float]]:
    intervals: list[tuple[float, float]] = []
    for seg in segments:
        try:
            start = float(seg.get("start", 0.0))
            end = float(seg.get("end", 0.0))
        except (TypeError, ValueError):
            continue
        if end - start >= min_duration_sec:
            intervals.append((start, end))
    intervals.sort(key=lambda x: x[0])
    if not intervals:
        return []
    merged: list[tuple[float, float]] = [intervals[0]]
    for s, e in intervals[1:]:
        ls, le = merged[-1]
        if s - le <= max_gap_sec:
            merged[-1] = (ls, max(le, e))
        else:
            merged.append((s, e))
    return merged


def transcribe_verbose_json(
    api_key: str,
    audio_path: str,
    *,
    model: str = "whisper-1",
    timeout_sec: float = 600.0,
) -> dict[str, Any]:
    path = Path(audio_path)
    data_bytes = path.read_bytes()
    if len(data_bytes) > 25 * 1024 * 1024:
        raise RuntimeError(
            "Audio file exceeds OpenAI transcription size limit (~25MB). "
            "Try a shorter video or lower bitrate extract."
        )
    with httpx.Client(timeout=timeout_sec) as client:
        resp = client.post(
            OPENAI_TRANSCRIPTIONS_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            files={"file": (path.name, data_bytes, "audio/mpeg")},
            data={
                "model": model,
                "response_format": "verbose_json",
            },
        )
    try:
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = resp.text
        try:
            detail = str(resp.json())
        except Exception:
            pass
        raise RuntimeError(f"OpenAI transcription failed: {detail}") from exc
    return resp.json()


def trim_video_to_openai_speech(
    input_video: str,
    output_dir: str,
    output_prefix: str,
    api_key: str,
    *,
    model: str = "whisper-1",
    usd_per_minute: float = 0.006,
    merge_gap_sec: float = 0.35,
    min_segment_sec: float = 0.04,
    music_exclude_intervals: Optional[Sequence[tuple[float, float]]] = None,
) -> dict[str, str]:
    """
    Transcribe audio via OpenAI, keep only intervals where speech segments exist, concat video.
    Writes MP4 and a JSON sidecar with raw verbose response.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    total = probe_duration_seconds(input_video)
    if total <= 0:
        raise RuntimeError("Input video duration is invalid (<= 0).")

    with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
        mp3_path = tmp.name
    try:
        _extract_audio_mp3(input_video, mp3_path)
        verbose = transcribe_verbose_json(api_key, mp3_path, model=model)
    finally:
        try:
            Path(mp3_path).unlink(missing_ok=True)
        except OSError:
            pass

    segs = verbose.get("segments")
    if not isinstance(segs, list):
        segs = []
    keep = merge_transcript_segments(
        segs,
        max_gap_sec=max(0.01, float(merge_gap_sec)),
        min_duration_sec=max(0.01, float(min_segment_sec)),
    )
    if not keep:
        raise RuntimeError("OpenAI returned no speech segments to keep.")
    if music_exclude_intervals:
        from webapp.music_remover import subtract_intervals_from_keep

        keep = subtract_intervals_from_keep(
            keep,
            music_exclude_intervals,
            total_duration=total,
            min_keep_sec=max(0.01, float(min_segment_sec)),
        )
    if not keep:
        raise RuntimeError("All speech segments were removed as music-overlap cuts. Disable 'Remove music' or review the source.")

    # Clamp to duration
    clamped: list[tuple[float, float]] = []
    for s, e in keep:
        s = max(0.0, min(s, total))
        e = max(0.0, min(e, total))
        if e - s > 0.05:
            clamped.append((s, e))
    if not clamped:
        raise RuntimeError("No valid keep intervals after clamping to video duration.")

    after = output_duration_from_keep_segments(clamped)
    dur_tag = duration_before_after_tag(total, after)
    base_name = f"{output_prefix}_{dur_tag}"
    raw_path = out_dir / f"{base_name}_openai_verbose.json"
    raw_path.write_text(json.dumps(verbose, ensure_ascii=True, indent=2), encoding="utf-8")
    out_video = out_dir / f"{base_name}_speech_openai.mp4"
    render_keep_segments_video(input_video, str(out_video), clamped)
    billed_minutes = total / 60.0
    cost_usd = billed_minutes * float(usd_per_minute)
    logger.info(
        "OpenAI speech trim: %s segments -> %s (duration %.2fs, est. $%.4f)",
        len(clamped),
        out_video,
        total,
        cost_usd,
    )
    return {
        "video_path": str(out_video),
        "transcript_json": str(raw_path),
        "segments_kept": str(len(clamped)),
        "input_audio_seconds": f"{total:.6f}",
        "output_video_seconds": f"{after:.6f}",
        "billed_minutes": f"{billed_minutes:.6f}",
        "estimated_cost_usd": f"{cost_usd:.6f}",
    }

