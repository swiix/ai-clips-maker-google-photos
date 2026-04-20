"""
Speech-only trimming via Silero VAD (snakers4/silero-vad).

Keeps segments where speech is detected; non-speech (silence, music beds without speech)
is removed after merge/subtract steps consistent with other trim modes.
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional, Sequence

from webapp.silence_remover import (
    duration_before_after_tag,
    merge_keep_segments_by_gap,
    output_duration_from_keep_segments,
    probe_duration_seconds,
    render_keep_segments_video,
)

logger = logging.getLogger(__name__)

_VAD_SAMPLE_RATE = 16000
_VAD_MODEL_CACHE = None


def _load_vad_model():
    global _VAD_MODEL_CACHE
    if _VAD_MODEL_CACHE is None:
        try:
            from silero_vad import load_silero_vad
        except ImportError as exc:
            raise RuntimeError(
                "silero-vad is not installed. Install with: pip install silero-vad torchaudio "
                "(see project extras [web])."
            ) from exc
        _VAD_MODEL_CACHE = load_silero_vad()
    return _VAD_MODEL_CACHE


def _extract_mono_wav_16k(video_path: str, wav_out: str) -> None:
    cmd = [
        "ffmpeg",
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        video_path,
        "-ac",
        "1",
        "-ar",
        str(_VAD_SAMPLE_RATE),
        wav_out,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            (proc.stderr or proc.stdout or "").strip() or "ffmpeg WAV extract failed"
        )


def speech_keep_segments_from_video(
    video_path: str,
    *,
    merge_gap_sec: float = 0.35,
    min_segment_sec: float = 0.04,
    vad_threshold: float = 0.5,
    music_exclude_intervals: Optional[Sequence[tuple[float, float]]] = None,
) -> tuple[list[tuple[float, float]], float]:
    """
    Run Silero VAD on the video's audio track and return (keep_segments, total_duration_sec).
    """
    total = probe_duration_seconds(video_path)
    if total <= 0:
        raise RuntimeError("Input duration is invalid (<= 0).")

    fd, wav_path = tempfile.mkstemp(suffix=".wav")
    os.close(fd)
    try:
        _extract_mono_wav_16k(video_path, wav_path)
        try:
            from silero_vad import get_speech_timestamps, read_audio
        except ImportError as exc:
            raise RuntimeError(
                "silero-vad is not installed. Install with: pip install silero-vad torchaudio "
                "(see project extras [web])."
            ) from exc

        wav = read_audio(wav_path, sampling_rate=_VAD_SAMPLE_RATE)
        model = _load_vad_model()
        timestamps = get_speech_timestamps(
            wav,
            model,
            sampling_rate=_VAD_SAMPLE_RATE,
            threshold=float(vad_threshold),
            return_seconds=True,
        )
    finally:
        try:
            os.unlink(wav_path)
        except OSError:
            pass

    intervals: list[tuple[float, float]] = []
    for d in timestamps:
        try:
            s = float(d["start"])
            e = float(d["end"])
        except (KeyError, TypeError, ValueError):
            continue
        if e <= s:
            continue
        intervals.append((max(0.0, s), min(total, e)))

    if not intervals:
        raise RuntimeError(
            "Silero VAD found no speech segments. "
            "Try OpenAI speech trim or a silence-based profile if audio is very noisy or off-axis."
        )

    merged = merge_keep_segments_by_gap(intervals, max(0.0, float(merge_gap_sec)))

    mk = max(0.01, float(min_segment_sec))
    filtered = [(s, e) for s, e in merged if e - s >= mk]
    if not filtered:
        raise RuntimeError(
            "All detected speech chunks were shorter than the minimum segment duration. "
            "Lower the minimum segment duration in cut settings."
        )

    if music_exclude_intervals:
        from webapp.music_remover import subtract_intervals_from_keep

        filtered = subtract_intervals_from_keep(
            filtered,
            music_exclude_intervals,
            total_duration=total,
            min_keep_sec=mk,
        )
        if not filtered:
            raise RuntimeError(
                "VAD trimming left no segments after removing detected music intervals. "
                "Disable 'Remove music' or try another trim mode."
            )

    return filtered, total


def trim_video_silero_vad(
    input_video: str,
    output_dir: str,
    output_prefix: str,
    *,
    merge_gap_sec: float = 0.35,
    min_segment_sec: float = 0.04,
    vad_threshold: Optional[float] = None,
    music_exclude_intervals: Optional[Sequence[tuple[float, float]]] = None,
) -> dict[str, Any]:
    """Export a single MP4 containing only VAD-detected speech segments."""
    thr = 0.5 if vad_threshold is None else float(vad_threshold)
    keep, total = speech_keep_segments_from_video(
        input_video,
        merge_gap_sec=merge_gap_sec,
        min_segment_sec=min_segment_sec,
        vad_threshold=thr,
        music_exclude_intervals=music_exclude_intervals,
    )
    out_dur = output_duration_from_keep_segments(keep)
    dur_tag = duration_before_after_tag(total, out_dur)
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = Path(output_dir) / f"{output_prefix}_{dur_tag}_vad_silero.mp4"
    render_keep_segments_video(input_video, str(output_path), keep)
    out_name = output_path.name
    logger.info(
        "Silero VAD trim done: %s segments -> %s (%.2fs -> %.2fs)",
        len(keep),
        out_name,
        total,
        out_dur,
    )
    return {
        "video_path": str(output_path),
        "input_audio_seconds": f"{total:.6f}",
        "output_video_seconds": f"{out_dur:.6f}",
        "estimated_cost_usd": "0",
    }
