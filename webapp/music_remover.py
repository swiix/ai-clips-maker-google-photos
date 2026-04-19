"""
Detect sustained instrumental / background music via harmonic-percussive separation
on the magnitude spectrogram, then expose intervals so callers can cut them out.

Heuristic only (no classifier): tuned for removing obvious BGM beds under speech vlogs.
"""

from __future__ import annotations

import logging
import subprocess
from typing import Optional, Sequence

import numpy as np
from scipy.ndimage import median_filter
from scipy import signal

logger = logging.getLogger(__name__)

_ANALYSIS_SR = 11025
_STFT_NPERSEG = 2048
_STFT_NOVERLAP = 1536
_HARMONIC_MEDIAN_WIDTH = 41
_RATIO_STRONG = 0.54
_MIN_MUSIC_SEGMENT_SEC = 1.05
_MAX_ANALYSIS_SECONDS = 7200.0


def load_mono_audio_f32(video_path: str, sample_rate: int = _ANALYSIS_SR) -> tuple[np.ndarray, float]:
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        video_path,
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        "-f",
        "f32le",
        "-acodec",
        "pcm_f32le",
        "-",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(
            (proc.stderr or b"").decode("utf-8", errors="replace").strip() or "ffmpeg audio decode failed"
        )
    audio = np.frombuffer(proc.stdout, dtype=np.float32)
    if audio.size == 0:
        raise RuntimeError("ffmpeg produced empty audio.")
    dur = float(audio.size) / float(sample_rate)
    return audio, dur


def _merge_sorted_intervals(intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x[0])
    merged: list[tuple[float, float]] = [intervals[0]]
    for s, e in intervals[1:]:
        ls, le = merged[-1]
        if s <= le + 1e-4:
            merged[-1] = (ls, max(le, e))
        else:
            merged.append((s, e))
    return merged


def subtract_intervals_from_keep(
    keep: Sequence[tuple[float, float]],
    remove: Sequence[tuple[float, float]],
    *,
    total_duration: float,
    min_keep_sec: float,
) -> list[tuple[float, float]]:
    """
    Clip each keep range by subtracting overlaps with ``remove`` (e.g. music intervals).
    """
    if not remove:
        return list(keep)
    td = max(0.0, float(total_duration))
    mk = max(1e-4, float(min_keep_sec))
    remove_m = _merge_sorted_intervals(
        [(max(0.0, a), min(td, b)) for a, b in remove if b > a and a < td]
    )
    if not remove_m:
        return list(keep)
    out: list[tuple[float, float]] = []
    for ks, ke in keep:
        ks = max(0.0, float(ks))
        ke = min(td, float(ke))
        if ke - ks < mk:
            continue
        parts: list[tuple[float, float]] = [(ks, ke)]
        for rs, re in remove_m:
            if re <= rs:
                continue
            new_parts: list[tuple[float, float]] = []
            for ps, pe in parts:
                if re <= ps or rs >= pe:
                    new_parts.append((ps, pe))
                else:
                    if rs > ps + 1e-6:
                        new_parts.append((ps, min(rs, pe)))
                    if re < pe - 1e-6:
                        new_parts.append((max(re, ps), pe))
            parts = new_parts
        for ps, pe in parts:
            if pe - ps >= mk:
                out.append((ps, pe))
    return out


def _frame_harmonic_ratio(magnitude: np.ndarray) -> np.ndarray:
    """Per time frame: harmonic energy share (HPSS-style median filtering along time)."""
    h_w = max(3, int(_HARMONIC_MEDIAN_WIDTH))
    if h_w % 2 == 0:
        h_w += 1
    H = median_filter(magnitude, size=(1, h_w))
    P = np.maximum(magnitude - H, 0.0)
    he = np.sum(H * H, axis=0)
    pe = np.sum(P * P, axis=0)
    den = he + pe + 1e-12
    return he / den


def _ratio_to_music_mask(
    ratio: np.ndarray,
    hop_sec: float,
    rms: Optional[np.ndarray] = None,
) -> np.ndarray:
    """Harmonic-ratio + RMS gate; keep runs longer than ``_MIN_MUSIC_SEGMENT_SEC``."""
    active = ratio > _RATIO_STRONG
    if rms is not None and rms.shape[0] == ratio.shape[0]:
        floor = float(np.percentile(rms, 35)) * 0.35
        active &= rms > max(floor, 1e-8)

    mask = np.zeros_like(active, dtype=bool)
    min_frames = max(2, int(_MIN_MUSIC_SEGMENT_SEC / max(hop_sec, 1e-6)))
    i = 0
    n = len(active)
    while i < n:
        if not active[i]:
            i += 1
            continue
        j = i + 1
        while j < n and active[j]:
            j += 1
        if (j - i) >= min_frames:
            mask[i:j] = True
        i = j
    return mask


def _mask_to_intervals(
    mask: np.ndarray,
    times: np.ndarray,
    total_duration: float,
    hop_sec: float,
) -> list[tuple[float, float]]:
    intervals: list[tuple[float, float]] = []
    i = 0
    n = len(mask)
    while i < n:
        if not mask[i]:
            i += 1
            continue
        j = i + 1
        while j < n and mask[j]:
            j += 1
        t0 = float(times[i]) if i < len(times) else 0.0
        t1 = min(total_duration, float(times[j - 1]) + max(hop_sec, 0.02)) if j - 1 < len(times) else total_duration
        if t1 - t0 >= _MIN_MUSIC_SEGMENT_SEC * 0.75:
            intervals.append((max(0.0, t0), min(total_duration, t1)))
        i = j
    return _merge_sorted_intervals(intervals)


def detect_music_intervals(
    video_path: str,
    *,
    max_seconds: Optional[float] = None,
) -> list[tuple[float, float]]:
    """
    Return time ranges [start, end) where sustained harmonic background is likely music.

    Empty list means no confident music regions (or analysis failed gracefully).
    """
    cap = float(max_seconds) if max_seconds is not None else _MAX_ANALYSIS_SECONDS
    try:
        audio, dur = load_mono_audio_f32(video_path, _ANALYSIS_SR)
    except Exception as exc:
        logger.warning("Music detection: could not decode audio: %s", exc)
        return []

    if dur <= 0.5:
        return []

    if dur > cap:
        audio = audio[: int(cap * _ANALYSIS_SR)]
        dur = float(audio.size) / float(_ANALYSIS_SR)

    _f, times, Zxx = signal.stft(
        audio,
        fs=_ANALYSIS_SR,
        nperseg=_STFT_NPERSEG,
        noverlap=_STFT_NOVERLAP,
        boundary=None,
        padded=False,
    )
    magnitude = np.abs(Zxx)
    ratio = _frame_harmonic_ratio(magnitude)
    frame_energy = np.sqrt(np.mean(magnitude * magnitude, axis=0))
    hop_sec = (_STFT_NPERSEG - _STFT_NOVERLAP) / float(_ANALYSIS_SR)
    mask = _ratio_to_music_mask(ratio, hop_sec, rms=frame_energy)
    intervals = _mask_to_intervals(mask, times, dur, hop_sec)
    if intervals:
        logger.info(
            "Music detection: %s segments (total %.2fs flagged of %.2fs)",
            len(intervals),
            sum(max(0.0, b - a) for a, b in intervals),
            dur,
        )
    return intervals
