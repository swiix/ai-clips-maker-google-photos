"""Shared helpers for estimating OpenAI Whisper transcription cost."""

from __future__ import annotations


def estimate_whisper_cost_usd(
    duration_seconds: float | None,
    *,
    usd_per_minute: float = 0.006,
) -> float | None:
    """
    Estimate USD cost from audio duration (OpenAI bills by audio length).

    Returns None when duration is unknown or non-positive.
    """
    if duration_seconds is None:
        return None
    try:
        seconds = float(duration_seconds)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    return round((seconds / 60.0) * float(usd_per_minute), 6)
