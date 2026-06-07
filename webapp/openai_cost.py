"""Shared helpers for estimating OpenAI transcription API cost."""

from __future__ import annotations

# Per-minute audio rates from https://developers.openai.com/api/docs/pricing (transcription models).
OPENAI_TRANSCRIPTION_USD_PER_MINUTE: dict[str, float] = {
    "whisper-1": 0.006,
    "gpt-4o-transcribe": 0.006,
    "gpt-4o-transcribe-diarize": 0.006,
    "gpt-4o-mini-transcribe": 0.003,
}


def transcription_usd_per_minute(
    model: str | None,
    *,
    fallback_usd_per_minute: float = 0.006,
) -> float:
    """Return USD/min for a transcription model; unknown models use fallback."""
    key = str(model or "whisper-1").strip().lower()
    return float(OPENAI_TRANSCRIPTION_USD_PER_MINUTE.get(key, fallback_usd_per_minute))


def estimate_transcription_cost_usd(
    duration_seconds: float | None,
    *,
    model: str | None = None,
    usd_per_minute: float | None = None,
    fallback_usd_per_minute: float = 0.006,
) -> float | None:
    """
    Estimate USD cost from audio duration (OpenAI bills by audio length).

    Uses model-specific rates when ``usd_per_minute`` is not provided.
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
    rate = (
        float(usd_per_minute)
        if usd_per_minute is not None
        else transcription_usd_per_minute(model, fallback_usd_per_minute=fallback_usd_per_minute)
    )
    return round((seconds / 60.0) * rate, 6)


def estimate_whisper_cost_usd(
    duration_seconds: float | None,
    *,
    usd_per_minute: float = 0.006,
    model: str | None = None,
) -> float | None:
    """Backward-compatible alias; prefer ``estimate_transcription_cost_usd``."""
    return estimate_transcription_cost_usd(
        duration_seconds,
        model=model,
        usd_per_minute=usd_per_minute if model is None else None,
        fallback_usd_per_minute=usd_per_minute,
    )
