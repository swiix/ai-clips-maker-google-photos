from webapp.openai_cost import (
    estimate_transcription_cost_usd,
    estimate_whisper_cost_usd,
    transcription_usd_per_minute,
)


def test_estimate_whisper_cost_usd_from_duration():
    # 103575 s ≈ OpenAI dashboard example → ~$10.36 at $0.006/min
    cost = estimate_whisper_cost_usd(103575.0, usd_per_minute=0.006)
    assert cost == 10.3575


def test_estimate_whisper_cost_usd_none_for_missing_duration():
    assert estimate_whisper_cost_usd(None) is None
    assert estimate_whisper_cost_usd(0) is None
    assert estimate_whisper_cost_usd(-1) is None


def test_transcription_usd_per_minute_by_model():
    assert transcription_usd_per_minute("whisper-1") == 0.006
    assert transcription_usd_per_minute("gpt-4o-transcribe") == 0.006
    assert transcription_usd_per_minute("gpt-4o-mini-transcribe") == 0.003
    assert transcription_usd_per_minute("unknown-model", fallback_usd_per_minute=0.005) == 0.005


def test_estimate_transcription_cost_usd_uses_model_rate():
    cost = estimate_transcription_cost_usd(600.0, model="gpt-4o-mini-transcribe")
    assert cost == 0.03
    cost_whisper = estimate_transcription_cost_usd(600.0, model="whisper-1")
    assert cost_whisper == 0.06
