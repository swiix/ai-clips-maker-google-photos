from webapp.openai_cost import estimate_whisper_cost_usd


def test_estimate_whisper_cost_usd_from_duration():
    # 103575 s ≈ OpenAI dashboard example → ~$10.36 at $0.006/min
    cost = estimate_whisper_cost_usd(103575.0, usd_per_minute=0.006)
    assert cost == 10.3575


def test_estimate_whisper_cost_usd_none_for_missing_duration():
    assert estimate_whisper_cost_usd(None) is None
    assert estimate_whisper_cost_usd(0) is None
    assert estimate_whisper_cost_usd(-1) is None
