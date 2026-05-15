"""Tests for Instagram-style karaoke caption burning."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from webapp import caption_burner as cb
from webapp.openai_speech_trim import transcribe_verbose_json


def test_extract_words_from_verbose_top_level():
    verbose = {
        "words": [
            {"word": "Hallo", "start": 0.0, "end": 0.4},
            {"word": "Welt", "start": 0.5, "end": 0.9},
        ]
    }
    words = cb.extract_words_from_verbose(verbose)
    assert len(words) == 2
    assert words[0]["word"] == "Hallo"
    assert words[1]["end"] == pytest.approx(0.9)


def test_extract_words_from_verbose_segment_fallback():
    verbose = {
        "segments": [
            {
                "start": 0.0,
                "end": 1.0,
                "text": " test",
                "words": [{"word": "test", "start": 0.1, "end": 0.5}],
            }
        ]
    }
    words = cb.extract_words_from_verbose(verbose)
    assert len(words) == 1
    assert words[0]["word"] == "test"


def test_build_karaoke_ass_centered_dialogues():
    words = [
        {"word": "Hello", "start": 0.0, "end": 0.3},
        {"word": "World", "start": 0.4, "end": 0.8},
    ]
    ass = cb.build_karaoke_ass(words, width=1080, height=1920)
    assert "PlayResX: 1080" in ass
    assert "PlayResY: 1920" in ass
    assert "Style: Instagram" in ass
    assert ",5,40,40,80,1" in ass  # Alignment 5 = middle center
    assert "Dialogue: 0,0:00:00.00,0:00:00.30,Instagram" in ass
    assert ",HELLO" in ass
    assert ",WORLD" in ass


def test_ass_escape_special_chars():
    words = [{"word": "a{b}", "start": 0.0, "end": 0.2}]
    ass = cb.build_karaoke_ass(words, width=720, height=1280)
    assert r"a\{b\}" in ass


def test_transcribe_verbose_json_requests_word_granularity(monkeypatch, tmp_path: Path):
    audio = tmp_path / "clip.mp3"
    audio.write_bytes(b"fake")

    captured: dict = {}

    class FakeResp:
        def raise_for_status(self):
            return None

        def json(self):
            return {"words": [{"word": "hi", "start": 0.0, "end": 0.2}]}

    class FakeClient:
        def __init__(self, timeout):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def post(self, url, headers, files, data):
            captured["data"] = list(data)
            return FakeResp()

    monkeypatch.setattr("webapp.openai_speech_trim.httpx.Client", FakeClient)
    out = transcribe_verbose_json("key", str(audio), include_word_timestamps=True)
    assert out["words"][0]["word"] == "hi"
    data = captured["data"]
    assert ("timestamp_granularities[]", "word") in data
    assert ("timestamp_granularities[]", "segment") in data


def test_burn_captions_into_video_calls_ffmpeg(monkeypatch, tmp_path: Path):
    video = tmp_path / "out.mp4"
    video.write_bytes(b"not-a-real-video")

    monkeypatch.setattr(cb, "probe_video_size", lambda _p: (1080, 1920))
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        out = cmd[-1]
        Path(out).write_bytes(b"encoded")
        return MagicMock(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cb.subprocess, "run", fake_run)

    words = [{"word": "Hi", "start": 0.0, "end": 0.2}]
    cb.burn_captions_into_video(video, words, work_dir=tmp_path)
    assert video.is_file()
    assert any("ass=" in part for cmd in calls for part in cmd)
    assert any(part == "libx264" for cmd in calls for part in cmd)
