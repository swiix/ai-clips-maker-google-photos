from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from webapp import db as dbmod
from webapp import jobs as jobsmod
from webapp.main import app
from webapp.settings import Settings
from webapp.openai_speech_trim import merge_transcript_segments
from webapp.silence_remover import PROFILES, build_keep_segments, detect_silences


def test_detect_silences_parses_ffmpeg_output(monkeypatch):
    class Proc:
        returncode = 0
        stdout = ""
        stderr = """
        [silencedetect @ x] silence_start: 1.20
        [silencedetect @ x] silence_end: 2.70 | silence_duration: 1.50
        [silencedetect @ x] silence_start: 6.00
        [silencedetect @ x] silence_end: 7.20 | silence_duration: 1.20
        """

    monkeypatch.setattr("webapp.silence_remover.subprocess.run", lambda *a, **k: Proc())
    got = detect_silences("in.mp4", noise_threshold_db=-34, min_silence_sec=0.6)
    assert got == [(1.2, 2.7), (6.0, 7.2)]


def test_build_keep_segments_complements_silences():
    keep = build_keep_segments(
        total_duration=10.0,
        silences=[(1.0, 2.0), (4.0, 5.0)],
        padding_sec=0.1,
        min_keep_sec=0.2,
    )
    assert keep == [(0.0, 1.1), (1.9, 4.1), (4.9, 10.0)]


def test_profiles_are_all_defined():
    names = [p.name for p in PROFILES]
    assert names == ["conservative", "balanced", "aggressive"]


def test_settings_loads_openai_key_from_json(tmp_path: Path):
    cred = tmp_path / "openai_credentials.json"
    cred.write_text('{"openai_api_key": "from-json-key"}', encoding="utf-8")
    s = Settings(data_dir=tmp_path, openai_credentials_json=cred)
    assert s.openai_api_key == "from-json-key"


def test_merge_transcript_segments_merges_small_gaps():
    segs = [{"start": 0.0, "end": 1.0}, {"start": 1.15, "end": 2.5}]
    got = merge_transcript_segments(segs, max_gap_sec=0.35)
    assert got == [(0.0, 2.5)]


def test_api_enqueue_silence_remove_job(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)
    conn.close()

    s = Settings(data_dir=tmp_path, output_dir=tmp_path / "outputs", cache_dir=tmp_path / "cache")
    app.dependency_overrides = {}
    app.dependency_overrides[__import__("webapp.main", fromlist=["_settings_dep"])._settings_dep] = lambda: s

    client = TestClient(app)
    resp = client.post(
        "/api/jobs/silence-remove",
        json={
            "items": [{"id": "m1", "baseUrl": "https://x", "filename": "a.mp4"}],
            "trim_method": "silence_balanced",
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert len(payload["queued_job_ids"]) == 1

    c2 = dbmod.connect(db_path)
    dbmod.init_db(c2)
    row = c2.execute("SELECT job_type, job_options FROM jobs WHERE media_item_id = 'm1'").fetchone()
    c2.close()
    assert row["job_type"] == "silence_remove"
    assert "silence_balanced" in str(row["job_options"] or "")


def test_api_enqueue_openai_trim_job(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)
    conn.close()

    s = Settings(data_dir=tmp_path, output_dir=tmp_path / "outputs", cache_dir=tmp_path / "cache")
    app.dependency_overrides = {}
    app.dependency_overrides[__import__("webapp.main", fromlist=["_settings_dep"])._settings_dep] = lambda: s

    client = TestClient(app)
    resp = client.post(
        "/api/jobs/silence-remove",
        json={
            "items": [{"id": "m_openai", "baseUrl": "https://x", "filename": "a.mp4"}],
            "trim_method": "openai_speech",
        },
    )
    assert resp.status_code == 200
    c2 = dbmod.connect(db_path)
    dbmod.init_db(c2)
    row = c2.execute("SELECT job_type, job_options FROM jobs WHERE media_item_id = 'm_openai'").fetchone()
    c2.close()
    assert row["job_type"] == "openai_speech_trim"
    assert "openai_speech" in str(row["job_options"] or "")


def test_worker_silence_remove_done(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)
    dbmod.create_or_requeue_job(
        conn,
        "m2",
        filename="b.mp4",
        base_url="https://x",
        job_type="silence_remove",
        job_options='{"trim_method":"silence_conservative","profiles":["conservative"]}',
    )
    row = conn.execute("SELECT id FROM jobs WHERE media_item_id = 'm2'").fetchone()
    job_id = int(row["id"])

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "m2.mp4"
    cache_file.write_bytes(b"video")

    settings = Settings(data_dir=tmp_path, output_dir=tmp_path / "out", cache_dir=cache_dir)

    monkeypatch.setattr("webapp.jobs._is_valid_cached_av", lambda *_a, **_k: True)
    monkeypatch.setattr(
        "webapp.silence_remover.remove_silence_selected_profiles",
        lambda *_a, **_k: [
            {"profile": "conservative"},
        ],
    )

    jobsmod._run_one_job(conn, settings, job_id)
    out = conn.execute("SELECT status, phase, output_dir FROM jobs WHERE id = ?", (job_id,)).fetchone()
    conn.close()
    assert out["status"] == "done"
    assert out["phase"] == "done"
    assert out["output_dir"] == str(settings.output_dir)


def test_worker_openai_trim_done(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)
    dbmod.create_or_requeue_job(
        conn,
        "m3",
        filename="c.mp4",
        base_url="https://x",
        job_type="openai_speech_trim",
        job_options='{"trim_method":"openai_speech"}',
    )
    row = conn.execute("SELECT id FROM jobs WHERE media_item_id = 'm3'").fetchone()
    job_id = int(row["id"])

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "m3.mp4").write_bytes(b"video")

    settings = Settings(
        data_dir=tmp_path,
        output_dir=tmp_path / "out",
        cache_dir=cache_dir,
        openai_api_key="test-key",
    )

    monkeypatch.setattr("webapp.jobs._is_valid_cached_av", lambda *_a, **_k: True)
    monkeypatch.setattr(
        "webapp.openai_speech_trim.trim_video_to_openai_speech",
        lambda *_a, **_k: {"video_path": str(tmp_path / "out" / "x.mp4")},
    )

    jobsmod._run_one_job(conn, settings, job_id)
    out = conn.execute("SELECT status, phase, job_type FROM jobs WHERE id = ?", (job_id,)).fetchone()
    conn.close()
    assert out["status"] == "done"
    assert out["phase"] == "done"
    assert out["job_type"] == "openai_speech_trim"
