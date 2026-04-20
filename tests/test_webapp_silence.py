from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from webapp import db as dbmod
from webapp import jobs as jobsmod
from webapp.main import app
from webapp.settings import Settings
from webapp.openai_speech_trim import merge_transcript_segments
from webapp.silence_remover import (
    PROFILES,
    build_keep_segments,
    detect_silences,
    duration_before_after_tag,
    format_duration_for_filename,
    output_duration_from_keep_segments,
    render_keep_segments_video,
)


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


def test_duration_filename_tags():
    assert format_duration_for_filename(120.0) == "120s"
    assert format_duration_for_filename(45.25) == "45s"
    assert output_duration_from_keep_segments([(0.0, 2.0), (5.0, 8.0)]) == 5.0
    assert duration_before_after_tag(100.0, 45.25) == "100s_to_45s"


def test_profiles_are_all_defined():
    names = [p.name for p in PROFILES]
    assert names == ["conservative", "balanced", "aggressive"]


def test_render_keep_segments_retries_without_audio_stream(monkeypatch):
    calls: list[list[str]] = []

    class OkProc:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(cmd, *args, **kwargs):
        calls.append(list(cmd))
        cmd_str = " ".join(cmd)
        if "[0:a]atrim" in cmd_str:
            raise subprocess.CalledProcessError(
                returncode=69,
                cmd=cmd,
                stderr="Stream specifier ':a' in filtergraph description matches no streams.",
                output="",
            )
        return OkProc()

    import subprocess

    monkeypatch.setattr("webapp.silence_remover.subprocess.run", fake_run)
    render_keep_segments_video("input.mov", "out.mp4", [(0.0, 2.0), (3.0, 5.0)])

    assert len(calls) == 2
    first = " ".join(calls[0])
    second = " ".join(calls[1])
    assert "[0:a]atrim" in first
    assert "[0:a]atrim" not in second
    assert "concat=n=2:v=1:a=0" in second
    assert " -an " in f" {second} "


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
    dbmod.prepare_database(conn)
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
    dbmod.prepare_database(c2)
    row = c2.execute("SELECT job_type, job_options FROM jobs WHERE media_item_id = 'm1'").fetchone()
    c2.close()
    assert row["job_type"] == "silence_remove"
    assert "silence_balanced" in str(row["job_options"] or "")


def test_api_enqueue_silero_vad_trim(tmp_path: Path):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.close()

    s = Settings(data_dir=tmp_path, output_dir=tmp_path / "outputs", cache_dir=tmp_path / "cache")
    app.dependency_overrides = {}
    app.dependency_overrides[__import__("webapp.main", fromlist=["_settings_dep"])._settings_dep] = lambda: s

    client = TestClient(app)
    resp = client.post(
        "/api/jobs/silence-remove",
        json={
            "items": [{"id": "m_vad", "baseUrl": "https://x", "filename": "a.mp4"}],
            "trim_method": "silero_vad",
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert len(payload["queued_job_ids"]) == 1

    c2 = dbmod.connect(db_path)
    dbmod.prepare_database(c2)
    row = c2.execute("SELECT job_type, job_options FROM jobs WHERE media_item_id = 'm_vad'").fetchone()
    c2.close()
    assert row["job_type"] == "silence_remove"
    opts = json.loads(str(row["job_options"] or "{}"))
    assert opts.get("trim_method") == "silero_vad"


def test_api_enqueue_silero_vad_persists_threshold(tmp_path: Path):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.close()

    s = Settings(data_dir=tmp_path, output_dir=tmp_path / "outputs", cache_dir=tmp_path / "cache")
    app.dependency_overrides = {}
    app.dependency_overrides[__import__("webapp.main", fromlist=["_settings_dep"])._settings_dep] = lambda: s

    client = TestClient(app)
    resp = client.post(
        "/api/jobs/silence-remove",
        json={
            "items": [{"id": "m_thr", "baseUrl": "https://x", "filename": "a.mp4"}],
            "trim_method": "silero_vad",
            "silero_vad_threshold": 0.42,
        },
    )
    assert resp.status_code == 200

    c2 = dbmod.connect(db_path)
    dbmod.prepare_database(c2)
    row = c2.execute("SELECT job_options FROM jobs WHERE media_item_id = 'm_thr'").fetchone()
    c2.close()
    opts = json.loads(str(row["job_options"] or "{}"))
    assert pytest.approx(float(opts.get("silero_vad_threshold")), rel=0, abs=1e-6) == 0.42


def test_api_enqueue_clip_pipeline_persists_cut_controls(tmp_path: Path):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.close()

    s = Settings(data_dir=tmp_path, output_dir=tmp_path / "outputs", cache_dir=tmp_path / "cache")
    app.dependency_overrides = {}
    app.dependency_overrides[__import__("webapp.main", fromlist=["_settings_dep"])._settings_dep] = lambda: s

    client = TestClient(app)
    resp = client.post(
        "/api/jobs",
        json={
            "items": [{"id": "m_pipe", "baseUrl": "https://x", "filename": "p.mp4"}],
            "cut_merge_gap_sec": 0.9,
            "cut_min_duration_sec": 0.3,
            "noise_reduction": True,
        },
    )
    assert resp.status_code == 200
    c2 = dbmod.connect(db_path)
    dbmod.prepare_database(c2)
    row = c2.execute("SELECT job_type, job_options FROM jobs WHERE media_item_id = 'm_pipe'").fetchone()
    c2.close()
    assert row["job_type"] == "clip_pipeline"
    opts = json.loads(str(row["job_options"] or "{}"))
    assert opts["cut_merge_gap_sec"] == pytest.approx(0.9)
    assert opts["cut_min_duration_sec"] == pytest.approx(0.3)
    assert opts["noise_reduction"] is True
    assert opts["noise_reduction_mode"] == "auto"
    assert opts.get("remove_music") is False


def test_api_enqueue_remove_music_persisted(tmp_path: Path):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.close()

    s = Settings(data_dir=tmp_path, output_dir=tmp_path / "outputs", cache_dir=tmp_path / "cache")
    app.dependency_overrides = {}
    app.dependency_overrides[__import__("webapp.main", fromlist=["_settings_dep"])._settings_dep] = lambda: s

    client = TestClient(app)
    resp = client.post(
        "/api/jobs/silence-remove",
        json={
            "items": [{"id": "m_music", "baseUrl": "https://x", "filename": "a.mp4"}],
            "trim_method": "silence_balanced",
            "remove_music": True,
        },
    )
    assert resp.status_code == 200
    c2 = dbmod.connect(db_path)
    dbmod.prepare_database(c2)
    row = c2.execute("SELECT job_options FROM jobs WHERE media_item_id = 'm_music'").fetchone()
    c2.close()
    opts = json.loads(str(row["job_options"] or "{}"))
    assert opts.get("remove_music") is True


def test_api_enqueue_openai_trim_job(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
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
            "cut_merge_gap_sec": 0.8,
            "cut_min_duration_sec": 0.2,
            "noise_reduction_mode": "strong",
        },
    )
    assert resp.status_code == 200
    c2 = dbmod.connect(db_path)
    dbmod.prepare_database(c2)
    row = c2.execute("SELECT job_type, job_options FROM jobs WHERE media_item_id = 'm_openai'").fetchone()
    c2.close()
    assert row["job_type"] == "openai_speech_trim"
    assert "openai_speech" in str(row["job_options"] or "")
    opts = json.loads(str(row["job_options"] or "{}"))
    assert opts["cut_merge_gap_sec"] == pytest.approx(0.8)
    assert opts["cut_min_duration_sec"] == pytest.approx(0.2)
    assert opts["noise_reduction"] is True
    assert opts["noise_reduction_mode"] == "strong"


def test_api_requeues_done_silence_job(tmp_path: Path):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.execute(
        """
        INSERT INTO jobs (
            media_item_id, filename, base_url, status, phase, phase_message, progress,
            job_type, job_options, created_at, updated_at
        ) VALUES (?, ?, ?, 'done', 'done', 'old', 1.0, 'silence_remove', ?, 0, 0)
        """,
        ("m_done", "d.mp4", "https://x", '{"trim_method":"silence_balanced"}'),
    )
    conn.commit()
    conn.close()

    s = Settings(data_dir=tmp_path, output_dir=tmp_path / "outputs", cache_dir=tmp_path / "cache")
    app.dependency_overrides = {}
    app.dependency_overrides[__import__("webapp.main", fromlist=["_settings_dep"])._settings_dep] = lambda: s

    client = TestClient(app)
    resp = client.post(
        "/api/jobs/silence-remove",
        json={
            "items": [{"id": "m_done", "baseUrl": "https://x", "filename": "d.mp4"}],
            "trim_method": "silence_balanced",
        },
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert len(payload["queued_job_ids"]) == 1

    c2 = dbmod.connect(db_path)
    dbmod.prepare_database(c2)
    row = c2.execute("SELECT status, phase FROM jobs WHERE media_item_id = 'm_done'").fetchone()
    c2.close()
    assert row["status"] == "queued"
    assert row["phase"] == "queued"


def test_worker_silence_remove_done(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    dbmod.create_or_requeue_job(
        conn,
        "m2",
        filename="b.mp4",
        base_url="https://x",
        job_type="silence_remove",
        job_options='{"trim_method":"silence_conservative","profiles":["conservative"],"cut_merge_gap_sec":0.5,"cut_min_duration_sec":0.3}',
    )
    row = conn.execute("SELECT id FROM jobs WHERE media_item_id = 'm2'").fetchone()
    job_id = int(row["id"])

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "m2.mp4"
    cache_file.write_bytes(b"video")

    settings = Settings(data_dir=tmp_path, output_dir=tmp_path / "out", cache_dir=cache_dir)

    monkeypatch.setattr("webapp.jobs._is_valid_cached_av", lambda *_a, **_k: True)
    captured_silence: dict[str, object] = {}

    def fake_silence(*_a, **kwargs):
        captured_silence.update(kwargs)
        return [{"profile": "conservative"}]

    monkeypatch.setattr("webapp.silence_remover.remove_silence_selected_profiles", fake_silence)

    jobsmod._run_one_job(conn, settings, job_id)
    out = conn.execute(
        "SELECT status, phase, output_dir, outputs_created, openai_cost_usd FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    conn.close()
    assert out["status"] == "done"
    assert out["phase"] == "done"
    assert out["output_dir"] == str(settings.output_dir)
    assert int(out["outputs_created"] or 0) == 1
    assert out["openai_cost_usd"] is None
    assert float(captured_silence.get("override_merge_gap_sec") or 0.0) == pytest.approx(0.5)
    assert float(captured_silence.get("override_min_keep_sec") or 0.0) == pytest.approx(0.3)


def test_worker_silero_vad_done(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    dbmod.create_or_requeue_job(
        conn,
        "m_vad",
        filename="b.mp4",
        base_url="https://x",
        job_type="silence_remove",
        job_options='{"trim_method":"silero_vad","cut_merge_gap_sec":0.6,"cut_min_duration_sec":0.05,"silero_vad_threshold":1.0}',
        trim_method_label="silero_vad",
    )
    row = conn.execute("SELECT id FROM jobs WHERE media_item_id = 'm_vad'").fetchone()
    job_id = int(row["id"])

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "m_vad.mp4").write_bytes(b"video")

    settings = Settings(data_dir=tmp_path, output_dir=tmp_path / "out", cache_dir=cache_dir)

    monkeypatch.setattr("webapp.jobs._is_valid_cached_av", lambda *_a, **_k: True)
    captured: dict[str, object] = {}

    def fake_vad(*_a, **kwargs):
        captured.update(kwargs)
        return {
            "video_path": str(tmp_path / "out" / "x_vad.mp4"),
            "input_audio_seconds": "90.000000",
            "output_video_seconds": "42.500000",
            "estimated_cost_usd": "0",
        }

    monkeypatch.setattr(
        "webapp.vad_speech_trim.trim_video_silero_vad",
        fake_vad,
    )

    jobsmod._run_one_job(conn, settings, job_id)
    out = conn.execute(
        "SELECT status, phase, job_type, outputs_created, cut_input_seconds, cut_output_seconds FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    conn.close()
    assert out["status"] == "done"
    assert out["phase"] == "done"
    assert out["job_type"] == "silence_remove"
    assert int(out["outputs_created"] or 0) == 1
    assert abs(float(out["cut_input_seconds"] or 0) - 90.0) < 1e-6
    assert abs(float(out["cut_output_seconds"] or 0) - 42.5) < 1e-6
    assert float(captured.get("merge_gap_sec") or 0.0) == pytest.approx(0.6)
    assert float(captured.get("min_segment_sec") or 0.0) == pytest.approx(0.05)
    assert float(captured.get("vad_threshold") or 0.0) == pytest.approx(0.95)


def test_worker_openai_trim_done(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    dbmod.create_or_requeue_job(
        conn,
        "m3",
        filename="c.mp4",
        base_url="https://x",
        job_type="openai_speech_trim",
        job_options='{"trim_method":"openai_speech","cut_merge_gap_sec":0.8,"cut_min_duration_sec":0.2}',
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
    captured: dict[str, object] = {}

    def fake_trim(*_a, **kwargs):
        captured.update(kwargs)
        return {
            "video_path": str(tmp_path / "out" / "x.mp4"),
            "input_audio_seconds": "120.000000",
            "output_video_seconds": "80.000000",
            "estimated_cost_usd": "0.012000",
        }

    monkeypatch.setattr(
        "webapp.openai_speech_trim.trim_video_to_openai_speech",
        fake_trim,
    )

    jobsmod._run_one_job(conn, settings, job_id)
    out = conn.execute(
        "SELECT status, phase, job_type, outputs_created, openai_input_seconds, openai_cost_usd, cut_input_seconds, cut_output_seconds FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    conn.close()
    assert out["status"] == "done"
    assert out["phase"] == "done"
    assert out["job_type"] == "openai_speech_trim"
    assert int(out["outputs_created"] or 0) == 1
    assert abs(float(out["openai_input_seconds"] or 0) - 120.0) < 1e-6
    assert abs(float(out["openai_cost_usd"] or 0) - 0.012) < 1e-6
    assert abs(float(out["cut_input_seconds"] or 0) - 120.0) < 1e-6
    assert abs(float(out["cut_output_seconds"] or 0) - 80.0) < 1e-6
    assert float(captured.get("merge_gap_sec") or 0.0) == pytest.approx(0.8)
    assert float(captured.get("min_segment_sec") or 0.0) == pytest.approx(0.2)


def test_get_trim_statistics_groups_methods(tmp_path: Path):
    conn = dbmod.connect(tmp_path / "stats.db")
    dbmod.prepare_database(conn)
    now = 1.0
    conn.execute(
        """
        INSERT INTO jobs (
            media_item_id, job_type, status, phase, phase_message, progress,
            trim_method_label, outputs_created, openai_cost_usd, openai_input_seconds,
            created_at, updated_at
        ) VALUES (?, ?, 'done', 'done', '', 1.0, ?, ?, ?, ?, ?, ?)
        """,
        ("m_openai", "openai_speech_trim", "openai_speech", 1, 0.006, 60.0, now, now),
    )
    conn.execute(
        """
        INSERT INTO jobs (
            media_item_id, job_type, status, phase, phase_message, progress,
            trim_method_label, outputs_created, created_at, updated_at
        ) VALUES (?, ?, 'done', 'done', '', 1.0, ?, ?, ?, ?)
        """,
        ("m_sil", "silence_remove", "silence_balanced", 2, now, now),
    )
    conn.commit()
    stats = dbmod.get_trim_statistics(conn)
    conn.close()
    assert stats["totals"]["jobs_done"] == 2
    assert stats["totals"]["outputs_created"] == 3
    assert stats["totals"]["openai_cost_usd"] == pytest.approx(0.006)
    assert stats["totals"]["openai_audio_minutes"] == pytest.approx(1.0)
    by = {r["method_key"]: r for r in stats["by_method"]}
    assert by["openai_speech"]["jobs_done"] == 1
    assert by["silence_balanced"]["outputs_created"] == 2


def test_api_stats_endpoint(tmp_path: Path, monkeypatch):
    db_path = tmp_path / "app_stats.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.execute(
        """
        INSERT INTO jobs (
            media_item_id, job_type, status, phase, phase_message, progress,
            trim_method_label, outputs_created, openai_cost_usd, openai_input_seconds,
            created_at, updated_at
        ) VALUES (?, ?, 'done', 'done', '', 1.0, 'openai_speech', 1, 0.01, 120.0, 0, 0)
        """,
        ("m1", "openai_speech_trim"),
    )
    conn.commit()
    conn.close()

    def fake_dep():
        c = dbmod.connect(db_path)
        dbmod.prepare_database(c)
        try:
            yield c
        finally:
            c.close()

    app.dependency_overrides.clear()
    from webapp.main import _db_dep

    app.dependency_overrides[_db_dep] = fake_dep
    try:
        client = TestClient(app)
        r = client.get("/api/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["totals"]["jobs_done"] == 1
        row = data["by_method"][0]
        assert row["method_key"] == "openai_speech"
        assert row["openai_usage_credits_usd"] == pytest.approx(0.01, rel=0, abs=1e-5)
    finally:
        app.dependency_overrides.clear()


def test_api_job_latest_video_endpoint(tmp_path: Path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    vid = out_dir / "demo_m_shortid123456_abc_speech_openai.mp4"
    vid.write_bytes(b"fake-mp4")

    db_path = tmp_path / "app_jobs.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.execute(
        """
        INSERT INTO jobs (
            media_item_id, job_type, status, phase, phase_message, progress,
            output_dir, created_at, updated_at
        ) VALUES (?, ?, 'done', 'done', '', 1.0, ?, 0, 0)
        """,
        ("m_shortid1234567890", "openai_speech_trim", str(out_dir)),
    )
    conn.commit()
    conn.close()

    def fake_db_dep():
        c = dbmod.connect(db_path)
        dbmod.prepare_database(c)
        try:
            yield c
        finally:
            c.close()

    s = Settings(data_dir=tmp_path, output_dir=out_dir, cache_dir=tmp_path / "cache")
    app.dependency_overrides.clear()
    from webapp.main import _db_dep, _settings_dep

    app.dependency_overrides[_db_dep] = fake_db_dep
    app.dependency_overrides[_settings_dep] = lambda: s
    try:
        client = TestClient(app)
        r = client.get("/api/jobs/1/latest-video")
        assert r.status_code == 200
        data = r.json()
        assert data["video_url"].endswith("/demo_m_shortid123456_abc_speech_openai.mp4")
    finally:
        app.dependency_overrides.clear()


def test_api_jobs_enriches_cut_metrics_with_filename_fallback(tmp_path: Path):
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    # legacy name with decimal marker "d"
    (out_dir / "demo_m_abc123456789_x_150d5s_to_81d3s_speech_openai.mp4").write_bytes(b"fake")

    db_path = tmp_path / "app_jobs_metrics.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    conn.execute(
        """
        INSERT INTO jobs (
            media_item_id, job_type, status, phase, phase_message, progress,
            output_dir, created_at, updated_at
        ) VALUES (?, ?, 'done', 'done', '', 1.0, ?, 0, 0)
        """,
        ("m_abc1234567890", "openai_speech_trim", str(out_dir)),
    )
    conn.commit()
    conn.close()

    def fake_db_dep():
        c = dbmod.connect(db_path)
        dbmod.prepare_database(c)
        try:
            yield c
        finally:
            c.close()

    s = Settings(data_dir=tmp_path, output_dir=out_dir, cache_dir=tmp_path / "cache")
    app.dependency_overrides.clear()
    from webapp.main import _db_dep, _settings_dep

    app.dependency_overrides[_db_dep] = fake_db_dep
    app.dependency_overrides[_settings_dep] = lambda: s
    try:
        client = TestClient(app)
        r = client.get("/api/jobs")
        assert r.status_code == 200
        rows = r.json()
        assert rows
        row = rows[0]
        assert row["cut_metrics_source"] == "filename"
        assert row["cut_saved_seconds"] == pytest.approx(69.2, abs=0.2)
        assert row["cut_saved_percent"] == pytest.approx(45.98, abs=0.3)
    finally:
        app.dependency_overrides.clear()
