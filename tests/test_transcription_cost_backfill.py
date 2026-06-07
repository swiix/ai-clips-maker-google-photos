from pathlib import Path

from webapp.db import backfill_transcription_job_costs, connect, create_transcription_job, init_db, update_transcription_job


def test_backfill_transcription_job_costs(tmp_path: Path):
    db = tmp_path / "app.db"
    conn = connect(db)
    init_db(conn)
    job_id = create_transcription_job(
        conn,
        filename="old.mp3",
        input_path=str(tmp_path / "old.mp3"),
        model="whisper-1",
    )
    update_transcription_job(
        conn,
        job_id,
        status="done",
        phase="done",
        progress=1.0,
        duration_seconds=120.0,
    )
    assert backfill_transcription_job_costs(conn, usd_per_minute=0.006) == 1
    row = conn.execute(
        "SELECT openai_cost_usd FROM transcription_jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    assert float(row["openai_cost_usd"]) == 0.012
    assert backfill_transcription_job_costs(conn, usd_per_minute=0.006) == 0
