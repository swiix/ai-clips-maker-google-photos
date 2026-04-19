from __future__ import annotations

import sqlite3
from pathlib import Path

from webapp import db as dbmod


def _job_columns(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(jobs)").fetchall()
    return {r[1] for r in rows}


EXPECTED_JOBS_COLUMNS = {
    "id",
    "media_item_id",
    "job_type",
    "job_options",
    "filename",
    "base_url",
    "product_url",
    "creation_time",
    "status",
    "phase",
    "phase_message",
    "progress",
    "output_dir",
    "error",
    "created_at",
    "updated_at",
    "trim_method_label",
    "outputs_created",
    "openai_input_seconds",
    "openai_cost_usd",
    "cut_input_seconds",
    "cut_output_seconds",
}


def test_prepare_database_fresh_file_has_expected_jobs_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    assert _job_columns(conn) == EXPECTED_JOBS_COLUMNS
    row = conn.execute(
        "SELECT value FROM sync_state WHERE key = ?",
        (dbmod.SCHEMA_VERSION_KEY,),
    ).fetchone()
    conn.close()
    assert row is not None
    assert int(row[0]) == dbmod.CURRENT_SCHEMA_VERSION


def test_migration_v1_upgrades_legacy_minimal_jobs_table(tmp_path: Path) -> None:
    """
    Simulates an older DB that had the main job row shape but not the later
    trim / metrics columns added via ALTER in pre-versioned ``init_db``.
    """
    db_path = tmp_path / "legacy.db"
    raw = sqlite3.connect(db_path)
    raw.execute(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            media_item_id TEXT NOT NULL UNIQUE,
            job_type TEXT NOT NULL DEFAULT 'clip_pipeline',
            job_options TEXT,
            filename TEXT,
            base_url TEXT,
            product_url TEXT,
            creation_time TEXT,
            status TEXT NOT NULL,
            phase TEXT,
            phase_message TEXT,
            progress REAL,
            output_dir TEXT,
            error TEXT,
            created_at REAL,
            updated_at REAL
        )
        """
    )
    raw.execute(
        """
        CREATE TABLE sync_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    raw.commit()
    raw.close()

    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)
    dbmod.apply_migrations(conn)
    cols = _job_columns(conn)
    conn.close()
    assert "trim_method_label" in cols
    assert "openai_cost_usd" in cols
    assert cols == EXPECTED_JOBS_COLUMNS


def test_apply_migrations_is_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "app.db"
    conn = dbmod.connect(db_path)
    dbmod.prepare_database(conn)
    dbmod.apply_migrations(conn)
    dbmod.apply_migrations(conn)
    v = conn.execute(
        "SELECT value FROM sync_state WHERE key = ?",
        (dbmod.SCHEMA_VERSION_KEY,),
    ).fetchone()
    conn.close()
    assert v is not None and int(v[0]) == dbmod.CURRENT_SCHEMA_VERSION
