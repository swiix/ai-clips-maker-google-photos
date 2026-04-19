"""
SQLite persistence for the web app.

Schema versioning
-----------------
``schema_version`` (integer string) in ``sync_state`` records the applied
migration level. Bump :data:`CURRENT_SCHEMA_VERSION` when adding a new
``_migrate_vN`` step in :func:`apply_migrations`.

Foreign keys are not enforced in SQLite (no ``PRAGMA foreign_keys``); relations
between ``jobs`` and ``tinder_reviews`` are logical only.

jobs
----
``id``, ``media_item_id`` (unique), ``job_type``, ``job_options``, ``filename``,
``base_url``, ``product_url``, ``creation_time``, ``status``, ``phase``,
``phase_message``, ``progress``, ``output_dir``, ``error``, ``created_at``,
``updated_at``, ``trim_method_label``, ``outputs_created``,
``openai_input_seconds``, ``openai_cost_usd``, ``cut_input_seconds``,
``cut_output_seconds``.

sync_state
----------
Arbitrary key/value pairs (Google sync cursor, schema version, etc.).

tinder_reviews
--------------
``clip_key`` (PK), optional ``job_id`` / ``media_item_id``, ``decision``,
``downloaded``, ``trim_mode``, ``source_filename``, ``folder``, ``video_url``,
``begin_sec``, ``finish_sec``, ``created_at``, ``updated_at``.

transcription_jobs
------------------
Standalone transcription queue rows with status and file paths.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_lock = threading.Lock()

# Stored in sync_state; increment when adding a new _migrate_vN branch.
SCHEMA_VERSION_KEY = "schema_version"
CURRENT_SCHEMA_VERSION = 1

_JOBS_COLUMN_ALIASES_V1 = (
    ("phase", "ALTER TABLE jobs ADD COLUMN phase TEXT"),
    ("phase_message", "ALTER TABLE jobs ADD COLUMN phase_message TEXT"),
    ("progress", "ALTER TABLE jobs ADD COLUMN progress REAL"),
    ("job_type", "ALTER TABLE jobs ADD COLUMN job_type TEXT NOT NULL DEFAULT 'clip_pipeline'"),
    ("job_options", "ALTER TABLE jobs ADD COLUMN job_options TEXT"),
    ("trim_method_label", "ALTER TABLE jobs ADD COLUMN trim_method_label TEXT"),
    ("outputs_created", "ALTER TABLE jobs ADD COLUMN outputs_created INTEGER"),
    ("openai_input_seconds", "ALTER TABLE jobs ADD COLUMN openai_input_seconds REAL"),
    ("openai_cost_usd", "ALTER TABLE jobs ADD COLUMN openai_cost_usd REAL"),
    ("cut_input_seconds", "ALTER TABLE jobs ADD COLUMN cut_input_seconds REAL"),
    ("cut_output_seconds", "ALTER TABLE jobs ADD COLUMN cut_output_seconds REAL"),
)

_TINDER_COLUMN_ALIASES_V1 = (
    ("job_id", "ALTER TABLE tinder_reviews ADD COLUMN job_id INTEGER"),
    ("media_item_id", "ALTER TABLE tinder_reviews ADD COLUMN media_item_id TEXT"),
)


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {r[1] for r in rows}


def _migrate_v1(conn: sqlite3.Connection) -> None:
    """
    Align legacy databases with the column set that newer clients expect.
    Idempotent: only runs ALTER for missing columns.
    """
    cols = _table_columns(conn, "jobs")
    for name, ddl in _JOBS_COLUMN_ALIASES_V1:
        if name not in cols:
            conn.execute(ddl)
            cols.add(name)

    if not _table_columns(conn, "tinder_reviews"):
        return
    tinder_cols = _table_columns(conn, "tinder_reviews")
    for name, ddl in _TINDER_COLUMN_ALIASES_V1:
        if name not in tinder_cols:
            conn.execute(ddl)
            tinder_cols.add(name)


def _get_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM sync_state WHERE key = ?", (SCHEMA_VERSION_KEY,)
    ).fetchone()
    if not row or row[0] is None:
        return 0
    try:
        return int(str(row[0]).strip())
    except ValueError:
        return 0


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        "INSERT INTO sync_state(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (SCHEMA_VERSION_KEY, str(version)),
    )


def apply_migrations(conn: sqlite3.Connection) -> None:
    """Run incremental DDL from stored ``schema_version`` to :data:`CURRENT_SCHEMA_VERSION`."""
    current = _get_schema_version(conn)
    while current < CURRENT_SCHEMA_VERSION:
        nxt = current + 1
        if nxt == 1:
            _migrate_v1(conn)
        else:
            raise RuntimeError(f"No SQLite migration implemented for schema version {nxt}")
        _set_schema_version(conn, nxt)
        current = nxt
    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    """
    Create base tables and indexes. New installations get the full ``jobs``
    definition in one statement; existing files are upgraded via
    :func:`apply_migrations`.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs (
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
            updated_at REAL,
            trim_method_label TEXT,
            outputs_created INTEGER,
            openai_input_seconds REAL,
            openai_cost_usd REAL,
            cut_input_seconds REAL,
            cut_output_seconds REAL
        );

        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS tinder_reviews (
            clip_key TEXT PRIMARY KEY,
            job_id INTEGER,
            media_item_id TEXT,
            decision TEXT,
            downloaded INTEGER NOT NULL DEFAULT 0,
            trim_mode TEXT,
            source_filename TEXT,
            folder TEXT,
            video_url TEXT,
            begin_sec REAL,
            finish_sec REAL,
            created_at REAL,
            updated_at REAL
        );

        CREATE TABLE IF NOT EXISTS transcription_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            input_path TEXT NOT NULL,
            output_txt_path TEXT,
            model TEXT NOT NULL,
            language TEXT,
            status TEXT NOT NULL,
            phase TEXT,
            progress REAL,
            duration_seconds REAL,
            error TEXT,
            created_at REAL,
            updated_at REAL
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        CREATE INDEX IF NOT EXISTS idx_tinder_reviews_job_id ON tinder_reviews(job_id);
        CREATE INDEX IF NOT EXISTS idx_tinder_reviews_media_item_id ON tinder_reviews(media_item_id);
        CREATE INDEX IF NOT EXISTS idx_tinder_reviews_updated_at ON tinder_reviews(updated_at);
        CREATE INDEX IF NOT EXISTS idx_transcription_jobs_status ON transcription_jobs(status);
        CREATE INDEX IF NOT EXISTS idx_transcription_jobs_updated_at ON transcription_jobs(updated_at);
        """
    )
    conn.commit()


def prepare_database(conn: sqlite3.Connection) -> None:
    """Create schema and apply migrations (use this at process startup / per-connection warm-up)."""
    init_db(conn)
    apply_migrations(conn)


def create_transcription_job(
    conn: sqlite3.Connection,
    *,
    filename: str,
    input_path: str,
    model: str,
    language: str | None = None,
) -> int:
    now = time.time()
    with _lock:
        conn.execute(
            """
            INSERT INTO transcription_jobs (
                filename, input_path, output_txt_path, model, language,
                status, phase, progress, duration_seconds, error, created_at, updated_at
            ) VALUES (?, ?, NULL, ?, ?, 'queued', 'queued', 0.0, NULL, NULL, ?, ?)
            """,
            (
                filename,
                input_path,
                model,
                language,
                now,
                now,
            ),
        )
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid()").fetchone()
    return int(row[0])


def update_transcription_job(
    conn: sqlite3.Connection,
    job_id: int,
    *,
    status: str | None = None,
    phase: str | None = None,
    progress: float | None = None,
    duration_seconds: float | None = None,
    error: str | None = None,
    output_txt_path: str | None = None,
) -> None:
    now = time.time()
    with _lock:
        conn.execute(
            """
            UPDATE transcription_jobs
            SET
                status = COALESCE(?, status),
                phase = COALESCE(?, phase),
                progress = COALESCE(?, progress),
                duration_seconds = COALESCE(?, duration_seconds),
                error = ?,
                output_txt_path = COALESCE(?, output_txt_path),
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                phase,
                progress,
                duration_seconds,
                error,
                output_txt_path,
                now,
                job_id,
            ),
        )
        conn.commit()


def get_transcription_job(conn: sqlite3.Connection, job_id: int) -> dict[str, Any] | None:
    with _lock:
        row = conn.execute(
            """
            SELECT id, filename, input_path, output_txt_path, model, language, status,
                   phase, progress, duration_seconds, error, created_at, updated_at
            FROM transcription_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
    if not row:
        return None
    return dict(row)


def list_transcription_jobs(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    with _lock:
        rows = conn.execute(
            """
            SELECT id, filename, input_path, output_txt_path, model, language, status,
                   phase, progress, duration_seconds, error, created_at, updated_at
            FROM transcription_jobs
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def create_or_requeue_job(
    conn: sqlite3.Connection,
    media_item_id: str,
    *,
    filename: str | None = None,
    base_url: str | None = None,
    product_url: str | None = None,
    creation_time: str | None = None,
    job_type: str = "clip_pipeline",
    job_options: str | None = None,
    trim_method_label: str | None = None,
) -> tuple[int, bool]:
    """
    Returns (job_id, enqueue_worker) — enqueue False only if already queued/running.
    """
    now = time.time()
    with _lock:
        row = conn.execute(
            "SELECT id, status FROM jobs WHERE media_item_id = ?", (media_item_id,)
        ).fetchone()
        if row:
            st = row["status"]
            if st in ("queued", "running"):
                return int(row["id"]), False
            conn.execute(
                """
                UPDATE jobs SET
                    filename = ?, base_url = ?, product_url = ?, creation_time = ?,
                    job_type = ?,
                    job_options = ?,
                    trim_method_label = ?,
                    outputs_created = NULL,
                    openai_input_seconds = NULL,
                    openai_cost_usd = NULL,
                    cut_input_seconds = NULL,
                    cut_output_seconds = NULL,
                    status = 'queued', phase = 'queued', phase_message = 'In Warteschlange',
                    progress = 0.0, error = NULL, output_dir = NULL, updated_at = ?
                WHERE id = ?
                """,
                (
                    filename,
                    base_url,
                    product_url,
                    creation_time,
                    job_type,
                    job_options,
                    trim_method_label,
                    now,
                    row["id"],
                ),
            )
            conn.commit()
            return int(row["id"]), True

        conn.execute(
            """
            INSERT INTO jobs (
                media_item_id, filename, base_url, product_url, creation_time,
                job_type, job_options, trim_method_label, status, phase, phase_message, progress, output_dir, error, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', 'queued', 'In Warteschlange', 0.0, NULL, NULL, ?, ?)
            """,
            (
                media_item_id,
                filename,
                base_url,
                product_url,
                creation_time,
                job_type,
                job_options,
                trim_method_label,
                now,
                now,
            ),
        )
        conn.commit()
        jid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    return jid, True


def upsert_job(
    conn: sqlite3.Connection,
    media_item_id: str,
    *,
    filename: str | None = None,
    base_url: str | None = None,
    product_url: str | None = None,
    creation_time: str | None = None,
    status: str = "queued",
    phase: str | None = None,
    phase_message: str | None = None,
    progress: float | None = None,
    output_dir: str | None = None,
    error: str | None = None,
    job_type: str | None = None,
) -> None:
    now = time.time()
    with _lock:
        row = conn.execute(
            "SELECT id FROM jobs WHERE media_item_id = ?", (media_item_id,)
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE jobs SET
                    filename = COALESCE(?, filename),
                    base_url = COALESCE(?, base_url),
                    product_url = COALESCE(?, product_url),
                    creation_time = COALESCE(?, creation_time),
                    job_type = COALESCE(?, job_type),
                    status = ?,
                    phase = COALESCE(?, phase),
                    phase_message = COALESCE(?, phase_message),
                    progress = COALESCE(?, progress),
                    output_dir = COALESCE(?, output_dir),
                    error = ?,
                    updated_at = ?
                WHERE media_item_id = ?
                """,
                (
                    filename,
                    base_url,
                    product_url,
                    creation_time,
                    job_type,
                    status,
                    phase,
                    phase_message,
                    progress,
                    output_dir,
                    error,
                    now,
                    media_item_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO jobs (
                    media_item_id, filename, base_url, product_url, creation_time,
                    job_type, status, phase, phase_message, progress, output_dir, error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    media_item_id,
                    filename,
                    base_url,
                    product_url,
                    creation_time,
                    job_type or "clip_pipeline",
                    status,
                    phase,
                    phase_message,
                    progress,
                    output_dir,
                    error,
                    now,
                    now,
                ),
            )
        conn.commit()


def set_job_run_metrics(
    conn: sqlite3.Connection,
    media_item_id: str,
    *,
    outputs_created: int,
    openai_input_seconds: float | None = None,
    openai_cost_usd: float | None = None,
    cut_input_seconds: float | None = None,
    cut_output_seconds: float | None = None,
) -> None:
    now = time.time()
    with _lock:
        conn.execute(
            """
            UPDATE jobs SET
                outputs_created = ?,
                openai_input_seconds = ?,
                openai_cost_usd = ?,
                cut_input_seconds = ?,
                cut_output_seconds = ?,
                updated_at = ?
            WHERE media_item_id = ?
            """,
            (
                outputs_created,
                openai_input_seconds,
                openai_cost_usd,
                cut_input_seconds,
                cut_output_seconds,
                now,
                media_item_id,
            ),
        )
        conn.commit()


def _resolve_method_key(
    trim_method_label: str | None, job_options: str | None, job_type: str | None
) -> str:
    if trim_method_label:
        return str(trim_method_label)
    try:
        o = json.loads(job_options or "{}")
        tm = str(o.get("trim_method") or "").strip()
        if tm:
            return tm
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    jt = str(job_type or "")
    if jt == "openai_speech_trim":
        return "openai_speech"
    if jt == "silence_remove":
        return "silence_unknown"
    if jt == "clip_pipeline":
        return "clip_pipeline_ai"
    return jt or "unknown"


def get_trim_statistics(conn: sqlite3.Connection) -> dict[str, Any]:
    with _lock:
        rows = conn.execute(
            """
            SELECT job_type, job_options, trim_method_label, outputs_created,
                   openai_cost_usd, openai_input_seconds
            FROM jobs
            WHERE status = 'done'
            """
        ).fetchall()
    by_method: dict[str, dict[str, Any]] = {}
    totals: dict[str, Any] = {
        "jobs_done": 0,
        "outputs_created": 0,
        "openai_cost_usd": 0.0,
        "openai_audio_minutes": 0.0,
    }
    for r in rows:
        key = _resolve_method_key(r["trim_method_label"], r["job_options"], r["job_type"])
        oc = int(r["outputs_created"] or 0)
        cost = float(r["openai_cost_usd"] or 0.0)
        secs = float(r["openai_input_seconds"] or 0.0)
        minutes = secs / 60.0
        if key not in by_method:
            by_method[key] = {
                "method_key": key,
                "jobs_done": 0,
                "outputs_created": 0,
                "openai_cost_usd": 0.0,
                "openai_audio_minutes": 0.0,
            }
        m = by_method[key]
        m["jobs_done"] += 1
        m["outputs_created"] += oc
        m["openai_cost_usd"] += cost
        m["openai_audio_minutes"] += minutes
        totals["jobs_done"] += 1
        totals["outputs_created"] += oc
        totals["openai_cost_usd"] += cost
        totals["openai_audio_minutes"] += minutes
    ordered = sorted(by_method.values(), key=lambda x: (x["method_key"]))
    return {"by_method": ordered, "totals": totals}


def list_jobs(conn: sqlite3.Connection, limit: int = 200) -> list[dict[str, Any]]:
    with _lock:
        rows = conn.execute(
            """
            SELECT
                j.*,
                COALESCE(rv_by_job.review_state, rv_by_media.review_state, 'none') AS review_state
            FROM jobs j
            LEFT JOIN (
                SELECT
                    job_id,
                    CASE
                        WHEN SUM(CASE WHEN decision = 'like' THEN 1 ELSE 0 END) > 0 THEN 'liked'
                        WHEN SUM(CASE WHEN decision = 'dislike' THEN 1 ELSE 0 END) > 0 THEN 'skipped'
                        ELSE 'none'
                    END AS review_state
                FROM tinder_reviews
                WHERE job_id IS NOT NULL
                GROUP BY job_id
            ) rv_by_job ON rv_by_job.job_id = j.id
            LEFT JOIN (
                SELECT
                    media_item_id,
                    CASE
                        WHEN SUM(CASE WHEN decision = 'like' THEN 1 ELSE 0 END) > 0 THEN 'liked'
                        WHEN SUM(CASE WHEN decision = 'dislike' THEN 1 ELSE 0 END) > 0 THEN 'skipped'
                        ELSE 'none'
                    END AS review_state
                FROM tinder_reviews
                WHERE media_item_id IS NOT NULL AND media_item_id != ''
                GROUP BY media_item_id
            ) rv_by_media ON rv_by_media.media_item_id = j.media_item_id
            ORDER BY j.updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_sync_value(conn: sqlite3.Connection, key: str) -> str | None:
    with _lock:
        row = conn.execute(
            "SELECT value FROM sync_state WHERE key = ?", (key,)
        ).fetchone()
    return row["value"] if row else None


def set_sync_value(conn: sqlite3.Connection, key: str, value: str) -> None:
    with _lock:
        conn.execute(
            "INSERT INTO sync_state(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()


def upsert_tinder_review(
    conn: sqlite3.Connection,
    *,
    clip_key: str,
    job_id: int | None = None,
    media_item_id: str | None = None,
    decision: str | None = None,
    downloaded: bool | None = None,
    trim_mode: str | None = None,
    source_filename: str | None = None,
    folder: str | None = None,
    video_url: str | None = None,
    begin_sec: float | None = None,
    finish_sec: float | None = None,
) -> None:
    now = time.time()
    decision_norm: str | None = None
    if decision is not None:
        raw = str(decision).strip().lower()
        if raw in {"like", "dislike", "none"}:
            decision_norm = raw
    downloaded_norm: int | None = None
    if downloaded is not None:
        downloaded_norm = 1 if bool(downloaded) else 0
    with _lock:
        row = conn.execute(
            "SELECT clip_key FROM tinder_reviews WHERE clip_key = ?",
            (clip_key,),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE tinder_reviews SET
                    job_id = COALESCE(?, job_id),
                    media_item_id = COALESCE(?, media_item_id),
                    decision = COALESCE(?, decision),
                    downloaded = COALESCE(?, downloaded),
                    trim_mode = COALESCE(?, trim_mode),
                    source_filename = COALESCE(?, source_filename),
                    folder = COALESCE(?, folder),
                    video_url = COALESCE(?, video_url),
                    begin_sec = COALESCE(?, begin_sec),
                    finish_sec = COALESCE(?, finish_sec),
                    updated_at = ?
                WHERE clip_key = ?
                """,
                (
                    job_id,
                    media_item_id,
                    decision_norm,
                    downloaded_norm,
                    trim_mode,
                    source_filename,
                    folder,
                    video_url,
                    begin_sec,
                    finish_sec,
                    now,
                    clip_key,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO tinder_reviews (
                    clip_key, job_id, media_item_id, decision, downloaded, trim_mode, source_filename, folder, video_url,
                    begin_sec, finish_sec, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    clip_key,
                    job_id,
                    media_item_id,
                    decision_norm,
                    downloaded_norm if downloaded_norm is not None else 0,
                    trim_mode,
                    source_filename,
                    folder,
                    video_url,
                    begin_sec,
                    finish_sec,
                    now,
                    now,
                ),
            )
        conn.commit()


def list_tinder_reviews(conn: sqlite3.Connection, limit: int = 5000) -> list[dict[str, Any]]:
    with _lock:
        rows = conn.execute(
            """
            SELECT clip_key, job_id, media_item_id, decision, downloaded, trim_mode, source_filename, folder, video_url,
                   begin_sec, finish_sec, created_at, updated_at
            FROM tinder_reviews
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]
