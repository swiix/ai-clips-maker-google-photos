from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_lock = threading.Lock()


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
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
            updated_at REAL
        );

        CREATE TABLE IF NOT EXISTS sync_state (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
        """
    )
    cols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)").fetchall()}
    if "phase" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN phase TEXT")
    if "phase_message" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN phase_message TEXT")
    if "progress" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN progress REAL")
    if "job_type" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN job_type TEXT NOT NULL DEFAULT 'clip_pipeline'")
    if "job_options" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN job_options TEXT")
    if "trim_method_label" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN trim_method_label TEXT")
    if "outputs_created" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN outputs_created INTEGER")
    if "openai_input_seconds" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN openai_input_seconds REAL")
    if "openai_cost_usd" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN openai_cost_usd REAL")
    if "cut_input_seconds" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN cut_input_seconds REAL")
    if "cut_output_seconds" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN cut_output_seconds REAL")
    conn.commit()


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
    Returns (job_id, enqueue_worker) — enqueue False if already queued/running/done.
    """
    now = time.time()
    with _lock:
        row = conn.execute(
            "SELECT id, status FROM jobs WHERE media_item_id = ?", (media_item_id,)
        ).fetchone()
        if row:
            st = row["status"]
            if st in ("queued", "running", "done"):
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
            "SELECT * FROM jobs ORDER BY updated_at DESC LIMIT ?", (limit,)
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
