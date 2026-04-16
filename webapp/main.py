"""
Local FastAPI entrypoint: Google Photos OAuth, media listing, clip jobs, gallery.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import shutil
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any, Optional, List

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import httpx
from pydantic import BaseModel, Field

from webapp import db as dbmod
from webapp import jobs as jobsmod
from webapp.db import connect, init_db
from webapp.google_photos import (
    build_oauth_flow,
    ensure_fresh_credentials,
    picker_create_session,
    picker_get_session,
    picker_list_media_items,
    save_credentials,
)
from webapp.logging_setup import configure_logging, install_global_exception_hooks
from webapp.settings import Settings, get_settings

logger = logging.getLogger(__name__)

# Google may return a superset of requested scopes (e.g. previously granted scopes).
# Avoid treating that as a hard failure during token exchange callback.
os.environ.setdefault("OAUTHLIB_RELAX_TOKEN_SCOPE", "1")
# PyTorch >=2.6 changed default torch.load(weights_only=True); pyannote checkpoints
# still require full object loading in this trusted local app context.
os.environ.setdefault("TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD", "1")


def _ensure_ffmpeg_on_path() -> None:
    """
    Ensure a binary named ``ffmpeg`` is on PATH.

    imageio-ffmpeg ships a binary with a versioned filename, not ``ffmpeg``;
    we add a small directory with a symlink so MediaEditor's ``ffmpeg`` calls work.
    """
    if shutil.which("ffmpeg"):
        return
    try:
        import imageio_ffmpeg

        real = Path(imageio_ffmpeg.get_ffmpeg_exe()).resolve()
        if not real.is_file():
            raise FileNotFoundError(str(real))
        bindir = Path(tempfile.gettempdir()) / "ai_clips_maker_ffmpeg_bin"
        bindir.mkdir(parents=True, exist_ok=True)
        link = bindir / "ffmpeg"
        if link.exists() or link.is_symlink():
            link.unlink(missing_ok=True)
        try:
            link.symlink_to(real)
        except OSError:
            shutil.copy2(real, link)
            os.chmod(link, 0o755)
        os.environ["PATH"] = str(bindir) + os.pathsep + os.environ.get("PATH", "")
        logger.info("ffmpeg on PATH -> %s (from %s)", link, real)
    except Exception as exc:
        logger.warning(
            "ffmpeg not found on PATH and imageio-ffmpeg setup failed (%s). "
            "Install ffmpeg (e.g. brew install ffmpeg) for video jobs.",
            exc,
        )


_ensure_ffmpeg_on_path()

_oauth_states: dict[str, float] = {}
_scheduler: BackgroundScheduler | None = None
_preflight_cache: dict[str, Any] = {"ts": 0.0, "result": None}


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(
        log_dir=settings.log_dir,
        log_level=settings.log_level,
        log_max_mb=settings.log_max_mb,
        log_backup_count=settings.log_backup_count,
    )
    install_global_exception_hooks(logger)
    logger.info("Logging initialized (dir=%s, level=%s)", settings.log_dir, settings.log_level)
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.output_dir.mkdir(parents=True, exist_ok=True)
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    conn = connect(settings.sqlite_path)
    init_db(conn)
    conn.close()

    def conn_factory():
        c = connect(settings.sqlite_path)
        init_db(c)
        return c

    jobsmod.start_worker(conn_factory, settings)
    qc = conn_factory()
    try:
        stale = jobsmod.mark_stale_running_jobs_failed(qc)
        if stale:
            logger.warning("Marked %s stale running job(s) as failed", stale)
        jobsmod.queue_pending_on_startup(qc)
    finally:
        qc.close()

    global _scheduler
    if settings.scheduler_interval_minutes > 0:
        _scheduler = BackgroundScheduler()

        def tick() -> None:
            try:
                _scheduled_poll(settings)
            except Exception:
                logger.exception("Scheduled poll failed")

        _scheduler.add_job(
            tick,
            "interval",
            minutes=settings.scheduler_interval_minutes,
            id="photos_poll",
            replace_existing=True,
        )
        _scheduler.start()

    yield

    if _scheduler:
        _scheduler.shutdown(wait=False)
    jobsmod.stop_worker()


app = FastAPI(title="ai-clips-maker Google Photos", lifespan=lifespan)

static_dir = Path(__file__).resolve().parent / "static"
if static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        logger.exception(
            "HTTP %s %s failed after %.1fms",
            request.method,
            request.url.path,
            elapsed_ms,
        )
        raise
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    logger.info(
        "HTTP %s %s -> %s (%.1fms)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


def _settings_dep() -> Settings:
    return get_settings()


def _db_dep(settings: Annotated[Settings, Depends(_settings_dep)]):
    conn = connect(settings.sqlite_path)
    init_db(conn)
    try:
        yield conn
    finally:
        conn.close()


SettingsDep = Annotated[Settings, Depends(_settings_dep)]
DbDep = Annotated[Any, Depends(_db_dep)]


def _scheduled_poll(settings: Settings) -> None:
    # Picker API is user-driven; no global library polling is available.
    if settings.auto_enqueue_new_videos:
        logger.warning(
            "AUTO_ENQUEUE_NEW_VIDEOS is enabled, but Picker API requires "
            "interactive user selection. Scheduler auto-poll is skipped."
        )


def _cache_target_path(settings: Settings, media_item_id: str, filename: str | None) -> Path:
    ext = Path(filename or "video.mp4").suffix or ".mp4"
    safe_id = "".join(ch for ch in media_item_id if ch.isalnum() or ch in ("-", "_"))
    if not safe_id:
        safe_id = "media"
    return settings.cache_dir / f"{safe_id}{ext}"


class MediaItemIn(BaseModel):
    id: str
    baseUrl: str = Field(alias="baseUrl")
    filename: Optional[str] = None
    productUrl: Optional[str] = Field(default=None, alias="productUrl")
    creationTime: Optional[str] = Field(default=None, alias="creationTime")
    processingStatus: Optional[str] = Field(default=None, alias="processingStatus")

    model_config = {"populate_by_name": True}


class EnqueueBody(BaseModel):
    items: list[MediaItemIn]
    profiles: Optional[List[str]] = None
    trim_method: Optional[str] = None


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    index_path = static_dir / "index.html"
    if index_path.is_file():
        return HTMLResponse(
            index_path.read_text(encoding="utf-8"),
            headers={
                "Cache-Control": "no-store, max-age=0",
                "Pragma": "no-cache",
            },
        )
    return HTMLResponse("<p>Missing static/index.html</p>")


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/auth/status")
def auth_status(settings: SettingsDep) -> dict[str, bool]:
    return {"connected": ensure_fresh_credentials(settings) is not None}


@app.get("/auth/google")
def auth_google(settings: SettingsDep) -> RedirectResponse:
    try:
        flow = build_oauth_flow(settings)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    state = secrets.token_urlsafe(24)
    _oauth_states[state] = time.time()
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=state,
    )
    return RedirectResponse(auth_url)


@app.get("/auth/callback")
def auth_callback(
    settings: SettingsDep,
    code: str = Query(...),
    state: str = Query(...),
) -> RedirectResponse:
    if state not in _oauth_states:
        raise HTTPException(400, "Invalid OAuth state")
    del _oauth_states[state]
    flow = build_oauth_flow(settings)
    flow.fetch_token(code=code)
    save_credentials(settings, flow.credentials)
    return RedirectResponse("/")


@app.get("/api/media/videos")
def list_videos(
    settings: SettingsDep,
    page_token: Optional[str] = None,
) -> dict[str, Any]:
    # Legacy endpoint kept for compatibility with older frontend calls.
    raise HTTPException(
        410,
        "Legacy library listing is removed. Use Picker flow: "
        "/api/picker/session + /api/picker/media.",
    )


@app.post("/api/picker/session")
def picker_session_create(settings: SettingsDep, conn: DbDep) -> dict[str, Any]:
    creds = ensure_fresh_credentials(settings)
    if creds is None:
        raise HTTPException(401, "Not connected to Google Photos")
    try:
        session = picker_create_session(creds.token)
        sid = session.get("id")
        if sid:
            dbmod.set_sync_value(conn, "last_picker_session_id", sid)
        return session
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        try:
            payload = exc.response.json()
            detail = payload.get("error", {}).get("message", detail)
        except Exception:
            pass
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Google Photos Picker error: {detail}",
        ) from exc


@app.get("/api/picker/last-session")
def picker_last_session(conn: DbDep) -> dict[str, Any]:
    return {"sessionId": dbmod.get_sync_value(conn, "last_picker_session_id")}


@app.get("/api/picker/session/{session_id}")
def picker_session_get(session_id: str, settings: SettingsDep) -> dict[str, Any]:
    creds = ensure_fresh_credentials(settings)
    if creds is None:
        raise HTTPException(401, "Not connected to Google Photos")
    try:
        return picker_get_session(creds.token, session_id)
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        try:
            payload = exc.response.json()
            detail = payload.get("error", {}).get("message", detail)
        except Exception:
            pass
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Google Photos Picker error: {detail}",
        ) from exc


@app.get("/api/picker/media")
def picker_media_list(
    settings: SettingsDep,
    session_id: str = Query(...),
    page_token: Optional[str] = None,
) -> dict[str, Any]:
    creds = ensure_fresh_credentials(settings)
    if creds is None:
        raise HTTPException(401, "Not connected to Google Photos")
    try:
        payload = picker_list_media_items(
            creds.token,
            session_id=session_id,
            page_size=settings.photos_page_size,
            page_token=page_token or None,
        )
        media_items = payload.get("mediaItems") or []
        filtered = []
        for it in media_items:
            media_file = it.get("mediaFile") or {}
            mime = str(media_file.get("mimeType") or it.get("mimeType") or "")
            if mime.lower().startswith("video/"):
                filtered.append(it)
        payload["mediaItems"] = filtered
        return payload
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text
        try:
            payload = exc.response.json()
            detail = payload.get("error", {}).get("message", detail)
        except Exception:
            pass
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Google Photos Picker error: {detail}",
        ) from exc


@app.get("/api/picker/proxy")
async def picker_media_proxy(
    settings: SettingsDep,
    base_url: str = Query(...),
    kind: str = Query("thumb"),
    range_header: Optional[str] = Header(default=None, alias="Range"),
):
    creds = ensure_fresh_credentials(settings)
    if creds is None or not creds.token:
        raise HTTPException(401, "Not connected to Google Photos")

    if kind not in {"thumb", "video"}:
        raise HTTPException(400, "Invalid proxy kind")

    candidates: list[str]
    if kind == "thumb":
        candidates = [f"{base_url}=w640-h360-c", base_url]
    else:
        if "video-downloads.googleusercontent.com" in base_url or "=dv" in base_url:
            candidates = [base_url]
        else:
            candidates = [f"{base_url}=dv", base_url]

    req_headers = {"Authorization": f"Bearer {creds.token}"}
    if range_header:
        req_headers["Range"] = range_header

    for url in candidates:
        async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=req_headers)
        if resp.status_code >= 400:
            continue

        out_headers = {}
        for hk in ("content-type", "content-length", "accept-ranges", "content-range", "cache-control"):
            hv = resp.headers.get(hk)
            if hv:
                out_headers[hk] = hv

        return StreamingResponse(
            iter([resp.content]),
            status_code=resp.status_code,
            headers=out_headers,
            media_type=resp.headers.get("content-type"),
        )

    raise HTTPException(404, "Media preview not available")


@app.get("/api/cache/video")
def cached_video(
    settings: SettingsDep,
    media_item_id: str = Query(...),
    base_url: str = Query(...),
    filename: Optional[str] = Query(default=None),
) -> FileResponse:
    """
    Return a local file for preview/playback.
    If missing, download once to local cache first.
    """
    cache_path = _cache_target_path(settings, media_item_id, filename)
    if cache_path.is_file() and cache_path.stat().st_size > 0:
        return FileResponse(cache_path)

    creds = ensure_fresh_credentials(settings)
    if creds is None or not creds.token:
        raise HTTPException(401, "Not connected to Google Photos")

    from webapp.google_photos import download_media_base_url

    try:
        download_media_base_url(base_url, cache_path, access_token=creds.token)
    except Exception as exc:
        raise HTTPException(
            409,
            f"Video ist noch nicht lokal verfuegbar: {exc}",
        ) from exc

    if not cache_path.is_file() or cache_path.stat().st_size <= 0:
        raise HTTPException(500, "Cached video file missing after download")
    return FileResponse(cache_path)


@app.get("/api/cache/status")
def cached_video_status(
    settings: SettingsDep,
    media_item_id: str = Query(...),
    filename: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    cache_path = _cache_target_path(settings, media_item_id, filename)
    if not cache_path.is_file():
        return {"ready": False, "size_bytes": 0}
    size = cache_path.stat().st_size
    return {"ready": size > 0, "size_bytes": size}


@app.post("/api/cache/clear")
def clear_video_cache(settings: SettingsDep) -> dict[str, Any]:
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    removed = 0
    failed = 0
    for p in settings.cache_dir.iterdir():
        if not p.is_file():
            continue
        try:
            p.unlink()
            removed += 1
        except Exception:
            failed += 1
    return {"removed_files": removed, "failed_files": failed}


_STATS_METHOD_LABELS_DE: dict[str, str] = {
    "openai_speech": "OpenAI (Whisper · gesprochene Segmente)",
    "silence_all": "Stille entfernen · alle Profile",
    "silence_conservative": "Stille entfernen · Conservative",
    "silence_balanced": "Stille entfernen · Balanced",
    "silence_aggressive": "Stille entfernen · Aggressive",
    "silence_unknown": "Stille entfernen (älterer Eintrag)",
    "clip_pipeline_ai": "KI-Clip-Pipeline (Diarize + Clips)",
}


@app.post("/api/jobs")
def enqueue_jobs(body: EnqueueBody, conn: DbDep) -> dict[str, Any]:
    queued = []
    skipped = []
    for it in body.items:
        if not it.id or not it.baseUrl:
            skipped.append(it.id or "<missing-id>")
            continue
        jid, enq = dbmod.create_or_requeue_job(
            conn,
            it.id,
            filename=it.filename,
            base_url=it.baseUrl,
            product_url=it.productUrl,
            creation_time=it.creationTime,
            job_type="clip_pipeline",
            trim_method_label="clip_pipeline_ai",
        )
        if enq:
            jobsmod.enqueue_job_id(jid)
            queued.append(jid)
        else:
            skipped.append(it.id)
    return {"queued_job_ids": queued, "skipped_media_ids": skipped}


def _trim_job_type_and_options(body: EnqueueBody) -> tuple[str, str]:
    """
    Map UI trim_method (and legacy profiles) to job_type + job_options JSON.
    """
    raw = (body.trim_method or "").strip().lower()
    valid_silence = {
        "silence_all",
        "silence_conservative",
        "silence_balanced",
        "silence_aggressive",
    }
    if raw == "openai_speech":
        return "openai_speech_trim", json.dumps({"trim_method": "openai_speech"}, ensure_ascii=True)
    if raw in valid_silence:
        return "silence_remove", json.dumps({"trim_method": raw}, ensure_ascii=True)
    profs = [p for p in (body.profiles or []) if p in {"conservative", "balanced", "aggressive"}]
    if len(profs) >= 3:
        return "silence_remove", json.dumps(
            {"trim_method": "silence_all", "profiles": profs[:3]}, ensure_ascii=True
        )
    if len(profs) == 1:
        return "silence_remove", json.dumps(
            {"trim_method": f"silence_{profs[0]}", "profiles": profs}, ensure_ascii=True
        )
    if profs:
        return "silence_remove", json.dumps(
            {"trim_method": "silence_balanced", "profiles": profs}, ensure_ascii=True
        )
    return "silence_remove", json.dumps({"trim_method": "silence_balanced"}, ensure_ascii=True)


def _trim_method_label_for_enqueue(body: EnqueueBody) -> str:
    _job_type, options_json = _trim_job_type_and_options(body)
    try:
        o = json.loads(options_json)
        tm = str(o.get("trim_method") or "").strip()
        if tm:
            return tm
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    if _job_type == "openai_speech_trim":
        return "openai_speech"
    return "silence_balanced"


@app.post("/api/jobs/silence-remove")
def enqueue_silence_remove_jobs(body: EnqueueBody, conn: DbDep) -> dict[str, Any]:
    queued = []
    skipped = []
    job_type, options_json = _trim_job_type_and_options(body)
    method_label = _trim_method_label_for_enqueue(body)
    for it in body.items:
        if not it.id or not it.baseUrl:
            skipped.append(it.id or "<missing-id>")
            continue
        jid, enq = dbmod.create_or_requeue_job(
            conn,
            it.id,
            filename=it.filename,
            base_url=it.baseUrl,
            product_url=it.productUrl,
            creation_time=it.creationTime,
            job_type=job_type,
            job_options=options_json,
            trim_method_label=method_label,
        )
        if enq:
            jobsmod.enqueue_job_id(jid)
            queued.append(jid)
        else:
            skipped.append(it.id)
    return {"queued_job_ids": queued, "skipped_media_ids": skipped}


@app.get("/api/preflight")
def preflight(settings: SettingsDep) -> dict[str, Any]:
    """
    Runtime checks before enqueueing long-running jobs.
    """
    now = time.time()
    cached = _preflight_cache.get("result")
    if cached is not None and (now - float(_preflight_cache.get("ts") or 0.0)) < 300:
        return cached

    checks: list[dict[str, Any]] = []

    ffmpeg_ok = shutil.which("ffmpeg") is not None
    checks.append(
        {
            "name": "ffmpeg",
            "ok": ffmpeg_ok,
            "detail": "ffmpeg found on PATH" if ffmpeg_ok else "ffmpeg missing on PATH",
        }
    )

    token = settings.pyannote_token or os.environ.get("HF_TOKEN", "")
    token_ok = bool(token)
    checks.append(
        {
            "name": "pyannote_token",
            "ok": token_ok,
            "detail": "Token available" if token_ok else "Missing PYANNOTE_TOKEN/HF_TOKEN",
        }
    )

    pyannote_ok = False
    pyannote_detail = "Skipped because token is missing"
    if token_ok:
        try:
            from pyannote.audio import Pipeline

            p = Pipeline.from_pretrained(
                "pyannote/speaker-diarization-3.1",
                use_auth_token=token,
            )
            pyannote_ok = p is not None
            pyannote_detail = (
                "Pipeline loaded successfully"
                if pyannote_ok
                else "Pipeline returned None (check HF model access/gating)"
            )
        except Exception as exc:
            pyannote_ok = False
            pyannote_detail = str(exc)
            logger.exception("Preflight pyannote check failed")
    checks.append(
        {
            "name": "pyannote_pipeline",
            "ok": pyannote_ok,
            "detail": pyannote_detail,
        }
    )

    ok = all(bool(c.get("ok")) for c in checks)
    result = {
        "ok": ok,
        "checks": checks,
        "message": "Preflight successful" if ok else "Preflight failed",
    }
    _preflight_cache["ts"] = now
    _preflight_cache["result"] = result
    return result


@app.get("/api/jobs")
def list_jobs(conn: DbDep) -> list[dict[str, Any]]:
    return dbmod.list_jobs(conn)


@app.get("/api/jobs/{job_id}/latest-video")
def job_latest_video(job_id: int, conn: DbDep, settings: SettingsDep) -> dict[str, Any]:
    row = conn.execute(
        "SELECT id, media_item_id, status, output_dir FROM jobs WHERE id = ?",
        (job_id,),
    ).fetchone()
    if not row:
        raise HTTPException(404, "Job not found")
    if str(row["status"] or "") != "done":
        raise HTTPException(409, "Job is not finished yet")
    out_dir = Path(str(row["output_dir"] or "")).expanduser().resolve()
    if not out_dir.is_dir():
        raise HTTPException(404, "Output folder missing")

    short_id = str(row["media_item_id"] or "")[:12]
    candidates = sorted(
        out_dir.rglob(f"*{short_id}*.mp4"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    ) if short_id else []
    if not candidates:
        candidates = sorted(out_dir.rglob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise HTTPException(404, "No rendered video found for this job")
    target = candidates[0].resolve()

    base = settings.output_dir.resolve()
    try:
        rel = target.relative_to(base)
    except ValueError as exc:
        raise HTTPException(404, "Video is outside configured output dir") from exc
    rel_url = str(rel).replace("\\", "/")
    return {
        "video_url": f"/api/gallery/file/{rel_url}",
        "filename": target.name,
        "folder": str(out_dir),
    }


@app.get("/api/stats")
def api_job_stats(conn: DbDep, settings: SettingsDep) -> dict[str, Any]:
    raw = dbmod.get_trim_statistics(conn)
    usd_per_min = float(settings.openai_whisper_usd_per_minute)
    enriched: list[dict[str, Any]] = []
    for m in raw["by_method"]:
        key = m["method_key"]
        is_openai = key == "openai_speech"
        row = {
            **m,
            "label_de": _STATS_METHOD_LABELS_DE.get(key, key.replace("_", " ")),
            "openai_usage_credits_usd": round(float(m["openai_cost_usd"]), 6) if is_openai else None,
        }
        enriched.append(row)
    totals = dict(raw["totals"])
    totals["openai_usage_credits_usd"] = round(float(totals.get("openai_cost_usd") or 0.0), 6)
    return {
        "by_method": enriched,
        "totals": totals,
        "openai_usd_per_minute_assumed": usd_per_min,
        "disclaimer_de": (
            "OpenAI: geschätzte Kosten aus transkribierter Audio-Länge × OPENAI_WHISPER_USD_PER_MINUTE. "
            "Im OpenAI-Dashboard erscheint die Nutzung in USD (häufig als Guthaben/Credits angezeigt)."
        ),
    }


@app.get("/api/gallery")
def gallery(settings: SettingsDep) -> list[dict[str, Any]]:
    root = settings.output_dir
    if not root.is_dir():
        return []
    entries: list[dict[str, Any]] = []
    manifests = sorted(root.glob("job_manifest*.json"))
    for man_path in manifests:
        man = json.loads(man_path.read_text(encoding="utf-8"))
        stem = man_path.stem
        suffix = stem.replace("job_manifest", "", 1)
        src_path = root / f"source{suffix}.json"

        meta = {}
        if src_path.is_file():
            meta = json.loads(src_path.read_text(encoding="utf-8"))

        clips_out: list[dict[str, Any]] = []
        for c in man.get("clips") or []:
            vr = c.get("video_relpath")
            tr = c.get("transcript_relpath")
            clips_out.append(
                {
                    **c,
                    "video_url": f"/api/gallery/file/{vr}" if vr else None,
                    "transcript_url": f"/api/gallery/file/{tr}" if tr else None,
                }
            )

        entries.append(
            {
                "folder": suffix.lstrip("_") or "default",
                "source": meta,
                "clips": clips_out,
                "error": man.get("error"),
            }
        )
    return entries


@app.get("/api/gallery/file/{filename:path}")
def gallery_file(
    filename: str,
    settings: SettingsDep,
) -> FileResponse:
    base = settings.output_dir.resolve()
    target = (base / filename).resolve()
    try:
        target.relative_to(base)
    except ValueError as exc:
        raise HTTPException(404, "Invalid path") from exc
    if not target.is_file():
        raise HTTPException(404, "Not found")
    return FileResponse(target)


@app.post("/api/sync/run")
def run_sync_now(settings: SettingsDep) -> dict[str, str]:
    _scheduled_poll(settings)
    return {"status": "ok"}


def main() -> None:
    import uvicorn

    s = get_settings()
    uvicorn.run(
        "webapp.main:app",
        host=s.host,
        port=s.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
