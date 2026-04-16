"""
Local FastAPI entrypoint: Google Photos OAuth, media listing, clip jobs, gallery.
"""

from __future__ import annotations

import json
import logging
import os
import re
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
_gallery_cache: dict[str, dict[str, Any]] = {}
_GALLERY_CACHE_TTL_SEC = 12.0
_DURATION_TAG_RE = re.compile(r"_(\d+(?:d\d+)?)s_to_(\d+(?:d\d+)?)s_")


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


app = FastAPI(title="Meta Glasses AI Magic Clips Google Photos", lifespan=lifespan)

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
    cut_merge_gap_sec: Optional[float] = None
    cut_min_duration_sec: Optional[float] = None
    openai_merge_gap_sec: Optional[float] = None
    openai_min_segment_sec: Optional[float] = None
    noise_reduction: Optional[bool] = True
    noise_reduction_mode: Optional[str] = "auto"


class CacheClearAdvancedBody(BaseModel):
    older_than_days: int = 30
    images: bool = True
    videos: bool = True
    audio: bool = True
    other_files: bool = True


class TinderReviewBody(BaseModel):
    clip_key: str
    job_id: Optional[int] = None
    media_item_id: Optional[str] = None
    decision: Optional[str] = None
    downloaded: Optional[bool] = None
    trim_mode: Optional[str] = None
    source_filename: Optional[str] = None
    folder: Optional[str] = None
    video_url: Optional[str] = None
    begin_sec: Optional[float] = None
    finish_sec: Optional[float] = None


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


def _cache_bucket_for_file(path: Path) -> str:
    ext = path.suffix.lower()
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".heif"}:
        return "images"
    if ext in {".mp4", ".mov", ".m4v", ".mkv", ".avi", ".webm"}:
        return "videos"
    if ext in {".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg"}:
        return "audio"
    return "other_files"


@app.get("/api/cache/summary")
def cache_summary(settings: SettingsDep) -> dict[str, Any]:
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    by_type: dict[str, dict[str, int]] = {
        "images": {"count": 0, "bytes": 0},
        "videos": {"count": 0, "bytes": 0},
        "audio": {"count": 0, "bytes": 0},
        "other_files": {"count": 0, "bytes": 0},
    }
    total_bytes = 0
    total_files = 0
    for p in settings.cache_dir.iterdir():
        if not p.is_file():
            continue
        try:
            size = int(p.stat().st_size)
        except OSError:
            continue
        bucket = _cache_bucket_for_file(p)
        by_type[bucket]["count"] += 1
        by_type[bucket]["bytes"] += size
        total_bytes += size
        total_files += 1
    return {
        "total_bytes": total_bytes,
        "total_files": total_files,
        "by_type": by_type,
    }


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


@app.post("/api/cache/clear-advanced")
def clear_video_cache_advanced(body: CacheClearAdvancedBody, settings: SettingsDep) -> dict[str, Any]:
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    now = time.time()
    cutoff = now - (max(0, int(body.older_than_days)) * 86400)
    enabled_buckets = set()
    if body.images:
        enabled_buckets.add("images")
    if body.videos:
        enabled_buckets.add("videos")
    if body.audio:
        enabled_buckets.add("audio")
    if body.other_files:
        enabled_buckets.add("other_files")

    removed = 0
    failed = 0
    skipped_recent = 0
    skipped_type = 0
    for p in settings.cache_dir.iterdir():
        if not p.is_file():
            continue
        bucket = _cache_bucket_for_file(p)
        if bucket not in enabled_buckets:
            skipped_type += 1
            continue
        try:
            st = p.stat()
            if st.st_mtime > cutoff:
                skipped_recent += 1
                continue
            p.unlink()
            removed += 1
        except Exception:
            failed += 1
    return {
        "removed_files": removed,
        "failed_files": failed,
        "skipped_recent_files": skipped_recent,
        "skipped_type_files": skipped_type,
    }


@app.get("/api/tinder/reviews")
def tinder_reviews_list(conn: DbDep) -> list[dict[str, Any]]:
    return dbmod.list_tinder_reviews(conn)


@app.post("/api/tinder/reviews")
def tinder_reviews_upsert(body: TinderReviewBody, conn: DbDep) -> dict[str, Any]:
    clip_key = str(body.clip_key or "").strip()
    if not clip_key:
        raise HTTPException(400, "clip_key is required")
    dbmod.upsert_tinder_review(
        conn,
        clip_key=clip_key,
        job_id=body.job_id,
        media_item_id=body.media_item_id,
        decision=body.decision,
        downloaded=body.downloaded,
        trim_mode=body.trim_mode,
        source_filename=body.source_filename,
        folder=body.folder,
        video_url=body.video_url,
        begin_sec=body.begin_sec,
        finish_sec=body.finish_sec,
    )
    return {"status": "ok", "clip_key": clip_key}


_STATS_METHOD_LABELS_DE: dict[str, str] = {
    "openai_speech": "OpenAI (Whisper · gesprochene Segmente)",
    "silence_all": "Stille entfernen · alle Profile",
    "silence_conservative": "Stille entfernen · Conservative",
    "silence_balanced": "Stille entfernen · Balanced",
    "silence_aggressive": "Stille entfernen · Aggressive",
    "silence_unknown": "Stille entfernen (älterer Eintrag)",
    "clip_pipeline_ai": "KI-Clip-Pipeline (Diarize + Clips)",
}


def _extract_cut_tuning_from_body(body: EnqueueBody) -> tuple[float | None, float | None]:
    cut_merge_gap_sec: float | None = None
    cut_min_duration_sec: float | None = None
    for candidate in (body.cut_merge_gap_sec, body.openai_merge_gap_sec):
        if candidate is None:
            continue
        try:
            val = float(candidate)
            if val > 0:
                cut_merge_gap_sec = val
                break
        except (TypeError, ValueError):
            pass
    for candidate in (body.cut_min_duration_sec, body.openai_min_segment_sec):
        if candidate is None:
            continue
        try:
            val = float(candidate)
            if val > 0:
                cut_min_duration_sec = val
                break
        except (TypeError, ValueError):
            pass
    return cut_merge_gap_sec, cut_min_duration_sec


@app.post("/api/jobs")
def enqueue_jobs(body: EnqueueBody, conn: DbDep) -> dict[str, Any]:
    queued = []
    skipped = []
    noise_reduction_enabled = bool(body.noise_reduction is not False)
    cut_merge_gap_sec, cut_min_duration_sec = _extract_cut_tuning_from_body(body)
    noise_mode = str(body.noise_reduction_mode or "auto").strip().lower()
    if noise_mode not in {"auto", "mild", "strong"}:
        noise_mode = "auto"
    options: dict[str, Any] = {
        "trim_method": "clip_pipeline_ai",
        "noise_reduction": noise_reduction_enabled,
        "noise_reduction_mode": noise_mode,
    }
    if cut_merge_gap_sec is not None:
        options["cut_merge_gap_sec"] = cut_merge_gap_sec
    if cut_min_duration_sec is not None:
        options["cut_min_duration_sec"] = cut_min_duration_sec
    options_json = json.dumps(options, ensure_ascii=True)
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
            job_options=options_json,
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
    noise_reduction_enabled = bool(body.noise_reduction is not False)
    noise_mode = str(body.noise_reduction_mode or "auto").strip().lower()
    if noise_mode not in {"auto", "mild", "strong"}:
        noise_mode = "auto"
    cut_merge_gap_sec, cut_min_duration_sec = _extract_cut_tuning_from_body(body)

    def _dump(opts: dict[str, Any]) -> str:
        payload = dict(opts)
        payload["noise_reduction"] = noise_reduction_enabled
        payload["noise_reduction_mode"] = noise_mode
        if cut_merge_gap_sec is not None:
            payload["cut_merge_gap_sec"] = cut_merge_gap_sec
        if cut_min_duration_sec is not None:
            payload["cut_min_duration_sec"] = cut_min_duration_sec
        return json.dumps(payload, ensure_ascii=True)

    if raw == "openai_speech":
        opts: dict[str, Any] = {"trim_method": "openai_speech"}
        if cut_merge_gap_sec is not None:
            opts["openai_merge_gap_sec"] = cut_merge_gap_sec
        if cut_min_duration_sec is not None:
            opts["openai_min_segment_sec"] = cut_min_duration_sec
        return "openai_speech_trim", _dump(opts)
    if raw in valid_silence:
        return "silence_remove", _dump({"trim_method": raw})
    profs = [p for p in (body.profiles or []) if p in {"conservative", "balanced", "aggressive"}]
    if len(profs) >= 3:
        return "silence_remove", _dump({"trim_method": "silence_all", "profiles": profs[:3]})
    if len(profs) == 1:
        return "silence_remove", _dump({"trim_method": f"silence_{profs[0]}", "profiles": profs})
    if profs:
        return "silence_remove", _dump({"trim_method": "silence_balanced", "profiles": profs})
    return "silence_remove", _dump({"trim_method": "silence_balanced"})


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
def list_jobs(conn: DbDep, settings: SettingsDep) -> list[dict[str, Any]]:
    rows = dbmod.list_jobs(conn)
    return [_enrich_job_cut_metrics(r, settings) for r in rows]


@app.post("/api/jobs/retry-failed-cached")
def retry_failed_cached_jobs(conn: DbDep, settings: SettingsDep) -> dict[str, Any]:
    """
    Requeue failed jobs only when local cached source file exists.
    Useful after dev-server reloads where running jobs were marked failed.
    """
    rows = conn.execute(
        """
        SELECT id, media_item_id, filename, base_url, product_url, creation_time,
               job_type, job_options, trim_method_label, status
        FROM jobs
        WHERE status = 'failed'
        ORDER BY id
        """
    ).fetchall()
    retried_job_ids: list[int] = []
    skipped_no_cache_ids: list[int] = []
    skipped_not_requeueable_ids: list[int] = []
    for r in rows:
        cache_path = _cache_target_path(settings, str(r["media_item_id"]), r["filename"])
        if not cache_path.is_file() or cache_path.stat().st_size <= 0:
            skipped_no_cache_ids.append(int(r["id"]))
            continue
        jid, enq = dbmod.create_or_requeue_job(
            conn,
            str(r["media_item_id"]),
            filename=r["filename"],
            base_url=r["base_url"],
            product_url=r["product_url"],
            creation_time=r["creation_time"],
            job_type=str(r["job_type"] or "clip_pipeline"),
            job_options=r["job_options"],
            trim_method_label=r["trim_method_label"],
        )
        if enq:
            jobsmod.enqueue_job_id(jid)
            retried_job_ids.append(jid)
        else:
            skipped_not_requeueable_ids.append(int(r["id"]))
    return {
        "retried_job_ids": retried_job_ids,
        "skipped_no_cache_ids": skipped_no_cache_ids,
        "skipped_not_requeueable_ids": skipped_not_requeueable_ids,
    }


def _parse_duration_token(tok: str) -> float | None:
    try:
        return float(tok.replace("d", "."))
    except (TypeError, ValueError):
        return None


def _find_cut_metrics_from_filenames(row: dict[str, Any], settings: Settings) -> tuple[float, float] | None:
    out_raw = str(row.get("output_dir") or "").strip()
    if not out_raw:
        return None
    out_dir = Path(out_raw).expanduser()
    if not out_dir.is_absolute():
        out_dir = (Path.cwd() / out_dir).resolve()
    if not out_dir.is_dir():
        return None
    short_id = str(row.get("media_item_id") or "")[:12]
    mp4s = sorted(out_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
    if short_id:
        preferred = [p for p in mp4s if short_id in p.name]
        if preferred:
            mp4s = preferred
    for p in mp4s:
        m = _DURATION_TAG_RE.search(p.name)
        if not m:
            continue
        before = _parse_duration_token(m.group(1))
        after = _parse_duration_token(m.group(2))
        if before is None or after is None or before <= 0 or after < 0:
            continue
        return (before, after)
    return None


def _enrich_job_cut_metrics(row: dict[str, Any], settings: Settings) -> dict[str, Any]:
    data = dict(row)
    before = data.get("cut_input_seconds")
    after = data.get("cut_output_seconds")
    source = "db"
    if (before is None or after is None) and str(data.get("status") or "") == "done":
        fallback = _find_cut_metrics_from_filenames(data, settings)
        if fallback is not None:
            before, after = fallback
            source = "filename"
    try:
        b = float(before) if before is not None else None
        a = float(after) if after is not None else None
    except (TypeError, ValueError):
        b = None
        a = None
    if b is not None and a is not None and b > 0:
        saved = max(0.0, b - a)
        percent = (saved / b) * 100.0
        data["cut_input_seconds"] = b
        data["cut_output_seconds"] = a
        data["cut_saved_seconds"] = saved
        data["cut_saved_percent"] = percent
        data["cut_metrics_source"] = source
    else:
        data["cut_saved_seconds"] = None
        data["cut_saved_percent"] = None
        data["cut_metrics_source"] = None
    return data


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


def _build_gallery_entries(
    settings: Settings,
    conn,
    *,
    include_orphans: bool,
) -> list[dict[str, Any]]:
    root = settings.output_dir
    if not root.is_dir():
        return []
    entries: list[dict[str, Any]] = []
    seen_video_rels: set[str] = set()
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
            if vr:
                seen_video_rels.add(str(vr).replace("\\", "/"))
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

    # Include all finished job outputs as video items, even when no job_manifest exists
    # (e.g. silence/openai output files that are generated directly as MP4).
    done_rows = conn.execute(
        """
        SELECT id, media_item_id, filename, creation_time, output_dir
        FROM jobs
        WHERE status = 'done' AND output_dir IS NOT NULL
        ORDER BY updated_at DESC
        """
    ).fetchall()
    base = settings.output_dir.resolve()
    for r in done_rows:
        out_dir = Path(str(r["output_dir"] or "")).expanduser()
        if not out_dir.is_absolute():
            out_dir = (Path.cwd() / out_dir).resolve()
        else:
            out_dir = out_dir.resolve()
        if not out_dir.is_dir():
            continue
        short_id = str(r["media_item_id"] or "")[:12]
        candidates = sorted(out_dir.rglob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
        if short_id:
            preferred = [p for p in candidates if short_id in p.name]
            if preferred:
                candidates = preferred
        clips_out: list[dict[str, Any]] = []
        for idx, target in enumerate(candidates, start=1):
            try:
                rel = target.resolve().relative_to(base)
            except ValueError:
                continue
            rel_url = str(rel).replace("\\", "/")
            if rel_url in seen_video_rels:
                continue
            seen_video_rels.add(rel_url)
            clips_out.append(
                {
                    "index": idx,
                    "begin_sec": 0,
                    "finish_sec": 0,
                    "video_url": f"/api/gallery/file/{rel_url}",
                    "transcript_url": None,
                }
            )
        if not clips_out:
            continue
        entries.append(
            {
                "folder": f"job_{int(r['id'])}",
                "source": {
                    "filename": r["filename"],
                    "creationTime": r["creation_time"],
                    "mediaItemId": r["media_item_id"],
                    "jobId": int(r["id"]),
                },
                "clips": clips_out,
                "error": None,
            }
        )

    if include_orphans:
        # Final fallback: include any MP4 files in output root/subfolders that were not
        # covered by manifests or done-job based discovery. This ensures old outputs are
        # still visible in TinderWatch for review.
        orphan_clips: list[dict[str, Any]] = []
        all_mp4 = sorted(root.rglob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
        for idx, target in enumerate(all_mp4, start=1):
            try:
                rel = target.resolve().relative_to(base)
            except ValueError:
                continue
            rel_url = str(rel).replace("\\", "/")
            if rel_url in seen_video_rels:
                continue
            seen_video_rels.add(rel_url)
            orphan_clips.append(
                {
                    "index": idx,
                    "begin_sec": 0,
                    "finish_sec": 0,
                    "video_url": f"/api/gallery/file/{rel_url}",
                    "transcript_url": None,
                }
            )
        if orphan_clips:
            entries.append(
                {
                    "folder": "legacy_outputs",
                    "source": {
                        "filename": "Legacy Output Videos",
                        "creationTime": None,
                        "mediaItemId": None,
                    },
                    "clips": orphan_clips,
                    "error": None,
                }
            )
    return entries


@app.get("/api/gallery")
def gallery(
    settings: SettingsDep,
    conn: DbDep,
    include_orphans: bool = Query(True),
    use_cache: bool = Query(True),
) -> list[dict[str, Any]]:
    cache_key = "with_orphans" if include_orphans else "without_orphans"
    now = time.time()
    if use_cache:
        cached = _gallery_cache.get(cache_key)
        if cached and (now - float(cached.get("ts", 0.0))) < _GALLERY_CACHE_TTL_SEC:
            return cached["entries"]
    entries = _build_gallery_entries(settings, conn, include_orphans=include_orphans)
    _gallery_cache[cache_key] = {"ts": now, "entries": entries}
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
