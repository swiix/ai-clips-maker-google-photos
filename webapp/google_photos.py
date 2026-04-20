"""Google Photos Picker API helpers (OAuth + REST)."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Callable

import httpx
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow

from webapp.settings import Settings

PICKER_API = "https://photospicker.googleapis.com/v1"


def build_oauth_flow(settings: Settings) -> Flow:
    if not settings.google_client_id or not settings.google_client_secret:
        raise ValueError("GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET must be set")

    client_config = {
        "web": {
            "client_id": settings.google_client_id,
            "client_secret": settings.google_client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [settings.redirect_uri],
        }
    }
    return Flow.from_client_config(
        client_config,
        scopes=list(settings.scopes),
        redirect_uri=settings.redirect_uri,
    )


def load_credentials(settings: Settings) -> Credentials | None:
    path = settings.credentials_path
    if not path.is_file():
        return None
    return Credentials.from_authorized_user_file(str(path), list(settings.scopes))


def save_credentials(settings: Settings, credentials: Credentials) -> None:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.credentials_path.write_text(credentials.to_json(), encoding="utf-8")


def ensure_fresh_credentials(settings: Settings) -> Credentials | None:
    creds = load_credentials(settings)
    if creds is None:
        return None
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(settings, creds)
    return creds


def picker_create_session(access_token: str) -> dict[str, Any]:
    r = httpx.post(
        f"{PICKER_API}/sessions",
        headers={"Authorization": f"Bearer {access_token}"},
        json={},
        timeout=120.0,
    )
    r.raise_for_status()
    return r.json()


def picker_get_session(access_token: str, session_id: str) -> dict[str, Any]:
    r = httpx.get(
        f"{PICKER_API}/sessions/{session_id}",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=120.0,
    )
    r.raise_for_status()
    return r.json()


def picker_list_media_items(
    access_token: str,
    *,
    session_id: str,
    page_size: int = 50,
    page_token: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"sessionId": session_id, "pageSize": page_size}
    if page_token:
        params["pageToken"] = page_token
    r = httpx.get(
        f"{PICKER_API}/mediaItems",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
        timeout=120.0,
    )
    r.raise_for_status()
    return r.json()


def download_media_base_url(
    base_url: str,
    dest: Path,
    access_token: str | None = None,
    progress_callback: Callable[[int, int | None], None] | None = None,
) -> None:
    """
    Download video bytes. Google Photos uses baseUrl + type-specific suffix.
    """
    # Picker / Photos can return different baseUrl forms:
    # - lh3... base URLs often need "=dv" for videos
    # - video-downloads... URLs are usually already final signed download links
    candidates: list[str] = []
    if "video-downloads.googleusercontent.com" in base_url:
        candidates = [base_url]
    elif "=d" in base_url or "=dv" in base_url:
        candidates = [base_url]
    else:
        candidates = [base_url + "=dv", base_url]

    dest.parent.mkdir(parents=True, exist_ok=True)
    headers = {}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    last_error: Exception | None = None
    start_ts = time.monotonic()
    max_wait_seconds = 300.0
    wait_seconds = 5.0
    attempt_no = 0

    while True:
        attempt_no += 1
        processing_like_failure = False
        permanent_access_failure = False

        for url in candidates:
            try:
                with httpx.stream(
                    "GET",
                    url,
                    headers=headers,
                    follow_redirects=True,
                    timeout=600.0,
                ) as resp:
                    resp.raise_for_status()
                    content_type = (resp.headers.get("content-type") or "").lower()
                    if content_type.startswith("image/"):
                        processing_like_failure = True
                        raise ValueError(
                            f"URL returned image payload instead of video ({content_type}). "
                            "Google asset may still be PROCESSING."
                        )
                    total_size: int | None = None
                    cl = resp.headers.get("content-length")
                    if cl and cl.isdigit():
                        total_size = int(cl)
                    downloaded = 0
                    if progress_callback:
                        progress_callback(downloaded, total_size)
                    with open(dest, "wb") as f:
                        for chunk in resp.iter_bytes():
                            f.write(chunk)
                            downloaded += len(chunk)
                            if progress_callback:
                                progress_callback(downloaded, total_size)
                logging.info("Downloaded media to %s via %s", dest, url)
                return
            except httpx.HTTPStatusError as exc:
                last_error = exc
                req_url = str(getattr(exc, "request", {}).url) if getattr(exc, "request", None) else url
                status = exc.response.status_code if exc.response is not None else None
                if status == 404 and "video-downloads.googleusercontent.com" in req_url:
                    processing_like_failure = True
                elif status in {401, 403}:
                    # Access/permission failures usually will not recover by waiting.
                    permanent_access_failure = True
                logging.warning(
                    "Download candidate failed (attempt %s, url=%s, status=%s): %s",
                    attempt_no,
                    url,
                    status,
                    exc,
                )
            except Exception as exc:
                last_error = exc
                logging.warning(
                    "Download candidate failed (attempt %s, url=%s): %s",
                    attempt_no,
                    url,
                    exc,
                )

        if permanent_access_failure:
            elapsed = time.monotonic() - start_ts
            logging.error(
                "Stopping download retries after permanent access failure (attempt=%s, elapsed=%.1fs, base_url=%s).",
                attempt_no,
                elapsed,
                base_url,
            )
            break

        elapsed = time.monotonic() - start_ts
        if elapsed >= max_wait_seconds:
            logging.error(
                "Stopping download retries after timeout (attempt=%s, elapsed=%.1fs, base_url=%s).",
                attempt_no,
                elapsed,
                base_url,
            )
            break

        if processing_like_failure:
            logging.info(
                "Asset appears to be processing (attempt=%s). Retrying in %ss (elapsed %.1fs/%ss)...",
                attempt_no,
                int(wait_seconds),
                elapsed,
                int(max_wait_seconds),
            )
            time.sleep(wait_seconds)
            wait_seconds = min(wait_seconds * 1.5, 30.0)
            continue

        # For non-processing failures, do a short retry window too.
        logging.info(
            "Transient download failure, retrying (attempt=%s) in %ss (elapsed %.1fs/%ss).",
            attempt_no,
            int(min(wait_seconds, 10.0)),
            elapsed,
            int(max_wait_seconds),
        )
        time.sleep(min(wait_seconds, 10.0))
        wait_seconds = min(wait_seconds * 1.5, 20.0)

    assert last_error is not None
    raise last_error
