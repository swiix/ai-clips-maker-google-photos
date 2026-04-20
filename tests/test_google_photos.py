from pathlib import Path

import httpx

from webapp.google_photos import download_media_base_url


class _FakeResponse:
    def __init__(self, status_code: int, chunks: list[bytes], content_type: str = "video/mp4") -> None:
        self.status_code = status_code
        self._chunks = chunks
        self.request = httpx.Request("GET", "https://example.test")
        self._response = httpx.Response(status_code, request=self.request)
        self.headers = {
            "content-type": content_type,
            "content-length": str(sum(len(c) for c in chunks)),
        }

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError(
                f"status {self.status_code}",
                request=self.request,
                response=self._response,
            )

    def iter_bytes(self):
        return iter(self._chunks)


class _FakeStreamCtx:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response

    def __enter__(self) -> _FakeResponse:
        return self._response

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def test_download_video_downloads_url_uses_raw_url(monkeypatch, tmp_path: Path):
    calls = []

    def fake_stream(method, url, headers=None, **kwargs):
        calls.append((method, url, headers or {}))
        return _FakeStreamCtx(_FakeResponse(200, [b"ok-video"]))

    monkeypatch.setattr(httpx, "stream", fake_stream)

    dest = tmp_path / "video.mov"
    raw_url = "https://video-downloads.googleusercontent.com/abc123"
    download_media_base_url(raw_url, dest, access_token="tok")

    assert dest.read_bytes() == b"ok-video"
    assert len(calls) == 1
    assert calls[0][1] == raw_url
    assert calls[0][2].get("Authorization") == "Bearer tok"


def test_download_lh3_url_falls_back_when_dv_fails(monkeypatch, tmp_path: Path):
    calls = []

    def fake_stream(method, url, headers=None, **kwargs):
        calls.append(url)
        if url.endswith("=dv"):
            return _FakeStreamCtx(_FakeResponse(404, []))
        return _FakeStreamCtx(_FakeResponse(200, [b"fallback-ok"]))

    monkeypatch.setattr(httpx, "stream", fake_stream)

    dest = tmp_path / "video.mov"
    base = "https://lh3.googleusercontent.com/ppa/testtoken"
    download_media_base_url(base, dest)

    assert calls == [base + "=dv", base]
    assert dest.read_bytes() == b"fallback-ok"


def test_download_lh3_403_fails_fast_without_retry_loop(monkeypatch, tmp_path: Path):
    calls = []
    sleeps = []

    def fake_stream(method, url, headers=None, **kwargs):
        calls.append(url)
        return _FakeStreamCtx(_FakeResponse(403, []))

    def fake_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(httpx, "stream", fake_stream)
    monkeypatch.setattr("webapp.google_photos.time.sleep", fake_sleep)

    dest = tmp_path / "video.mov"
    base = "https://lh3.googleusercontent.com/ppa/testtoken"

    try:
        download_media_base_url(base, dest)
        assert False, "Expected HTTPStatusError for 403 response"
    except httpx.HTTPStatusError as exc:
        assert exc.response.status_code == 403

    # One pass over candidates only, then abort (no backoff loop).
    assert calls == [base + "=dv", base]
    assert sleeps == []
