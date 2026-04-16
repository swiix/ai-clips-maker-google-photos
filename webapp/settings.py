from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    google_client_id: str = ""
    google_client_secret: str = ""
    redirect_uri: str = "http://127.0.0.1:8844/auth/callback"

    host: str = "127.0.0.1"
    port: int = 8844

    data_dir: Path = Path(".ai-clips-web")
    output_dir: Path = Path("outputs")
    cache_dir: Path = Path("cache_downloads")
    log_dir: Path = Path(".ai-clips-web/logs")
    log_level: str = "INFO"
    log_max_mb: int = 10
    log_backup_count: int = 5

    pyannote_token: str = ""

    openai_api_key: str = ""
    openai_transcription_model: str = "whisper-1"
    # Estimated USD per minute for whisper-1 (verify on https://platform.openai.com/docs/pricing )
    openai_whisper_usd_per_minute: float = 0.006
    # Optional: path to JSON with openai_api_key (default: {data_dir}/openai_credentials.json). Env: OPENAI_CREDENTIALS_JSON
    openai_credentials_json: Optional[Path] = None

    scheduler_interval_minutes: int = 0
    auto_enqueue_new_videos: bool = False
    scheduler_max_new_per_run: int = 25
    worker_concurrency: int = 10

    photos_page_size: int = 50

    scopes: tuple[str, ...] = (
        "https://www.googleapis.com/auth/photospicker.mediaitems.readonly",
    )

    @property
    def sqlite_path(self) -> Path:
        return self.data_dir / "app.db"

    @property
    def credentials_path(self) -> Path:
        return self.data_dir / "google_credentials.json"

    @property
    def openai_credentials_path(self) -> Path:
        if self.openai_credentials_json is not None:
            return self.openai_credentials_json
        return self.data_dir / "openai_credentials.json"

    @model_validator(mode="after")
    def load_openai_key_from_json(self) -> "Settings":
        """
        Prefer API key from local JSON over OPENAI_API_KEY / openai_api_key env when the file exists
        and contains a non-empty key.
        """
        path = self.openai_credentials_path
        if not path.is_file():
            return self
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            key = str(
                payload.get("openai_api_key")
                or payload.get("api_key")
                or payload.get("apiKey")
                or ""
            ).strip()
            if key:
                object.__setattr__(self, "openai_api_key", key)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
        return self


def get_settings() -> Settings:
    return Settings()
