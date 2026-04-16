from __future__ import annotations

from pathlib import Path

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

    scheduler_interval_minutes: int = 0
    auto_enqueue_new_videos: bool = False
    scheduler_max_new_per_run: int = 25

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


def get_settings() -> Settings:
    return Settings()
