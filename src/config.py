"""Unified configuration: reads all settings from .env."""

import os
from dataclasses import dataclass
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class DatabaseConfig:
    host: str = os.environ.get("DB_HOST", "localhost")
    port: int = int(os.environ.get("DB_PORT", "5432"))
    dbname: str = os.environ.get("DB_NAME", "quant_db")
    user: str = os.environ.get("DB_USER", "postgres")
    password: str = os.environ.get("DB_PASSWORD", "")

    @property
    def url(self) -> str:
        safe_pass = quote_plus(self.password)
        return f"postgresql://{self.user}:{safe_pass}@{self.host}:{self.port}/{self.dbname}?client_encoding=utf8"


@dataclass(frozen=True)
class TushareConfig:
    token: str = os.environ.get("TUSHARE_TOKEN", "")
    rate_limit_per_min: int = int(os.environ.get("TUSHARE_RATE_LIMIT", "200"))


@dataclass(frozen=True)
class SyncConfig:
    max_workers: int = int(os.environ.get("SYNC_MAX_WORKERS", "5"))
    batch_size: int = int(os.environ.get("SYNC_BATCH_SIZE", "1000"))
    progress_dir: str = os.environ.get("SYNC_PROGRESS_DIR", ".progress")
