"""Unified configuration: reads all settings from .env."""

import os
import sys
from dataclasses import dataclass
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()

# --------------------------------------------------------------------------- #
#  启动时预检查关键环境变量
# --------------------------------------------------------------------------- #
_REQUIRED_ENV_VARS = {
    "DB_HOST": "数据库主机地址",
    "DB_PORT": "数据库端口 (默认 5432)",
    "DB_NAME": "数据库名称",
    "DB_USER": "数据库用户名",
    "DB_PASSWORD": "数据库密码",
    "TUSHARE_TOKEN": "Tushare Pro API Token",
}

_MISSING_VARS = [
    var for var in _REQUIRED_ENV_VARS if not os.environ.get(var)
]

if _MISSING_VARS:
    lines = [""]
    lines.append("=" * 60)
    lines.append("  配置错误: 缺少必需的环境变量")
    lines.append("=" * 60)
    lines.append("")
    for var in _MISSING_VARS:
        desc = _REQUIRED_ENV_VARS[var]
        lines.append(f"  [缺失] {var}  — {desc}")
    lines.append("")
    lines.append("请检查项目根目录下的 .env 文件是否存在并已正确配置。")
    lines.append("如果没有 .env，请运行:")
    lines.append("  cp .env.example .env")
    lines.append("  # 编辑 .env 填入真实值")
    lines.append("")
    print("".join(lines), file=sys.stderr)
    sys.exit(1)


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
