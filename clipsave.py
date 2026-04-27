import asyncio
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse, urlunparse
import urllib.error
import urllib.request
from decimal import Decimal, InvalidOperation

import httpx
import yt_dlp
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


def resolve_path_env(value: str, default: Path) -> Path:
    raw = (value or "").strip()
    if not raw:
        path = default
    else:
        path = Path(raw)
    if not path.is_absolute():
        path = (BASE_DIR / path).resolve()
    return path


WEB_HOST = os.getenv("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.getenv("WEB_PORT", "8093"))
WEB_SECRET_KEY = os.getenv("WEB_SECRET_KEY", "change_me")
WEB_LOGIN_KEY = os.getenv("WEB_LOGIN_KEY", "change_me_login")
WEB_ADMIN_LOGIN_KEY = os.getenv("WEB_ADMIN_LOGIN_KEY", "change_me_admin_login")
ADMIN_COOKIES_FILE = os.getenv("ADMIN_COOKIES_FILE", "admin_cookies.txt")
WEB_BASE_PATH = os.getenv("WEB_BASE_PATH", "/df2sf4gf54dfchg45dfg4h5fg4").rstrip("/") or "/app"
WEB_COOKIE_UID = os.getenv("WEB_COOKIE_UID", "web_ytd_uid")
WEB_COOKIE_SESSION = os.getenv("WEB_COOKIE_SESSION", "web_ytd_session")
WEB_UID_MAX_AGE = int(os.getenv("WEB_UID_MAX_AGE", str(180 * 24 * 3600)))
WEB_SESSION_MAX_AGE = int(os.getenv("WEB_SESSION_MAX_AGE", str(7 * 24 * 3600)))
USER_RETENTION_DAYS = int(os.getenv("USER_RETENTION_DAYS", "30"))
MAX_ACTIVE_TASKS = max(1, int(os.getenv("MAX_ACTIVE_TASKS", "1")))
MAX_ACTIVE_TASKS_PER_USER = max(1, int(os.getenv("MAX_ACTIVE_TASKS_PER_USER", "1")))
REQUEST_TTL_HOURS = int(os.getenv("REQUEST_TTL_HOURS", "1"))
DOWNLOAD_PATH = resolve_path_env(os.getenv("DOWNLOAD_PATH", "/download"), Path("/download"))
COOKIES_PATH = resolve_path_env(os.getenv("COOKIES_PATH", "cookies"), BASE_DIR / "cookies")
DATA_PATH = resolve_path_env(os.getenv("DATA_PATH", "data"), BASE_DIR / "data")
LOG_PATH = resolve_path_env(os.getenv("LOG_PATH", "logs"), BASE_DIR / "logs")
PUBLIC_BASE_URL = os.getenv("WEB_PUBLIC_BASE_URL", os.getenv("PUBLIC_BASE_URL", "")).rstrip("/")
DEBUG_YTDLP = os.getenv("DEBUG_YTDLP", "0") == "1"
SQLITE_DB_NAME = os.getenv("SQLITE_DB_NAME", "web_ytd.sqlite3")
SQLITE_PATH = os.getenv("SQLITE_PATH", "").strip()

DB_PATH = resolve_path_env(SQLITE_PATH, DATA_PATH / SQLITE_DB_NAME)
USERS_FILE = DATA_PATH / "users.json"
SESSIONS_FILE = DATA_PATH / "sessions.json"
HISTORY_DIR = DATA_PATH / "history"
MIGRATION_BACKUP_DIR = DATA_PATH / "migration_backup"

DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
COOKIES_PATH.mkdir(parents=True, exist_ok=True)
DATA_PATH.mkdir(parents=True, exist_ok=True)
LOG_PATH.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)
MIGRATION_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH / "web_ytd.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("web_ytd")

REQUEST_TTL = dt.timedelta(hours=REQUEST_TTL_HOURS)
USER_RETENTION = dt.timedelta(days=USER_RETENTION_DAYS)

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
web = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.mount(WEB_BASE_PATH, web)
web.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

analysis_lock = asyncio.Lock()
queue_state_lock = asyncio.Lock()
db_write_lock = asyncio.Lock()


task_queue: asyncio.Queue[str] = asyncio.Queue()
queued_task_ids: list[str] = []
worker_tasks: list[asyncio.Task] = []
active_tasks: dict[str, dict[str, Any]] = {}
proxy_stream_tokens: dict[str, dict[str, Any]] = {}
proxy_stream_lock = asyncio.Lock()


class DownloadCancelledError(RuntimeError):
    pass


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL,
  last_seen_at TEXT,
  last_activity_at TEXT,
  last_download_at TEXT,
  is_admin INTEGER NOT NULL DEFAULT 0,
  is_disabled INTEGER NOT NULL DEFAULT 0,
  access_type TEXT NOT NULL DEFAULT 'universal',
  invite_id INTEGER,
  cookie_file TEXT,
  cookie_uploaded_at TEXT,
  FOREIGN KEY (invite_id) REFERENCES invite_links(invite_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS invite_links (
  invite_id INTEGER PRIMARY KEY AUTOINCREMENT,
  token TEXT NOT NULL UNIQUE,
  label TEXT,
  created_at TEXT NOT NULL,
  created_by_user_id TEXT,
  activated_user_id TEXT UNIQUE,
  activated_at TEXT,
  revoked_at TEXT,
  note TEXT,
  FOREIGN KEY (created_by_user_id) REFERENCES users(user_id) ON DELETE SET NULL,
  FOREIGN KEY (activated_user_id) REFERENCES users(user_id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS download_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  title TEXT,
  mode TEXT,
  status TEXT NOT NULL,
  error TEXT,
  source_url TEXT,
  download_url TEXT,
  filename TEXT,
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS downloaded_files (
  file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  history_id INTEGER,
  public_token TEXT NOT NULL UNIQUE,
  source_url TEXT,
  stored_filename TEXT NOT NULL,
  file_path TEXT NOT NULL,
  file_size INTEGER NOT NULL DEFAULT 0,
  mime_type TEXT,
  quality_label TEXT,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  last_accessed_at TEXT,
  access_count INTEGER NOT NULL DEFAULT 0,
  deleted_at TEXT,
  delete_reason TEXT,
  FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
  FOREIGN KEY (history_id) REFERENCES download_history(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_users_last_seen_at ON users(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_users_last_activity_at ON users(last_activity_at);
CREATE INDEX IF NOT EXISTS idx_users_invite_id ON users(invite_id);
CREATE INDEX IF NOT EXISTS idx_history_user_created_at ON download_history(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_invite_links_activated_at ON invite_links(activated_at);
CREATE INDEX IF NOT EXISTS idx_invite_links_revoked_at ON invite_links(revoked_at);
CREATE INDEX IF NOT EXISTS idx_files_user_created_at ON downloaded_files(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_files_token ON downloaded_files(public_token);
CREATE INDEX IF NOT EXISTS idx_files_expires_at ON downloaded_files(expires_at);
CREATE INDEX IF NOT EXISTS idx_files_deleted_at ON downloaded_files(deleted_at);
"""


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def iso(dt_obj: dt.datetime | None) -> str | None:
    if dt_obj is None:
        return None
    return dt_obj.astimezone(dt.timezone.utc).isoformat()


def from_iso(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return json.loads(json.dumps(default))
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read JSON from %s", path)
        return json.loads(json.dumps(default))


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [row_to_dict(row) for row in rows if row is not None]


def _db_init() -> None:
    with db_connect() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def _db_fetchone(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    with db_connect() as conn:
        row = conn.execute(query, params).fetchone()
        return row_to_dict(row)


def _db_fetchall(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with db_connect() as conn:
        rows = conn.execute(query, params).fetchall()
        return rows_to_dicts(rows)


def _db_execute(query: str, params: tuple[Any, ...] = ()) -> int:
    with db_connect() as conn:
        cur = conn.execute(query, params)
        conn.commit()
        return cur.rowcount


def _db_execute_insert(query: str, params: tuple[Any, ...] = ()) -> int:
    with db_connect() as conn:
        cur = conn.execute(query, params)
        conn.commit()
        return int(cur.lastrowid)


async def db_init() -> None:
    async with db_write_lock:
        await asyncio.to_thread(_db_init)


async def db_fetchone(query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    return await asyncio.to_thread(_db_fetchone, query, params)


async def db_fetchall(query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    return await asyncio.to_thread(_db_fetchall, query, params)


async def db_execute(query: str, params: tuple[Any, ...] = ()) -> int:
    async with db_write_lock:
        return await asyncio.to_thread(_db_execute, query, params)


async def db_execute_insert(query: str, params: tuple[Any, ...] = ()) -> int:
    async with db_write_lock:
        return await asyncio.to_thread(_db_execute_insert, query, params)


DEFAULT_SETTINGS: dict[str, str] = {
    "download_retention_minutes": "60",
    "watch_extend_minutes": "240",
    "extend_expiry_on_watch": "1",
    "max_single_file_gb": "4",
    "max_download_dir_gb": "18",
    "min_free_disk_gb": "5",
    "max_video_height": "1080",
    "allow_unlimited_file_size": "0",
    "allow_unlimited_download_dir": "0",
    "allow_unlimited_quality": "0",
    "user_quality_selection_enabled": "1",
    "default_user_quality": "1080",
    "experimental_proxy_download_enabled": "0",
    "experimental_proxy_max_file_gb": "2",
    "experimental_proxy_max_duration_minutes": "30",
}

QUALITY_HEIGHTS = [720, 1080, 1440, 2160]


def parse_decimal_setting(value: Any, default: Decimal) -> Decimal:
    raw = str(value if value is not None else "").strip().replace(",", ".")
    if not raw:
        return default
    try:
        parsed = Decimal(raw)
    except (InvalidOperation, ValueError):
        return default
    if parsed < 0:
        return default
    return parsed


def setting_bool(settings: dict[str, str], key: str) -> bool:
    return str(settings.get(key, DEFAULT_SETTINGS.get(key, "0"))).strip().lower() in {"1", "true", "yes", "on"}


def setting_int(settings: dict[str, str], key: str, default: int) -> int:
    try:
        return int(str(settings.get(key, str(default))).strip())
    except Exception:
        return default


def setting_gb_to_bytes(settings: dict[str, str], key: str, default_gb: Decimal) -> int:
    value = parse_decimal_setting(settings.get(key), default_gb)
    return int(value * Decimal(1024 ** 3))


def _ensure_default_settings_sync() -> None:
    now_iso = iso(now_utc())
    with db_connect() as conn:
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                "INSERT OR IGNORE INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
                (key, value, now_iso),
            )
        conn.commit()


async def ensure_default_settings() -> None:
    async with db_write_lock:
        await asyncio.to_thread(_ensure_default_settings_sync)


def _get_settings_sync() -> dict[str, str]:
    _ensure_default_settings_sync()
    with db_connect() as conn:
        rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    settings = dict(DEFAULT_SETTINGS)
    settings.update({row["key"]: row["value"] for row in rows})
    return settings


async def get_settings() -> dict[str, str]:
    return await asyncio.to_thread(_get_settings_sync)


def normalize_quality_height(value: Any, *, allow_unlimited: bool) -> int:
    raw = str(value if value is not None else "").strip().lower()
    if allow_unlimited and raw in {"0", "none", "unlimited", "без ограничения"}:
        return 0
    try:
        height = int(raw)
    except Exception:
        return int(DEFAULT_SETTINGS["max_video_height"])
    if height <= 0:
        return 0 if allow_unlimited else int(DEFAULT_SETTINGS["max_video_height"])
    return height


def normalize_gb_input(value: Any, default_value: str) -> str:
    parsed = parse_decimal_setting(value, Decimal(default_value.replace(",", ".")))
    if parsed <= 0:
        parsed = Decimal(default_value.replace(",", "."))
    normalized = format(parsed.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or default_value


async def update_settings_from_form(form: dict[str, Any]) -> dict[str, str]:
    current = await get_settings()
    allowed_quality_values = {0, 720, 1080, 1440, 2160}

    next_values = {
        "download_retention_minutes": str(max(1, setting_int(form, "download_retention_minutes", setting_int(current, "download_retention_minutes", 60)))),
        "watch_extend_minutes": str(max(1, setting_int(form, "watch_extend_minutes", setting_int(current, "watch_extend_minutes", 240)))),
        "extend_expiry_on_watch": "1" if str(form.get("extend_expiry_on_watch", "")).lower() in {"1", "true", "on", "yes"} else "0",
        "max_single_file_gb": normalize_gb_input(form.get("max_single_file_gb"), current.get("max_single_file_gb", DEFAULT_SETTINGS["max_single_file_gb"])),
        "max_download_dir_gb": normalize_gb_input(form.get("max_download_dir_gb"), current.get("max_download_dir_gb", DEFAULT_SETTINGS["max_download_dir_gb"])),
        "min_free_disk_gb": normalize_gb_input(form.get("min_free_disk_gb"), current.get("min_free_disk_gb", DEFAULT_SETTINGS["min_free_disk_gb"])),
        "allow_unlimited_file_size": "1" if str(form.get("allow_unlimited_file_size", "")).lower() in {"1", "true", "on", "yes"} else "0",
        "allow_unlimited_download_dir": "1" if str(form.get("allow_unlimited_download_dir", "")).lower() in {"1", "true", "on", "yes"} else "0",
        "allow_unlimited_quality": "1" if str(form.get("allow_unlimited_quality", "")).lower() in {"1", "true", "on", "yes"} else "0",
        "user_quality_selection_enabled": "1" if str(form.get("user_quality_selection_enabled", "")).lower() in {"1", "true", "on", "yes"} else "0",
        "default_user_quality": str(normalize_quality_height(form.get("default_user_quality"), allow_unlimited=True)),
        "experimental_proxy_download_enabled": "1" if str(form.get("experimental_proxy_download_enabled", "")).lower() in {"1", "true", "on", "yes"} else "0",
        "experimental_proxy_max_file_gb": normalize_gb_input(form.get("experimental_proxy_max_file_gb"), current.get("experimental_proxy_max_file_gb", DEFAULT_SETTINGS["experimental_proxy_max_file_gb"])),
        "experimental_proxy_max_duration_minutes": str(max(1, setting_int(form, "experimental_proxy_max_duration_minutes", setting_int(current, "experimental_proxy_max_duration_minutes", 30)))),
    }

    max_video_height = normalize_quality_height(form.get("max_video_height"), allow_unlimited=True)
    if max_video_height not in allowed_quality_values:
        max_video_height = setting_int(current, "max_video_height", 1080)
    next_values["max_video_height"] = str(max_video_height)

    if int(next_values["default_user_quality"]) not in allowed_quality_values:
        next_values["default_user_quality"] = current.get("default_user_quality", DEFAULT_SETTINGS["default_user_quality"])

    now_iso = iso(now_utc())
    async with db_write_lock:
        def _save() -> None:
            with db_connect() as conn:
                for key, value in next_values.items():
                    conn.execute(
                        """
                        INSERT INTO app_settings (key, value, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                        """,
                        (key, value, now_iso),
                    )
                conn.commit()
        await asyncio.to_thread(_save)

    return await get_settings()


def settings_public_view(settings: dict[str, str]) -> dict[str, Any]:
    max_height = setting_int(settings, "max_video_height", 1080)
    default_quality = setting_int(settings, "default_user_quality", 1080)
    if max_height and default_quality > max_height:
        default_quality = max_height
    return {
        **settings,
        "download_retention_minutes": setting_int(settings, "download_retention_minutes", 60),
        "watch_extend_minutes": setting_int(settings, "watch_extend_minutes", 240),
        "extend_expiry_on_watch": setting_bool(settings, "extend_expiry_on_watch"),
        "max_single_file_gb": settings.get("max_single_file_gb", "4"),
        "max_download_dir_gb": settings.get("max_download_dir_gb", "18"),
        "min_free_disk_gb": settings.get("min_free_disk_gb", "5"),
        "max_video_height": max_height,
        "allow_unlimited_file_size": setting_bool(settings, "allow_unlimited_file_size"),
        "allow_unlimited_download_dir": setting_bool(settings, "allow_unlimited_download_dir"),
        "allow_unlimited_quality": setting_bool(settings, "allow_unlimited_quality"),
        "user_quality_selection_enabled": setting_bool(settings, "user_quality_selection_enabled"),
        "default_user_quality": default_quality,
        "experimental_proxy_download_enabled": setting_bool(settings, "experimental_proxy_download_enabled"),
        "experimental_proxy_max_file_gb": settings.get("experimental_proxy_max_file_gb", "2"),
        "experimental_proxy_max_duration_minutes": setting_int(settings, "experimental_proxy_max_duration_minutes", 30),
    }


def allowed_quality_options(settings: dict[str, str]) -> list[int]:
    if setting_bool(settings, "allow_unlimited_quality") or setting_int(settings, "max_video_height", 1080) == 0:
        return QUALITY_HEIGHTS[:]
    max_height = setting_int(settings, "max_video_height", 1080)
    return [height for height in QUALITY_HEIGHTS if height <= max_height]


def _rename_to_backup(path: Path) -> None:
    if not path.exists():
        return
    stamp = now_utc().strftime("%Y%m%d_%H%M%S")
    target = MIGRATION_BACKUP_DIR / f"{path.name}.{stamp}.bak"
    shutil.move(str(path), str(target))


def _migrate_json_to_sqlite() -> dict[str, int]:
    users_count_row = _db_fetchone("SELECT COUNT(*) AS cnt FROM users")
    if (users_count_row or {}).get("cnt", 0) > 0:
        return {"users": 0, "sessions": 0, "history": 0}

    users_data = read_json_file(USERS_FILE, {})
    sessions_data = read_json_file(SESSIONS_FILE, {})
    history_files = list(HISTORY_DIR.glob("history_*.json"))

    if not users_data and not sessions_data and not history_files:
        return {"users": 0, "sessions": 0, "history": 0}

    migrated_users = 0
    migrated_sessions = 0
    migrated_history = 0

    with db_connect() as conn:
        conn.execute("BEGIN")

        for user_id, meta in users_data.items():
            access_type = "admin" if meta.get("is_admin") else "universal"
            conn.execute(
                """
                INSERT OR IGNORE INTO users (
                  user_id, created_at, last_seen_at, last_activity_at, last_download_at,
                  is_admin, is_disabled, access_type, invite_id, cookie_file, cookie_uploaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, NULL, ?, ?)
                """,
                (
                    user_id,
                    meta.get("created_at") or iso(now_utc()),
                    meta.get("last_seen_at"),
                    meta.get("last_seen_at"),
                    meta.get("last_download_at"),
                    1 if meta.get("is_admin") else 0,
                    access_type,
                    meta.get("cookie_file"),
                    meta.get("cookie_uploaded_at"),
                ),
            )
            migrated_users += 1

        for session_id, meta in sessions_data.items():
            user_id = meta.get("user_id")
            if not user_id:
                continue
            conn.execute(
                """
                INSERT OR IGNORE INTO sessions (session_id, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    session_id,
                    user_id,
                    meta.get("created_at") or iso(now_utc()),
                    meta.get("expires_at") or iso(now_utc()),
                ),
            )
            migrated_sessions += 1

        for path in history_files:
            match = re.match(r"history_(.+)\.json$", path.name)
            if not match:
                continue
            user_id = match.group(1)
            items = read_json_file(path, [])
            if not isinstance(items, list):
                continue
            for item in items:
                conn.execute(
                    """
                    INSERT INTO download_history (
                      user_id, created_at, title, mode, status, error, source_url, download_url, filename
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        item.get("created_at") or iso(now_utc()),
                        item.get("title") or "Видео",
                        item.get("mode"),
                        item.get("status") or "done",
                        item.get("error"),
                        item.get("source_url"),
                        item.get("download_url"),
                        item.get("filename"),
                    ),
                )
                migrated_history += 1

        conn.commit()

    if USERS_FILE.exists():
        _rename_to_backup(USERS_FILE)
    if SESSIONS_FILE.exists():
        _rename_to_backup(SESSIONS_FILE)
    if HISTORY_DIR.exists():
        stamp = now_utc().strftime("%Y%m%d_%H%M%S")
        target_dir = MIGRATION_BACKUP_DIR / f"history_{stamp}"
        if target_dir.exists():
            shutil.rmtree(target_dir, ignore_errors=True)
        shutil.move(str(HISTORY_DIR), str(target_dir))
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)

    logger.info(
        "JSON migration finished: users=%s sessions=%s history=%s",
        migrated_users,
        migrated_sessions,
        migrated_history,
    )
    return {"users": migrated_users, "sessions": migrated_sessions, "history": migrated_history}


async def migrate_json_to_sqlite() -> dict[str, int]:
    async with db_write_lock:
        return await asyncio.to_thread(_migrate_json_to_sqlite)


async def create_user(
    *,
    is_admin: bool,
    access_type: str,
    invite_id: int | None = None,
) -> str:
    user_id = "u_" + secrets.token_hex(8)
    now_iso = iso(now_utc())
    await db_execute(
        """
        INSERT INTO users (
          user_id, created_at, last_seen_at, last_activity_at, last_download_at,
          is_admin, is_disabled, access_type, invite_id, cookie_file, cookie_uploaded_at
        ) VALUES (?, ?, ?, ?, NULL, ?, 0, ?, ?, NULL, NULL)
        """,
        (
            user_id,
            now_iso,
            now_iso,
            now_iso,
            1 if is_admin else 0,
            access_type,
            invite_id,
        ),
    )
    return user_id


async def get_user_by_id(user_id: str) -> dict[str, Any] | None:
    return await db_fetchone("SELECT * FROM users WHERE user_id = ?", (user_id,))


async def mark_user_seen(user_id: str, *, touch_activity: bool = False) -> None:
    now_iso = iso(now_utc())
    if touch_activity:
        await db_execute(
            "UPDATE users SET last_seen_at = ?, last_activity_at = ? WHERE user_id = ?",
            (now_iso, now_iso, user_id),
        )
    else:
        await db_execute(
            "UPDATE users SET last_seen_at = ? WHERE user_id = ?",
            (now_iso, user_id),
        )


async def create_session_for_user(user_id: str) -> str:
    session_id = "sess_" + secrets.token_hex(16)
    created = now_utc()
    await db_execute(
        """
        INSERT INTO sessions (session_id, user_id, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (
            session_id,
            user_id,
            iso(created),
            iso(created + dt.timedelta(seconds=WEB_SESSION_MAX_AGE)),
        ),
    )
    return session_id


async def delete_session(session_id: str) -> None:
    await db_execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))


async def delete_sessions_for_user(user_id: str) -> None:
    await db_execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))


async def get_current_user_id(request: Request, *, require_auth: bool = True) -> str | None:
    session_id = request.cookies.get(WEB_COOKIE_SESSION)
    if not session_id:
        if require_auth:
            raise HTTPException(status_code=401, detail="Требуется вход")
        return None

    session = await db_fetchone(
        """
        SELECT s.session_id, s.user_id, s.expires_at, u.is_disabled
        FROM sessions s
        JOIN users u ON u.user_id = s.user_id
        WHERE s.session_id = ?
        """,
        (session_id,),
    )
    if not session:
        if require_auth:
            raise HTTPException(status_code=401, detail="Требуется вход")
        return None

    expires_at = from_iso(session.get("expires_at"))
    if not expires_at or expires_at <= now_utc():
        await delete_session(session_id)
        if require_auth:
            raise HTTPException(status_code=401, detail="Сессия истекла")
        return None

    user_id = session.get("user_id")
    user = await get_user_by_id(user_id)
    if not user:
        await delete_session(session_id)
        if require_auth:
            raise HTTPException(status_code=401, detail="Пользователь не найден")
        return None

    if user.get("is_disabled"):
        await delete_sessions_for_user(user_id)
        if require_auth:
            raise HTTPException(status_code=403, detail="Доступ отключён")
        return None

    await mark_user_seen(user_id, touch_activity=False)
    return user_id


async def get_or_create_browser_user_id(request: Request, *, is_admin: bool = False) -> tuple[str, bool]:
    uid_cookie = request.cookies.get(WEB_COOKIE_UID)
    existing_user_id = parse_uid_cookie_value(uid_cookie)

    if existing_user_id:
        user = await get_user_by_id(existing_user_id)
        if user and not user.get("is_disabled"):
            if is_admin and not user.get("is_admin"):
                await db_execute(
                    "UPDATE users SET is_admin = 1, access_type = 'admin', last_seen_at = ? WHERE user_id = ?",
                    (iso(now_utc()), existing_user_id),
                )
            return existing_user_id, False

    access_type = "admin" if is_admin else "universal"
    user_id = await create_user(is_admin=is_admin, access_type=access_type)
    return user_id, True


async def append_history(user_id: str, record: dict[str, Any]) -> int:
    return await db_execute_insert(
        """
        INSERT INTO download_history (
          user_id, created_at, title, mode, status, error, source_url, download_url, filename
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            record.get("created_at") or iso(now_utc()),
            record.get("title") or "Видео",
            record.get("mode"),
            record.get("status") or "done",
            record.get("error"),
            record.get("source_url"),
            record.get("download_url"),
            record.get("filename"),
        ),
    )

async def get_history(user_id: str) -> list[dict[str, Any]]:
    return await db_fetchall(
        """
        SELECT created_at, title, mode, status, error, source_url, download_url, filename
        FROM download_history
        WHERE user_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 20
        """,
        (user_id,),
    )


async def update_last_download_at(user_id: str) -> None:
    await db_execute("UPDATE users SET last_download_at = ? WHERE user_id = ?", (iso(now_utc()), user_id))


def sign_value(payload: str) -> str:
    sig = hmac.new(WEB_SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def verify_signed_value(value: str | None) -> str | None:
    if not value or "." not in value:
        return None
    payload, sig = value.rsplit(".", 1)
    expected = hmac.new(WEB_SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        return None
    return payload


def make_uid_cookie_value(user_id: str) -> str:
    return sign_value(user_id)


def parse_uid_cookie_value(value: str | None) -> str | None:
    return verify_signed_value(value)


def get_cookie_file(user_id: str) -> Path | None:
    path = COOKIES_PATH / f"cookies_{user_id}.txt"
    return path if path.exists() else None


def get_admin_cookie_file() -> Path | None:
    path = COOKIES_PATH / ADMIN_COOKIES_FILE
    return path if path.exists() else None


def get_file_mtime_iso(path: Path | None) -> str | None:
    if not path or not path.exists():
        return None
    try:
        return iso(dt.datetime.fromtimestamp(path.stat().st_mtime, dt.timezone.utc))
    except Exception:
        return None


def inspect_cookie_file(path: Path | None) -> dict[str, str]:
    if not path or not path.exists():
        return {
            "status_class": "warn",
            "status_text": "Файл cookies не загружен",
        }

    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        logger.exception("Failed to read cookie file %s", path)
        return {
            "status_class": "error",
            "status_text": "Файл повреждён",
        }

    valid_lines = 0
    active_target_cookies = 0
    now_ts = int(time.time())
    target_domains = ("youtube.com", "google.com", "googlevideo.com", "youtu.be")

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = raw_line.split("\t")
        if len(parts) < 7:
            continue

        valid_lines += 1

        domain = parts[0].strip().lower()
        expires_raw = parts[4].strip()

        try:
            expires_at = int(expires_raw)
        except Exception:
            expires_at = 0

        if any(target in domain for target in target_domains):
            if expires_at == 0 or expires_at > now_ts:
                active_target_cookies += 1

    if valid_lines == 0:
        return {
            "status_class": "error",
            "status_text": "Файл повреждён",
        }

    if active_target_cookies > 0:
        return {
            "status_class": "ok",
            "status_text": "Активен",
        }

    return {
        "status_class": "warn",
        "status_text": "Нужен свежий файл",
    }


def build_effective_cookie_state(user_id: str, user_meta: dict[str, Any]) -> dict[str, Any]:
    user_cookie = get_cookie_file(user_id)
    if user_cookie:
        state = inspect_cookie_file(user_cookie)
        return {
            "source": "user",
            "filename": user_cookie.name,
            "uploaded_at": user_meta.get("cookie_uploaded_at"),
            "status_class": state["status_class"],
            "status_text": state["status_text"],
            "helper_text": "Используется ваш cookies файл",
        }

    admin_cookie = get_admin_cookie_file()
    if admin_cookie:
        state = inspect_cookie_file(admin_cookie)
        return {
            "source": "admin",
            "filename": admin_cookie.name,
            "uploaded_at": get_file_mtime_iso(admin_cookie),
            "status_class": state["status_class"],
            "status_text": state["status_text"],
            "helper_text": "Используется общий cookies администратора",
        }

    return {
        "source": None,
        "filename": None,
        "uploaded_at": None,
        "status_class": "warn",
        "status_text": "Файл cookies не загружен",
        "helper_text": "Поддерживается файл cookies.txt в формате Netscape.",
    }


def build_admin_cookie_state() -> dict[str, Any]:
    admin_cookie = get_admin_cookie_file()
    if admin_cookie:
        state = inspect_cookie_file(admin_cookie)
        return {
            "source": "admin",
            "filename": admin_cookie.name,
            "uploaded_at": get_file_mtime_iso(admin_cookie),
            "status_class": state["status_class"],
            "status_text": state["status_text"],
            "helper_text": "Этот файл используется по умолчанию для всех пользователей без личного cookies",
        }

    return {
        "source": None,
        "filename": None,
        "uploaded_at": None,
        "status_class": "warn",
        "status_text": "Общий cookies не загружен",
        "helper_text": "Здесь можно загрузить общий cookies.txt для YouTube.",
    }


def sanitize_filename(title: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]', "", title)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > 150:
        hash_part = hashlib.md5(title.encode()).hexdigest()[:8]
        name = name[:140] + "_" + hash_part
    return name or "video"


def clean_youtube_url(url: str) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    allowed_params = ["v", "t", "list"]
    new_query = {k: v for k, v in query.items() if k in allowed_params}
    clean_query = "&".join([f"{k}={v[0]}" for k, v in new_query.items()])
    return urlunparse(parsed._replace(query=clean_query))


def detect_node_path() -> str | None:
    p = shutil.which("node")
    if p:
        return p
    for cand in ("/usr/bin/node", "/usr/local/bin/node", "/snap/bin/node"):
        if os.path.exists(cand):
            return cand
    return None


def fmt_size(size_bytes: int | float | None) -> str:
    if not size_bytes:
        return ""
    value = float(size_bytes)
    units = ["Б", "КБ", "МБ", "ГБ", "ТБ"]
    idx = 0
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024
        idx += 1
    if idx == 0:
        return f"{int(value)} {units[idx]}"
    return f"{value:.1f} {units[idx]}"


def fmt_speed(speed_bytes_per_sec: float | int | None) -> str:
    if not speed_bytes_per_sec:
        return ""
    return f"{fmt_size(speed_bytes_per_sec)}/с"


def fmt_eta(seconds: float | int | None) -> str:
    if seconds is None:
        return ""
    try:
        sec = max(0, int(seconds))
    except Exception:
        return ""

    hours, remainder = divmod(sec, 3600)
    minutes, secs = divmod(remainder, 60)

    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def guess_mime_type(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == ".mp4":
        return "video/mp4"
    if ext == ".webm":
        return "video/webm"
    if ext == ".mkv":
        return "video/x-matroska"
    if ext == ".mp3":
        return "audio/mpeg"
    if ext == ".m4a":
        return "audio/mp4"
    if ext == ".jpg" or ext == ".jpeg":
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    return "application/octet-stream"


def get_download_dir_usage_bytes() -> int:
    total = 0
    for path in DOWNLOAD_PATH.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            continue
    return total


def get_disk_stats() -> dict[str, Any]:
    usage = shutil.disk_usage(DOWNLOAD_PATH)
    download_usage = get_download_dir_usage_bytes()
    return {
        "disk_total": usage.total,
        "disk_used": usage.used,
        "disk_free": usage.free,
        "download_usage": download_usage,
        "disk_total_text": fmt_size(usage.total),
        "disk_used_text": fmt_size(usage.used),
        "disk_free_text": fmt_size(usage.free),
        "download_usage_text": fmt_size(download_usage),
    }


async def enforce_storage_limits(settings: dict[str, str]) -> None:
    stats = await asyncio.to_thread(get_disk_stats)
    if stats["disk_free"] < setting_gb_to_bytes(settings, "min_free_disk_gb", Decimal("5")):
        raise HTTPException(status_code=507, detail="Недостаточно свободного места на сервере. Попробуйте позже.")

    if not setting_bool(settings, "allow_unlimited_download_dir"):
        max_dir = setting_gb_to_bytes(settings, "max_download_dir_gb", Decimal("18"))
        if stats["download_usage"] >= max_dir:
            raise HTTPException(status_code=507, detail="Квота каталога загрузок исчерпана. Попробуйте позже.")


def build_quality_label(mode: str, format_id: str | None, requested_height: int | None = None) -> str:
    if mode == "audio":
        return "MP3"
    if requested_height:
        return f"{requested_height}p"
    if mode == "safe":
        return "MP4"
    if mode == "bestq":
        return "Лучшее"
    if mode == "any":
        return "Любой"
    if mode == "pick":
        return f"Формат {format_id}" if format_id else "Выбранный формат"
    return mode


def bytes_limit_error(max_bytes: int) -> str:
    return f"Файл больше лимита, заданного администратором ({fmt_size(max_bytes)}). Выбери качество ниже."


def estimate_info_size_bytes(info: dict[str, Any] | None) -> int:
    if not isinstance(info, dict):
        return 0

    requested_formats = info.get("requested_formats")
    if isinstance(requested_formats, list) and requested_formats:
        total = 0
        has_known_size = False
        for item in requested_formats:
            if not isinstance(item, dict):
                continue
            size = int(item.get("filesize") or item.get("filesize_approx") or 0)
            if size > 0:
                total += size
                has_known_size = True
        if has_known_size:
            return total

    requested_downloads = info.get("requested_downloads")
    if isinstance(requested_downloads, list) and requested_downloads:
        total = 0
        has_known_size = False
        for item in requested_downloads:
            if not isinstance(item, dict):
                continue
            size = int(item.get("filesize") or item.get("filesize_approx") or item.get("total_bytes") or item.get("total_bytes_estimate") or 0)
            if size > 0:
                total += size
                has_known_size = True
        if has_known_size:
            return total

    return int(info.get("filesize") or info.get("filesize_approx") or 0)


def enforce_single_file_size_limit_by_value(size_bytes: int, max_bytes: int | None) -> None:
    if max_bytes and size_bytes and size_bytes > max_bytes:
        raise RuntimeError(bytes_limit_error(max_bytes))


def enforce_single_file_size_limit_by_info(info: dict[str, Any] | None, max_bytes: int | None) -> None:
    enforce_single_file_size_limit_by_value(estimate_info_size_bytes(info), max_bytes)


async def register_downloaded_file(
    *,
    user_id: str,
    source_url: str,
    stored_filename: str,
    file_path: Path,
    quality_label: str | None,
) -> dict[str, Any]:
    settings = await get_settings()
    created = now_utc()
    expires = created + dt.timedelta(minutes=setting_int(settings, "download_retention_minutes", 60))
    token = secrets.token_urlsafe(24)
    size = file_path.stat().st_size if file_path.exists() else 0
    mime_type = guess_mime_type(file_path)

    file_id = await db_execute_insert(
        """
        INSERT INTO downloaded_files (
          user_id, public_token, source_url, stored_filename, file_path, file_size,
          mime_type, quality_label, created_at, expires_at, last_accessed_at,
          access_count, deleted_at, delete_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, NULL, NULL)
        """,
        (
            user_id,
            token,
            source_url,
            stored_filename,
            str(file_path),
            size,
            mime_type,
            quality_label,
            iso(created),
            iso(expires),
        ),
    )

    row = await db_fetchone("SELECT * FROM downloaded_files WHERE file_id = ?", (file_id,))
    if not row:
        raise RuntimeError("Не удалось зарегистрировать скачанный файл")
    return row


async def get_file_by_token(token: str) -> dict[str, Any] | None:
    return await db_fetchone("SELECT * FROM downloaded_files WHERE public_token = ?", (token,))


def file_public_links(request: Request, token: str) -> dict[str, str]:
    return {
        "download_url": build_public_url(request, f"/media/download/{quote(token)}"),
        "watch_url": build_public_url(request, f"/media/watch/{quote(token)}"),
    }


async def mark_file_accessed(file_id: int, *, extend_expiry: bool) -> None:
    settings = await get_settings()
    now = now_utc()
    expires = None
    if extend_expiry and setting_bool(settings, "extend_expiry_on_watch"):
        expires = iso(now + dt.timedelta(minutes=setting_int(settings, "watch_extend_minutes", 240)))

    if expires:
        await db_execute(
            """
            UPDATE downloaded_files
            SET last_accessed_at = ?, access_count = access_count + 1, expires_at = ?
            WHERE file_id = ?
            """,
            (iso(now), expires, file_id),
        )
    else:
        await db_execute(
            "UPDATE downloaded_files SET last_accessed_at = ?, access_count = access_count + 1 WHERE file_id = ?",
            (iso(now), file_id),
        )


async def delete_downloaded_file(file_id: int, reason: str) -> bool:
    row = await db_fetchone("SELECT * FROM downloaded_files WHERE file_id = ?", (file_id,))
    if not row or row.get("deleted_at"):
        return False
    path = Path(row["file_path"])
    try:
        if path.exists() and path.is_file():
            path.unlink()
    except Exception:
        logger.exception("Failed to delete downloaded file file_id=%s path=%s", file_id, path)
        raise
    await db_execute(
        "UPDATE downloaded_files SET deleted_at = ?, delete_reason = ? WHERE file_id = ?",
        (iso(now_utc()), reason, file_id),
    )
    return True


async def cleanup_expired_downloaded_files() -> int:
    rows = await db_fetchall(
        """
        SELECT file_id
        FROM downloaded_files
        WHERE deleted_at IS NULL
          AND expires_at <= ?
        """,
        (iso(now_utc()),),
    )
    removed = 0
    for row in rows:
        try:
            if await delete_downloaded_file(int(row["file_id"]), "expired"):
                removed += 1
        except Exception:
            logger.exception("Failed to cleanup expired downloaded file_id=%s", row.get("file_id"))
    return removed


async def cleanup_missing_downloaded_files() -> int:
    rows = await db_fetchall("SELECT file_id, file_path FROM downloaded_files WHERE deleted_at IS NULL")
    marked = 0
    for row in rows:
        path = Path(row["file_path"])
        if not path.exists():
            await db_execute(
                "UPDATE downloaded_files SET deleted_at = ?, delete_reason = ? WHERE file_id = ?",
                (iso(now_utc()), "missing_on_disk", row["file_id"]),
            )
            marked += 1
    return marked


async def cleanup_stale_temp_files(max_age_minutes: int = 180) -> int:
    cutoff = time.time() - max_age_minutes * 60
    removed = 0
    for path in DOWNLOAD_PATH.glob("webtmp_*"):
        try:
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
                removed += 1
        except Exception:
            logger.exception("Failed to delete stale temp file %s", path)
    return removed


async def list_active_downloaded_files(request: Request | None = None) -> list[dict[str, Any]]:
    rows = await db_fetchall(
        """
        SELECT
          f.file_id, f.user_id, f.public_token, f.stored_filename, f.file_size,
          f.mime_type, f.quality_label, f.created_at, f.expires_at, f.last_accessed_at,
          f.access_count, i.label AS access_label
        FROM downloaded_files f
        LEFT JOIN users u ON u.user_id = f.user_id
        LEFT JOIN invite_links i ON i.invite_id = u.invite_id
        WHERE f.deleted_at IS NULL
        ORDER BY f.created_at DESC, f.file_id DESC
        """
    )
    now = now_utc()
    result: list[dict[str, Any]] = []
    for row in rows:
        expires = from_iso(row.get("expires_at"))
        left_seconds = max(0, int((expires - now).total_seconds())) if expires else 0
        item = dict(row)
        item["file_size_text"] = fmt_size(item.get("file_size"))
        item["time_left_seconds"] = left_seconds
        item["time_left_text"] = fmt_eta(left_seconds)
        item["user_label"] = item.get("access_label") or item.get("user_id")
        if request is not None:
            item.update(file_public_links(request, item["public_token"]))
        result.append(item)
    return result


def build_base_ydl_opts(user_id: str, *, skip_download: bool, quiet: bool, task_id: str | None = None) -> dict[str, Any]:
    user_cookie = get_cookie_file(user_id)
    cookie_file = user_cookie or get_admin_cookie_file()
    node_path = detect_node_path()
    prefix = f"webtmp_{user_id}_"
    if task_id:
        prefix += f"{task_id}_"

    opts: dict[str, Any] = {
        "outtmpl": str(DOWNLOAD_PATH / f"{prefix}%(id)s.%(ext)s"),
        "paths": {"home": str(DOWNLOAD_PATH)},
        "noplaylist": True,
        "skip_download": skip_download,
        "quiet": quiet,
        "no_warnings": quiet,
        "nocheckcertificate": True,
        "geo_bypass": True,
        "retries": 20,
        "fragment_retries": 20,
        "socket_timeout": 30,
        "http_chunk_size": 10 * 1024 * 1024,
        "concurrent_fragment_downloads": 1,
        "continuedl": True,
        "force_ipv4": True,
        "merge_output_format": "mp4",
        "ignore_no_formats_error": skip_download,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            )
        },
        "remote_components": ["ejs:github"],
        "progress_hooks": [],
        "postprocessor_hooks": [],
    }

    if node_path:
        opts["js_runtimes"] = {"node": {"path": node_path}}
    else:
        opts["js_runtimes"] = {}

    if cookie_file:
        opts["cookiefile"] = str(cookie_file)

    if DEBUG_YTDLP:
        opts["verbose"] = True

    return opts


def get_format_string(mode: str, format_id: str | None, max_height: int = 0) -> str:
    """
    Build yt-dlp format selector.

    For horizontal videos:
      720p  -> 1280x720
      1080p -> 1920x1080

    For vertical videos and YouTube Shorts:
      720p  -> 720x1280
      1080p -> 1080x1920

    The limit is applied to the largest frame side, so vertical Shorts
    are not rejected by a simple height<=720 filter.
    """
    if max_height and max_height > 0:
        max_side = int(max_height) * 2
        video_filter = f"[width<={max_side}][height<={max_side}]"
    else:
        video_filter = ""

    if mode == "pick" and format_id:
        return f"{format_id}+ba/{format_id}/b"

    if video_filter:
        return f"bv*{video_filter}+ba/b{video_filter}/bv*+ba/b"

    return "bv*+ba/b"


def ydl_extract(url: str, opts: dict[str, Any], *, download: bool):
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=download)


def human_download_error(exc: Exception) -> str:
    raw = str(exc)
    lower = raw.lower()

    if "no video formats found" in lower or "requested format is not available" in lower:
        return (
            "Не удалось найти подходящий видеоформат. Попробуй выбрать другое качество "
            "или загрузить cookies.txt для YouTube в формате Netscape. Если ссылка публичная, "
            "обнови yt-dlp и yt-dlp-ejs через скрипт установки/обновления."
        )

    if "file is larger than max-filesize" in lower or "larger than max-filesize" in lower:
        return "Файл больше лимита, заданного администратором. Выбери качество ниже."

    return raw


def find_downloaded_file(info: dict[str, Any]) -> str | None:
    def existing(path: str | None) -> str | None:
        if path and os.path.exists(path):
            return path
        return None

    rds = info.get("requested_downloads") or []
    if isinstance(rds, list):
        for item in rds:
            for key in ("filepath", "filename", "_filename"):
                path = existing(item.get(key))
                if path and not re.search(r"\.f\d+\.", os.path.basename(path)):
                    return path
        for item in rds:
            for key in ("filepath", "filename", "_filename"):
                path = existing(item.get(key))
                if path:
                    return path

    for key in ("filepath", "filename", "_filename"):
        path = existing(info.get(key))
        if path and not re.search(r"\.f\d+\.", os.path.basename(path)):
            return path
    for key in ("filepath", "filename", "_filename"):
        path = existing(info.get(key))
        if path:
            return path

    vid = info.get("id")
    if not vid:
        return None

    candidates = list(DOWNLOAD_PATH.glob(f"*{vid}.*"))
    candidates = [p for p in candidates if not str(p).endswith(".part")]
    if not candidates:
        return None

    final_candidates = [p for p in candidates if not re.search(r"\.f\d+\.", p.name)]
    if final_candidates:
        final_candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
        return str(final_candidates[0])

    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    return str(candidates[0])


async def cleanup_temp_files(task_id: str) -> None:
    pattern = f"*{task_id}*"
    for path in DOWNLOAD_PATH.glob(pattern):
        if path.is_file() and path.name.startswith("webtmp_"):
            try:
                path.unlink(missing_ok=True)
            except Exception:
                logger.exception("Failed to delete temp file %s", path)


async def analyze_url_for_user(user_id: str, url: str) -> dict[str, Any]:
    async with analysis_lock:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, sync_analyze_url, user_id, url)


def sync_analyze_url(user_id: str, url: str) -> dict[str, Any]:
    url = clean_youtube_url(url.strip())
    settings = _get_settings_sync()
    max_height = setting_int(settings, "max_video_height", 1080)
    allow_unlimited_quality = setting_bool(settings, "allow_unlimited_quality") or max_height == 0
    opts_info = build_base_ydl_opts(user_id, skip_download=True, quiet=True)
    info = ydl_extract(url, opts_info, download=False)

    title = info.get("title") or "Видео"
    thumbnail_url = info.get("thumbnail")
    formats = info.get("formats") or []

    available = []
    seen_labels: set[str] = set()

    for f in formats:
        fid = f.get("format_id")
        ext = (f.get("ext") or "").lower()
        height = f.get("height")
        vcodec = f.get("vcodec")

        if not fid:
            continue
        if ext == "mhtml" or "storyboard" in str(fid).lower():
            continue
        if not vcodec or vcodec == "none":
            continue

        if not height:
            match = re.search(r"(\d{3,4})p", f.get("format", "") or "")
            if match:
                height = int(match.group(1))

        if not height:
            continue
        if not allow_unlimited_quality and height > max_height:
            continue

        size = f.get("filesize") or f.get("filesize_approx") or 0
        label = f"{height}p {ext}"
        if fmt_size(size):
            label += f" ({fmt_size(size)})"

        if label in seen_labels:
            continue

        seen_labels.add(label)
        available.append(
            {
                "label": label,
                "format_id": fid,
                "height": height,
                "ext": ext,
                "filesize": size,
            }
        )

    available.sort(key=lambda x: x.get("height", 0), reverse=True)

    return {
        "title": title,
        "url": url,
        "thumbnail_url": thumbnail_url,
        "formats": available,
        "settings": {
            "quality_options": allowed_quality_options(settings),
            "default_user_quality": setting_int(settings, "default_user_quality", 1080),
            "experimental_proxy_download_enabled": setting_bool(settings, "experimental_proxy_download_enabled"),
        },
    }


async def update_task(task_id: str, **kwargs: Any) -> None:
    task = active_tasks.get(task_id)
    if not task:
        return
    task.update(kwargs)
    task["updated_at"] = iso(now_utc())


def schedule_task_update(loop: asyncio.AbstractEventLoop, task_id: str, **kwargs: Any) -> None:
    def runner() -> None:
        asyncio.create_task(update_task(task_id, **kwargs))

    loop.call_soon_threadsafe(runner)


def ensure_task_not_cancelled(task_id: str) -> None:
    task = active_tasks.get(task_id)
    if task and task.get("cancel_requested"):
        raise DownloadCancelledError("Скачивание отменено.")


async def cancel_download_task(task_id: str, requester_user_id: str, *, is_admin: bool = False) -> dict[str, Any]:
    task = active_tasks.get(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Задача не найдена.")
    if not is_admin and task.get("user_id") != requester_user_id:
        raise HTTPException(status_code=404, detail="Задача не найдена.")
    if task.get("done"):
        return task

    async with queue_state_lock:
        is_queued = task_id in queued_task_ids
        if is_queued:
            queued_task_ids.remove(task_id)

    if is_queued:
        await update_task(
            task_id,
            status="cancelled",
            status_label="Отменено",
            detail="Скачивание отменено",
            error="Скачивание отменено.",
            cancel_requested=True,
            cancelled_by="admin" if is_admin else "user",
            done=True,
            queue_position=None,
        )
        await refresh_queue_positions()
    else:
        await update_task(
            task_id,
            status="cancelling",
            status_label="Отмена",
            detail="Останавливаю скачивание. Это может занять несколько секунд.",
            cancel_requested=True,
            cancelled_by="admin" if is_admin else "user",
            queue_position=None,
        )

    return active_tasks.get(task_id) or task


async def refresh_queue_positions() -> None:
    async with queue_state_lock:
        for idx, queued_id in enumerate(queued_task_ids, start=1):
            task = active_tasks.get(queued_id)
            if not task or task.get("done"):
                continue
            task["queue_position"] = idx
            task["status"] = "queued"
            task["status_label"] = "В очереди"
            task["detail"] = f"Позиция в очереди: {idx}"
            task["updated_at"] = iso(now_utc())


async def add_to_queue(task_id: str) -> int:
    async with queue_state_lock:
        queued_task_ids.append(task_id)
        position = len(queued_task_ids)
    await refresh_queue_positions()
    await task_queue.put(task_id)
    return position


async def mark_task_started(task_id: str) -> None:
    async with queue_state_lock:
        if task_id in queued_task_ids:
            queued_task_ids.remove(task_id)
    await refresh_queue_positions()
    await update_task(
        task_id,
        queue_position=None,
        status="preparing",
        status_label="Подготовка",
        detail="Подготавливаю запуск задачи",
    )


def init_task(user_id: str, url: str) -> dict[str, Any]:
    task_id = secrets.token_hex(8)
    task = {
        "task_id": task_id,
        "user_id": user_id,
        "url": url,
        "created_at": iso(now_utc()),
        "updated_at": iso(now_utc()),
        "status": "queued",
        "status_label": "В очереди",
        "detail": "Задача создана",
        "queue_position": None,
        "mode": None,
        "title": None,
        "download_url": None,
        "filename": None,
        "error": None,
        "done": False,
        "cancel_requested": False,
        "cancelled_by": None,
        "thumbnail_url": None,
    }
    active_tasks[task_id] = task
    return task


async def count_user_active_tasks(user_id: str) -> int:
    total = 0
    for task in active_tasks.values():
        if task.get("user_id") == user_id and not task.get("done"):
            total += 1
    return total


async def start_download_task(
    user_id: str,
    url: str,
    mode: str,
    format_id: str | None = None,
    title_hint: str | None = None,
    requested_height: int | None = None,
) -> dict[str, Any]:
    if await count_user_active_tasks(user_id) >= MAX_ACTIVE_TASKS_PER_USER:
        raise HTTPException(status_code=429, detail="У вас уже есть активная задача. Дождитесь завершения.")

    settings = await get_settings()
    await enforce_storage_limits(settings)

    task = init_task(user_id, url)
    task["mode"] = mode
    task["title"] = title_hint
    task["format_id"] = format_id
    task["requested_height"] = requested_height

    position = await add_to_queue(task["task_id"])
    await update_task(
        task["task_id"],
        queue_position=position,
        status="queued",
        status_label="В очереди",
        detail=f"Позиция в очереди: {position}",
    )
    return task


def sync_download_media(
    loop: asyncio.AbstractEventLoop,
    task_id: str,
    user_id: str,
    url: str,
    mode: str,
    format_id: str | None,
    title_hint: str | None,
    requested_height: int | None,
) -> dict[str, Any]:
    opts = build_base_ydl_opts(user_id, skip_download=False, quiet=False, task_id=task_id)
    settings = _get_settings_sync()
    max_file_bytes = None
    if not setting_bool(settings, "allow_unlimited_file_size"):
        max_file_bytes = setting_gb_to_bytes(settings, "max_single_file_gb", Decimal("4"))
        opts["max_filesize"] = max_file_bytes
    max_height = setting_int(settings, "max_video_height", 1080)
    if setting_bool(settings, "allow_unlimited_quality"):
        max_height = 0
    if requested_height:
        if max_height and requested_height > max_height:
            requested_height = max_height
        max_height = requested_height
    title = title_hint or "Видео"
    last_progress_emit = 0.0

    def progress_hook(data: dict[str, Any]) -> None:
        nonlocal last_progress_emit

        try:
            status = data.get("status")
            ensure_task_not_cancelled(task_id)
            now_monotonic = time.monotonic()

            if status == "downloading":
                if now_monotonic - last_progress_emit < 0.8:
                    return
                last_progress_emit = now_monotonic

                downloaded = data.get("downloaded_bytes") or 0
                total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
                enforce_single_file_size_limit_by_value(total, max_file_bytes)
                enforce_single_file_size_limit_by_value(downloaded, max_file_bytes)
                speed = data.get("speed")
                eta = data.get("eta")

                parts: list[str] = []

                if total:
                    percent = int((downloaded / total) * 100) if total > 0 else 0
                    parts.append(f"{percent}%")
                    parts.append(f"{fmt_size(downloaded)} / {fmt_size(total)}")
                elif downloaded:
                    parts.append(fmt_size(downloaded))

                speed_text = fmt_speed(speed)
                if speed_text:
                    parts.append(speed_text)

                eta_text = fmt_eta(eta)
                if eta_text:
                    parts.append(f"ETA {eta_text}")

                detail = " • ".join(parts) if parts else "Скачивание файла"

                schedule_task_update(
                    loop,
                    task_id,
                    status="downloading",
                    status_label="Скачивание",
                    detail=detail,
                )

            elif status == "finished":
                schedule_task_update(
                    loop,
                    task_id,
                    status="processing",
                    status_label="Обработка",
                    detail="Скачивание завершено, выполняю обработку ffmpeg",
                )
        except RuntimeError:
            raise
        except Exception:
            logger.exception("Progress hook error for task_id=%s", task_id)

    def postprocessor_hook(data: dict[str, Any]) -> None:
        try:
            status = data.get("status")
            ensure_task_not_cancelled(task_id)
            postprocessor = data.get("postprocessor") or "FFmpeg"

            if status in {"started", "processing"}:
                schedule_task_update(
                    loop,
                    task_id,
                    status="processing",
                    status_label="Обработка",
                    detail=f"Обработка {postprocessor}",
                )
            elif status == "finished":
                schedule_task_update(
                    loop,
                    task_id,
                    status="processing",
                    status_label="Обработка",
                    detail="Финальная подготовка файла",
                )
        except Exception:
            logger.exception("Postprocessor hook error for task_id=%s", task_id)

    opts["progress_hooks"] = [progress_hook]
    opts["postprocessor_hooks"] = [postprocessor_hook]

    if mode == "audio":
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ]
    else:
        if mode == "pick" and not format_id:
            raise RuntimeError("Не передан format_id.")
        opts["format"] = get_format_string(mode, format_id, max_height=max_height)

    cookiefile = opts.get("cookiefile")
    logger.info(
        "Downloading url=%s mode=%s format=%s cookies=%s",
        url,
        mode,
        opts.get("format"),
        Path(str(cookiefile)).name if cookiefile else "no",
    )

    def is_format_or_cookie_related_error(exc: Exception) -> bool:
        text = str(exc).lower()
        markers = (
            "requested format is not available",
            "no video formats found",
            "only images are available",
            "video formats found",
            "n challenge solving failed",
        )
        return any(marker in text for marker in markers)

    def make_no_cookie_opts(source_opts: dict[str, Any]) -> dict[str, Any]:
        new_opts = dict(source_opts)
        new_opts.pop("cookiefile", None)
        return new_opts

    def ydl_extract_checked(source_opts: dict[str, Any]) -> dict[str, Any]:
        ensure_task_not_cancelled(task_id)
        if max_file_bytes:
            check_opts = dict(source_opts)
            check_opts["skip_download"] = True
            check_opts["quiet"] = True
            check_opts["no_warnings"] = True
            check_opts["progress_hooks"] = []
            check_opts["postprocessor_hooks"] = []
            check_opts.pop("downloader", None)
            check_info = ydl_extract(url, check_opts, download=False)
            ensure_task_not_cancelled(task_id)
            enforce_single_file_size_limit_by_info(check_info, max_file_bytes)

        ensure_task_not_cancelled(task_id)
        downloaded_info = ydl_extract(url, source_opts, download=True)
        ensure_task_not_cancelled(task_id)
        enforce_single_file_size_limit_by_info(downloaded_info, max_file_bytes)
        return downloaded_info

    try:
        info = ydl_extract_checked(opts)
    except Exception as e1:
        logger.warning("Primary download failed. err=%s", e1)

        if "cookiefile" in opts and is_format_or_cookie_related_error(e1):
            logger.warning("Download failed with cookies, retry without cookies. err=%s", e1)
            schedule_task_update(
                loop,
                task_id,
                status="processing",
                status_label="Повторная попытка",
                detail="Cookies не подошли для этого видео, пробую скачать без cookies",
            )

            opts_no_cookie = make_no_cookie_opts(opts)
            try:
                info = ydl_extract_checked(opts_no_cookie)
            except Exception as e_no_cookie_1:
                logger.warning("No-cookie download failed, retry with ffmpeg downloader. err=%s", e_no_cookie_1)
                schedule_task_update(
                    loop,
                    task_id,
                    status="processing",
                    status_label="Повторная попытка",
                    detail="Пробую резервный способ скачивания без cookies",
                )

                opts_no_cookie_ff = make_no_cookie_opts(opts)
                opts_no_cookie_ff["downloader"] = "ffmpeg"

                try:
                    info = ydl_extract_checked(opts_no_cookie_ff)
                except Exception as e_no_cookie_2:
                    logger.warning("No-cookie fallback download failed. err=%s", e_no_cookie_2)
                    raise RuntimeError(human_download_error(e_no_cookie_2)) from e_no_cookie_2
        else:
            logger.warning("Primary download failed, retry with ffmpeg downloader. err=%s", e1)
            schedule_task_update(
                loop,
                task_id,
                status="processing",
                status_label="Повторная попытка",
                detail="Пробую резервный способ скачивания",
            )

            opts_ff = dict(opts)
            opts_ff["downloader"] = "ffmpeg"

            try:
                info = ydl_extract_checked(opts_ff)
            except Exception as e2:
                logger.warning("Fallback download failed. err=%s", e2)
                raise RuntimeError(human_download_error(e2)) from e2

    path = find_downloaded_file(info)
    if not path or not os.path.exists(path):
        raise RuntimeError("Файл не найден после скачивания.")

    title = info.get("title") or title
    thumbnail_url = info.get("thumbnail")
    ext = Path(path).suffix[1:] if Path(path).suffix else "bin"
    unique = f"{hashlib.md5((title + task_id).encode()).hexdigest()[:8]}_{int(time.time())}.{ext}"
    final = DOWNLOAD_PATH / unique
    os.replace(path, final)
    try:
        enforce_single_file_size_limit_by_value(final.stat().st_size, max_file_bytes)
    except Exception:
        final.unlink(missing_ok=True)
        raise

    return {
        "title": title,
        "filename": final.name,
        "file_path": str(final),
        "file_size": final.stat().st_size,
        "quality_label": build_quality_label(mode, format_id, requested_height),
        "thumbnail_url": thumbnail_url,
    }


async def download_worker(worker_index: int) -> None:
    logger.info("Download worker #%s started", worker_index)

    while True:
        task_id = await task_queue.get()
        try:
            task = active_tasks.get(task_id)
            if not task or task.get("done"):
                continue

            user = await get_user_by_id(task["user_id"])
            if not user or user.get("is_disabled"):
                await update_task(
                    task_id,
                    status="error",
                    status_label="Ошибка",
                    detail="Доступ пользователя отключён",
                    error="Доступ пользователя отключён",
                    done=True,
                    queue_position=None,
                )
                continue

            await mark_task_started(task_id)
            if task.get("cancel_requested"):
                raise DownloadCancelledError("Скачивание отменено.")

            user_id = task["user_id"]
            url = task["url"]
            mode = task["mode"]
            format_id = task.get("format_id")
            title_hint = task.get("title")
            requested_height = task.get("requested_height")

            loop = asyncio.get_running_loop()

            result = await loop.run_in_executor(
                None,
                sync_download_media,
                loop,
                task_id,
                user_id,
                url,
                mode,
                format_id,
                title_hint,
                requested_height,
            )

            if task.get("cancel_requested"):
                raise DownloadCancelledError("Скачивание отменено.")

            file_row = await register_downloaded_file(
                user_id=user_id,
                source_url=url,
                stored_filename=result["filename"],
                file_path=Path(result["file_path"]),
                quality_label=result.get("quality_label"),
            )
            media_download_url = f"{WEB_BASE_PATH}/media/download/{quote(file_row['public_token'])}"
            media_watch_url = f"{WEB_BASE_PATH}/media/watch/{quote(file_row['public_token'])}"

            await update_task(
                task_id,
                status="done",
                status_label="Готово",
                detail="Файл готов к скачиванию и просмотру",
                title=result["title"],
                filename=result["filename"],
                file_id=file_row["file_id"],
                download_url=media_download_url,
                watch_url=media_watch_url,
                thumbnail_url=result.get("thumbnail_url"),
                error=None,
                done=True,
                queue_position=None,
            )

            await update_last_download_at(user_id)
            history_id = await append_history(
                user_id,
                {
                    "created_at": iso(now_utc()),
                    "title": result["title"],
                    "mode": mode,
                    "status": "done",
                    "download_url": media_download_url,
                    "filename": result["filename"],
                    "source_url": url,
                },
            )
            await db_execute(
                "UPDATE downloaded_files SET history_id = ? WHERE file_id = ?",
                (history_id, file_row["file_id"]),
            )

        except DownloadCancelledError as exc:
            logger.info("Download task cancelled: %s", task_id)
            task = active_tasks.get(task_id)
            user_id = task.get("user_id") if task else None
            mode = task.get("mode") if task else None
            url = task.get("url") if task else None
            title_hint = task.get("title") if task else "Видео"

            await update_task(
                task_id,
                status="cancelled",
                status_label="Отменено",
                detail="Скачивание отменено",
                error=str(exc),
                done=True,
                queue_position=None,
            )

            if user_id and mode and url:
                await append_history(
                    user_id,
                    {
                        "created_at": iso(now_utc()),
                        "title": title_hint or "Видео",
                        "mode": mode,
                        "status": "cancelled",
                        "error": str(exc),
                        "source_url": url,
                    },
                )

        except Exception as exc:
            logger.exception("Download task failed: %s", task_id)
            task = active_tasks.get(task_id)
            user_id = task.get("user_id") if task else None
            mode = task.get("mode") if task else None
            url = task.get("url") if task else None
            title_hint = task.get("title") if task else "Видео"

            await update_task(
                task_id,
                status="error",
                status_label="Ошибка",
                detail=str(exc),
                error=str(exc),
                done=True,
                queue_position=None,
            )

            if user_id and mode and url:
                await append_history(
                    user_id,
                    {
                        "created_at": iso(now_utc()),
                        "title": title_hint or "Видео",
                        "mode": mode,
                        "status": "error",
                        "error": str(exc),
                        "source_url": url,
                    },
                )

        finally:
            await cleanup_temp_files(task_id)
            task_queue.task_done()


async def purge_old_finished_tasks() -> int:
    removed = 0
    now = now_utc()
    for task_id in list(active_tasks.keys()):
        task = active_tasks.get(task_id)
        if not task or not task.get("done"):
            continue
        updated_at = from_iso(task.get("updated_at")) or from_iso(task.get("created_at")) or now
        if now - updated_at > REQUEST_TTL:
            active_tasks.pop(task_id, None)
            removed += 1
    return removed


async def revoke_runtime_access_for_user(user_id: str) -> None:
    async with queue_state_lock:
        for task_id in list(queued_task_ids):
            task = active_tasks.get(task_id)
            if not task or task.get("user_id") != user_id:
                continue
            task["status"] = "error"
            task["status_label"] = "Ошибка"
            task["detail"] = "Доступ пользователя отключён"
            task["error"] = "Доступ пользователя отключён"
            task["done"] = True
            task["queue_position"] = None
            task["updated_at"] = iso(now_utc())
            queued_task_ids.remove(task_id)
    await refresh_queue_positions()


async def perform_cleanup() -> dict[str, int]:
    now = now_utc()
    removed_sessions = 0
    removed_universal_users = 0
    removed_revoked_invites = 0
    removed_finished_tasks = 0
    removed_expired_files = 0
    marked_missing_files = 0
    removed_stale_temp_files = 0

    expired_sessions = await db_fetchall(
        "SELECT session_id FROM sessions WHERE expires_at IS NULL OR expires_at <= ?",
        (iso(now),),
    )
    for item in expired_sessions:
        await db_execute("DELETE FROM sessions WHERE session_id = ?", (item["session_id"],))
        removed_sessions += 1

    revoked_invites = await db_fetchall(
        """
        SELECT invite_id, activated_user_id
        FROM invite_links
        WHERE revoked_at IS NOT NULL
          AND revoked_at <= ?
        """,
        (iso(now - USER_RETENTION),),
    )
    for item in revoked_invites:
        activated_user_id = item.get("activated_user_id")

        if activated_user_id:
            cookie_file = get_cookie_file(activated_user_id)
            if cookie_file:
                cookie_file.unlink(missing_ok=True)

            await revoke_runtime_access_for_user(activated_user_id)
            await db_execute("DELETE FROM users WHERE user_id = ?", (activated_user_id,))

        await db_execute("DELETE FROM invite_links WHERE invite_id = ?", (item["invite_id"],))
        removed_revoked_invites += 1

    old_users = await db_fetchall(
        """
        SELECT user_id, cookie_file
        FROM users
        WHERE is_admin = 0
          AND access_type = 'universal'
          AND (last_seen_at IS NULL OR last_seen_at <= ?)
        """,
        (iso(now - USER_RETENTION),),
    )
    for item in old_users:
        cookie_file = get_cookie_file(item["user_id"])
        if cookie_file:
            cookie_file.unlink(missing_ok=True)

        await revoke_runtime_access_for_user(item["user_id"])
        await db_execute("DELETE FROM users WHERE user_id = ?", (item["user_id"],))
        removed_universal_users += 1

    removed_finished_tasks = await purge_old_finished_tasks()
    removed_expired_files = await cleanup_expired_downloaded_files()
    marked_missing_files = await cleanup_missing_downloaded_files()
    removed_stale_temp_files = await cleanup_stale_temp_files()

    logger.info(
        "Cleanup finished: removed_universal_users=%s removed_revoked_invites=%s removed_sessions=%s removed_finished_tasks=%s removed_expired_files=%s marked_missing_files=%s removed_stale_temp_files=%s",
        removed_universal_users,
        removed_revoked_invites,
        removed_sessions,
        removed_finished_tasks,
        removed_expired_files,
        marked_missing_files,
        removed_stale_temp_files,
    )

    return {
        "removed_universal_users": removed_universal_users,
        "removed_revoked_invites": removed_revoked_invites,
        "removed_sessions": removed_sessions,
        "removed_finished_tasks": removed_finished_tasks,
        "removed_expired_files": removed_expired_files,
        "marked_missing_files": marked_missing_files,
        "removed_stale_temp_files": removed_stale_temp_files,
    }


async def cleanup_scheduler() -> None:
    await asyncio.sleep(2)
    while True:
        try:
            await perform_cleanup()
        except Exception:
            logger.exception("Scheduled cleanup failed")
        await asyncio.sleep(60)

def build_public_url(request: Request, path: str) -> str:
    base = PUBLIC_BASE_URL or str(request.base_url).rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{WEB_BASE_PATH}{path}"


async def create_invite_link(created_by_user_id: str, label: str | None) -> dict[str, Any]:
    token = secrets.token_urlsafe(24)
    invite_id = await db_execute_insert(
        """
        INSERT INTO invite_links (token, label, created_at, created_by_user_id, activated_user_id, activated_at, revoked_at, note)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL)
        """,
        (token, (label or "").strip() or None, iso(now_utc()), created_by_user_id),
    )
    invite = await db_fetchone("SELECT * FROM invite_links WHERE invite_id = ?", (invite_id,))
    if not invite:
        raise RuntimeError("Не удалось создать ссылку доступа")
    return invite


async def get_invite_by_token(token: str) -> dict[str, Any] | None:
    return await db_fetchone("SELECT * FROM invite_links WHERE token = ?", (token,))


async def get_invite_by_id(invite_id: int) -> dict[str, Any] | None:
    return await db_fetchone("SELECT * FROM invite_links WHERE invite_id = ?", (invite_id,))


async def activate_invite(invite_id: int) -> tuple[dict[str, Any], str]:
    async with db_write_lock:
        def _activate() -> tuple[dict[str, Any], str]:
            with db_connect() as conn:
                row = conn.execute("SELECT * FROM invite_links WHERE invite_id = ?", (invite_id,)).fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="Ссылка доступа не найдена")
                invite = row_to_dict(row)
                if invite.get("revoked_at"):
                    raise HTTPException(status_code=403, detail="Ссылка доступа отключена")
                if invite.get("activated_user_id"):
                    return invite, invite["activated_user_id"]

                user_id = "u_" + secrets.token_hex(8)
                now_iso = iso(now_utc())
                conn.execute(
                    """
                    INSERT INTO users (
                      user_id, created_at, last_seen_at, last_activity_at, last_download_at,
                      is_admin, is_disabled, access_type, invite_id, cookie_file, cookie_uploaded_at
                    ) VALUES (?, ?, ?, ?, NULL, 0, 0, 'invite', ?, NULL, NULL)
                    """,
                    (user_id, now_iso, now_iso, now_iso, invite_id),
                )
                conn.execute(
                    "UPDATE invite_links SET activated_user_id = ?, activated_at = ? WHERE invite_id = ?",
                    (user_id, now_iso, invite_id),
                )
                conn.commit()
                fresh_invite = conn.execute("SELECT * FROM invite_links WHERE invite_id = ?", (invite_id,)).fetchone()
                return row_to_dict(fresh_invite), user_id

        return await asyncio.to_thread(_activate)


async def update_invite_label(invite_id: int, label: str | None) -> None:
    await db_execute(
        "UPDATE invite_links SET label = ? WHERE invite_id = ?",
        (((label or "").strip() or None), invite_id),
    )


async def revoke_invite_access(invite_id: int) -> None:
    invite = await get_invite_by_id(invite_id)
    if not invite:
        raise HTTPException(status_code=404, detail="Ссылка доступа не найдена")

    now_iso = iso(now_utc())
    await db_execute("UPDATE invite_links SET revoked_at = ? WHERE invite_id = ?", (now_iso, invite_id))

    activated_user_id = invite.get("activated_user_id")
    if activated_user_id:
        await db_execute(
            "UPDATE users SET is_disabled = 1, last_seen_at = ? WHERE user_id = ?",
            (now_iso, activated_user_id),
        )
        await delete_sessions_for_user(activated_user_id)
        cookie_file = get_cookie_file(activated_user_id)
        if cookie_file:
            cookie_file.unlink(missing_ok=True)
        await revoke_runtime_access_for_user(activated_user_id)


def build_logged_out_url(reason: str = "expired") -> str:
    return f"{WEB_BASE_PATH}/logged-out?reason={quote(reason)}"


async def get_dashboard_user_and_reason(request: Request) -> tuple[str | None, str | None]:
    session_id = request.cookies.get(WEB_COOKIE_SESSION)
    if not session_id:
        return None, "expired"

    session = await db_fetchone(
        """
        SELECT s.session_id, s.user_id, s.expires_at, u.is_disabled
        FROM sessions s
        JOIN users u ON u.user_id = s.user_id
        WHERE s.session_id = ?
        """,
        (session_id,),
    )
    if not session:
        return None, "expired"

    expires_at = from_iso(session.get("expires_at"))
    if not expires_at or expires_at <= now_utc():
        await delete_session(session_id)
        return None, "expired"

    user_id = session.get("user_id")
    user = await get_user_by_id(user_id)
    if not user:
        await delete_session(session_id)
        return None, "expired"

    if user.get("is_disabled"):
        await delete_sessions_for_user(user_id)
        return None, "revoked"

    await mark_user_seen(user_id, touch_activity=False)
    return user_id, None


async def get_admin_overview(request: Request) -> dict[str, Any]:
    now = now_utc()
    recent_5_since = iso(now - dt.timedelta(minutes=5))
    recent_10_since = iso(now - dt.timedelta(minutes=10))

    users_5 = await db_fetchone(
        "SELECT COUNT(*) AS cnt FROM users WHERE is_admin = 0 AND is_disabled = 0 AND last_activity_at >= ?",
        (recent_5_since,),
    )
    users_10 = await db_fetchone(
        "SELECT COUNT(*) AS cnt FROM users WHERE is_admin = 0 AND is_disabled = 0 AND last_activity_at >= ?",
        (recent_10_since,),
    )

    invites = await db_fetchall(
        """
        SELECT
          i.invite_id,
          i.token,
          i.label,
          i.created_at,
          i.created_by_user_id,
          i.activated_user_id,
          i.activated_at,
          i.revoked_at,
          u.last_seen_at,
          u.last_activity_at,
          u.last_download_at,
          u.is_disabled
        FROM invite_links i
        LEFT JOIN users u ON u.user_id = i.activated_user_id
        ORDER BY i.created_at DESC, i.invite_id DESC
        """
    )

    invite_rows: list[dict[str, Any]] = []
    for item in invites:
        if item.get("revoked_at"):
            status = "revoked"
            status_label = "Доступ удалён"
        elif item.get("activated_user_id"):
            status = "activated"
            status_label = "Активирован"
        else:
            status = "new"
            status_label = "Не активирован"

        item["status"] = status
        item["status_label"] = status_label
        item["invite_url"] = build_public_url(request, f"/invite/{item['token']}")
        item["access_label"] = item.get("label") or "Без имени"
        invite_rows.append(item)

    active_task_rows = []
    active_user_ids: set[str] = set()
    for task in active_tasks.values():
        if task.get("done"):
            continue
        active_user_ids.add(task.get("user_id"))
        task_user = await get_user_by_id(task.get("user_id")) if task.get("user_id") else None
        user_label = None
        if task_user and task_user.get("invite_id"):
            invite = next((x for x in invite_rows if x["invite_id"] == task_user.get("invite_id")), None)
            if invite:
                user_label = invite.get("access_label")
        active_task_rows.append(
            {
                "task_id": task.get("task_id"),
                "user_id": task.get("user_id"),
                "user_label": user_label or task.get("user_id"),
                "status_label": task.get("status_label"),
                "title": task.get("title") or f"Задача {task.get('task_id')}",
                "filename": task.get("filename") or "",
                "quality_label": build_quality_label(task.get("mode") or "", task.get("format_id"), task.get("requested_height")),
                "detail": task.get("detail"),
                "updated_at": task.get("updated_at"),
                "cancel_requested": bool(task.get("cancel_requested")),
            }
        )

    safe_to_restart = len(active_task_rows) == 0 and int((users_5 or {}).get("cnt", 0)) == 0
    settings = await get_settings()
    disk_stats = await asyncio.to_thread(get_disk_stats)
    files = await list_active_downloaded_files(request)

    return {
        "generated_at": iso(now),
        "settings": settings_public_view(settings),
        "disk": disk_stats,
        "files": files,
        "stats": {
            "active_last_5m": int((users_5 or {}).get("cnt", 0)),
            "active_last_10m": int((users_10 or {}).get("cnt", 0)),
            "active_tasks": len(active_task_rows),
            "active_task_users": len(active_user_ids),
            "active_files": len(files),
            "safe_to_restart": safe_to_restart,
        },
        "links": {
            "universal_login_url": build_public_url(request, f"/login?key={quote(WEB_LOGIN_KEY)}"),
        },
        "invites": invite_rows,
        "active_tasks": active_task_rows,
    }


@app.on_event("startup")
async def on_startup() -> None:
    await db_init()
    await ensure_default_settings()
    await migrate_json_to_sqlite()
    await perform_cleanup()
    asyncio.create_task(cleanup_scheduler())

    for i in range(MAX_ACTIVE_TASKS):
        worker_task = asyncio.create_task(download_worker(i + 1))
        worker_tasks.append(worker_task)


@web.get("/login", response_class=HTMLResponse)
async def login(request: Request, key: str | None = None):
    is_admin_login = bool(WEB_ADMIN_LOGIN_KEY) and key == WEB_ADMIN_LOGIN_KEY

    if not is_admin_login and key != WEB_LOGIN_KEY:
        return HTMLResponse("Доступ запрещён", status_code=403)

    user_id, is_new = await get_or_create_browser_user_id(request, is_admin=is_admin_login)
    session_id = await create_session_for_user(user_id)

    redirect = RedirectResponse(url=request.url_for("dashboard"), status_code=303)
    redirect.set_cookie(
        WEB_COOKIE_UID,
        make_uid_cookie_value(user_id),
        max_age=WEB_UID_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
        path=WEB_BASE_PATH,
    )
    redirect.set_cookie(
        WEB_COOKIE_SESSION,
        session_id,
        max_age=WEB_SESSION_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
        path=WEB_BASE_PATH,
    )
    logger.info("Login success user_id=%s is_new=%s is_admin=%s", user_id, is_new, is_admin_login)
    return redirect


@web.get("/invite/{token}", name="invite_login")
async def invite_login(request: Request, token: str):
    invite = await get_invite_by_token(token)
    if not invite:
        return HTMLResponse("Ссылка доступа не найдена", status_code=404)
    if invite.get("revoked_at"):
        return HTMLResponse("Ссылка доступа отключена", status_code=403)

    current_user_id = await get_current_user_id(request, require_auth=False)

    if invite.get("activated_user_id"):
        return RedirectResponse(url=request.url_for("dashboard"), status_code=303)

    invite, user_id = await activate_invite(int(invite["invite_id"]))
    session_id = await create_session_for_user(user_id)

    redirect = RedirectResponse(url=request.url_for("dashboard"), status_code=303)
    redirect.set_cookie(
        WEB_COOKIE_UID,
        make_uid_cookie_value(user_id),
        max_age=WEB_UID_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
        path=WEB_BASE_PATH,
    )
    redirect.set_cookie(
        WEB_COOKIE_SESSION,
        session_id,
        max_age=WEB_SESSION_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
        path=WEB_BASE_PATH,
    )
    logger.info("Invite activated invite_id=%s user_id=%s label=%s", invite["invite_id"], user_id, invite.get("label"))
    return redirect


@web.get("/logout")
async def logout(request: Request):
    session_id = request.cookies.get(WEB_COOKIE_SESSION)
    if session_id:
        await delete_session(session_id)
    response = RedirectResponse(url=build_logged_out_url("logout"), status_code=303)
    response.delete_cookie(WEB_COOKIE_SESSION, path=WEB_BASE_PATH)
    return response


@web.get("/logged-out", response_class=HTMLResponse)
async def logged_out(request: Request):
    reason = (request.query_params.get("reason") or "expired").strip().lower()

    reason_map = {
        "logout": {
            "title": "Вы вышли из сервиса",
            "lead": "Сессия в этом браузере завершена по вашему запросу.",
            "note": "Чтобы снова открыть сервис, используйте свою ссылку входа или новую ссылку доступа от администратора.",
        },
        "revoked": {
            "title": "Доступ к сервису отключён",
            "lead": "Доступ для этой учётной записи был отключён администратором.",
            "note": "Если это произошло по ошибке, запросите новую ссылку доступа у администратора.",
        },
        "expired": {
            "title": "Доступ нужно открыть заново",
            "lead": "Сессия в этом браузере завершилась или больше не действует.",
            "note": "Чтобы снова войти, откройте свою ссылку входа или действующую ссылку доступа.",
        },
    }
    view = reason_map.get(reason, reason_map["expired"])

    return templates.TemplateResponse(
        request,
        "logged_out.html",
        {
            "base_path": WEB_BASE_PATH,
            "reason": reason,
            "title_text": view["title"],
            "lead_text": view["lead"],
            "note_text": view["note"],
        },
    )


@web.get("/", response_class=HTMLResponse, name="dashboard")
async def dashboard(request: Request):
    user_id, logout_reason = await get_dashboard_user_and_reason(request)
    if not user_id:
        return RedirectResponse(url=build_logged_out_url(logout_reason or "expired"), status_code=303)

    meta = await get_user_by_id(user_id)
    if not meta:
        return RedirectResponse(url=build_logged_out_url("expired"), status_code=303)

    history = await get_history(user_id)
    cookie_state = build_effective_cookie_state(user_id, meta)
    admin_cookie_state = build_admin_cookie_state() if meta.get("is_admin") else None

    history_view = []
    for item in history:
        row = dict(item)
        row["file_exists"] = False
        row["watch_url"] = None
        download_url = row.get("download_url")
        if row.get("status") == "done" and download_url:
            token = None
            if "/media/download/" in download_url:
                token = download_url.rsplit("/media/download/", 1)[1].split("?", 1)[0].strip("/")
            if token:
                file_row = await get_file_by_token(token)
                if file_row and not file_row.get("deleted_at") and Path(file_row["file_path"]).exists():
                    row["file_exists"] = True
                    row["watch_url"] = f"{WEB_BASE_PATH}/media/watch/{quote(token)}"
        history_view.append(row)

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "base_path": WEB_BASE_PATH,
            "user": meta,
            "user_id": user_id,
            "cookie_state": cookie_state,
            "admin_cookie_state": admin_cookie_state,
            "history": history_view,
        },
    )


@web.get("/api/me")
async def api_me(request: Request):
    user_id = await get_current_user_id(request)
    meta = await get_user_by_id(user_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    return {
        "user_id": user_id,
        "is_admin": bool(meta.get("is_admin")),
        "last_seen_at": meta.get("last_seen_at"),
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state() if meta.get("is_admin") else None,
        "settings": settings_public_view(await get_settings()),
    }


@web.post("/api/heartbeat")
async def api_heartbeat(request: Request):
    user_id = await get_current_user_id(request)
    await mark_user_seen(user_id, touch_activity=True)
    return {"ok": True, "now": iso(now_utc())}


@web.post("/api/cookies/upload")
async def upload_cookies(request: Request, file: UploadFile = File(...)):
    user_id = await get_current_user_id(request)
    filename = (file.filename or "").lower()
    if not filename.endswith(".txt"):
        raise HTTPException(status_code=400, detail="Нужен .txt файл в формате Netscape.")

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Файл слишком большой.")
    text = content.decode("utf-8", errors="ignore")
    if not text.strip():
        raise HTTPException(status_code=400, detail="Файл пустой.")

    target = COOKIES_PATH / f"cookies_{user_id}.txt"
    target.write_text(text, encoding="utf-8")

    now_iso = iso(now_utc())
    await db_execute(
        "UPDATE users SET cookie_file = ?, cookie_uploaded_at = ?, last_seen_at = ?, last_activity_at = ? WHERE user_id = ?",
        (target.name, now_iso, now_iso, now_iso, user_id),
    )

    meta = await get_user_by_id(user_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return {
        "ok": True,
        "filename": target.name,
        "uploaded_at": now_iso,
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state() if meta.get("is_admin") else None,
    }


@web.post("/api/cookies/delete")
async def delete_cookies(request: Request):
    user_id = await get_current_user_id(request)
    target = COOKIES_PATH / f"cookies_{user_id}.txt"

    try:
        if target.exists():
            target.unlink()
    except Exception as exc:
        logger.exception("Failed to delete cookies for user_id=%s", user_id)
        raise HTTPException(status_code=500, detail=f"Не удалось удалить cookies.txt: {exc}")

    now_iso = iso(now_utc())
    await db_execute(
        "UPDATE users SET cookie_file = NULL, cookie_uploaded_at = NULL, last_seen_at = ?, last_activity_at = ? WHERE user_id = ?",
        (now_iso, now_iso, user_id),
    )

    meta = await get_user_by_id(user_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return {
        "ok": True,
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state() if meta.get("is_admin") else None,
    }


@web.post("/api/admin/cookies/upload")
async def upload_admin_cookies(request: Request, file: UploadFile = File(...)):
    user_id = await get_current_user_id(request)
    meta = await get_user_by_id(user_id)
    if not meta or not meta.get("is_admin"):
        raise HTTPException(status_code=403, detail="Доступно только администратору.")

    filename = (file.filename or "").lower()
    if not filename.endswith(".txt"):
        raise HTTPException(status_code=400, detail="Нужен .txt файл в формате Netscape.")

    content = await file.read()
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Файл слишком большой.")
    text = content.decode("utf-8", errors="ignore")
    if not text.strip():
        raise HTTPException(status_code=400, detail="Файл пустой.")

    target = COOKIES_PATH / ADMIN_COOKIES_FILE
    target.write_text(text, encoding="utf-8")
    await mark_user_seen(user_id, touch_activity=True)

    meta = await get_user_by_id(user_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return {
        "ok": True,
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state(),
    }


@web.post("/api/admin/cookies/delete")
async def delete_admin_cookies(request: Request):
    user_id = await get_current_user_id(request)
    meta = await get_user_by_id(user_id)
    if not meta or not meta.get("is_admin"):
        raise HTTPException(status_code=403, detail="Доступно только администратору.")

    target = COOKIES_PATH / ADMIN_COOKIES_FILE

    try:
        if target.exists():
            target.unlink()
    except Exception as exc:
        logger.exception("Failed to delete admin cookies")
        raise HTTPException(status_code=500, detail=f"Не удалось удалить общий cookies.txt: {exc}")

    await mark_user_seen(user_id, touch_activity=True)

    meta = await get_user_by_id(user_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return {
        "ok": True,
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state(),
    }


@web.post("/api/analyze")
async def api_analyze(request: Request, url: str = Form(...)):
    user_id = await get_current_user_id(request)
    url = url.strip()
    if not re.match(r"https?://\S+", url):
        raise HTTPException(status_code=400, detail="Нужна корректная ссылка http/https.")

    await mark_user_seen(user_id, touch_activity=True)
    data = await analyze_url_for_user(user_id, url)
    return {"ok": True, **data}


@web.post("/api/download")
async def api_download(
    request: Request,
    url: str = Form(...),
    mode: str = Form(...),
    format_id: str | None = Form(None),
    title: str | None = Form(None),
    quality_height: int | None = Form(None),
):
    user_id = await get_current_user_id(request)
    url = url.strip()
    if mode not in {"safe", "bestq", "any", "audio", "pick"}:
        raise HTTPException(status_code=400, detail="Неизвестный режим скачивания.")

    await mark_user_seen(user_id, touch_activity=True)
    task = await start_download_task(user_id, url, mode, format_id=format_id, title_hint=title, requested_height=quality_height)
    return {
        "ok": True,
        "task_id": task["task_id"],
        "status": task["status"],
        "status_label": task["status_label"],
        "detail": task["detail"],
        "queue_position": task.get("queue_position"),
    }


@web.get("/api/task/{task_id}")
async def api_task_status(request: Request, task_id: str):
    user_id = await get_current_user_id(request)
    task = active_tasks.get(task_id)
    if not task or task.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Задача не найдена.")
    return task


@web.post("/api/task/{task_id}/cancel")
async def api_task_cancel(request: Request, task_id: str):
    user_id = await get_current_user_id(request)
    await mark_user_seen(user_id, touch_activity=True)
    task = await cancel_download_task(task_id, user_id, is_admin=False)
    return {"ok": True, "task": task}


@web.get("/media/download/{token}")
async def media_download(request: Request, token: str):
    file_row = await get_file_by_token(token)
    if not file_row or file_row.get("deleted_at"):
        raise HTTPException(status_code=404, detail="Файл уже удалён или ссылка недействительна.")
    expires_at = from_iso(file_row.get("expires_at"))
    if expires_at and expires_at <= now_utc():
        await delete_downloaded_file(int(file_row["file_id"]), "expired")
        raise HTTPException(status_code=404, detail="Файл уже удалён по таймеру.")

    path = Path(file_row["file_path"])
    if not path.exists() or not path.is_file():
        await db_execute(
            "UPDATE downloaded_files SET deleted_at = ?, delete_reason = ? WHERE file_id = ?",
            (iso(now_utc()), "missing_on_disk", file_row["file_id"]),
        )
        raise HTTPException(status_code=404, detail="Файл не найден на сервере.")

    await mark_file_accessed(int(file_row["file_id"]), extend_expiry=False)
    headers = {
        "X-Accel-Redirect": f"/_protected_downloads/{quote(file_row['stored_filename'])}",
        "Content-Type": file_row.get("mime_type") or guess_mime_type(path),
        "Content-Disposition": f'attachment; filename="{file_row["stored_filename"]}"',
    }
    return Response(status_code=200, headers=headers)


@web.get("/media/watch/{token}")
async def media_watch(request: Request, token: str):
    file_row = await get_file_by_token(token)
    if not file_row or file_row.get("deleted_at"):
        raise HTTPException(status_code=404, detail="Файл уже удалён или ссылка недействительна.")
    expires_at = from_iso(file_row.get("expires_at"))
    if expires_at and expires_at <= now_utc():
        await delete_downloaded_file(int(file_row["file_id"]), "expired")
        raise HTTPException(status_code=404, detail="Файл уже удалён по таймеру.")

    path = Path(file_row["file_path"])
    if not path.exists() or not path.is_file():
        await db_execute(
            "UPDATE downloaded_files SET deleted_at = ?, delete_reason = ? WHERE file_id = ?",
            (iso(now_utc()), "missing_on_disk", file_row["file_id"]),
        )
        raise HTTPException(status_code=404, detail="Файл не найден на сервере.")

    await mark_file_accessed(int(file_row["file_id"]), extend_expiry=True)
    headers = {
        "X-Accel-Redirect": f"/_protected_watch/{quote(file_row['stored_filename'])}",
        "Content-Type": file_row.get("mime_type") or guess_mime_type(path),
        "Content-Disposition": f'inline; filename="{file_row["stored_filename"]}"',
    }
    return Response(status_code=200, headers=headers)



def _proxy_quality_side_limit(height: int | None) -> int:
    if not height or height <= 0:
        return 0
    return int(height) * 2


def _proxy_format_short_side(fmt: dict[str, Any]) -> int:
    width = int(fmt.get("width") or 0)
    height = int(fmt.get("height") or 0)
    if width and height:
        return min(width, height)
    return height or width or 0


def _proxy_format_long_side(fmt: dict[str, Any]) -> int:
    width = int(fmt.get("width") or 0)
    height = int(fmt.get("height") or 0)
    return max(width, height)


def _proxy_video_codec_priority(fmt: dict[str, Any]) -> int:
    codec = str(fmt.get("vcodec") or "").lower()
    ext = str(fmt.get("ext") or "").lower()
    if ext == "mp4" and (codec.startswith("avc") or codec.startswith("h264")):
        return 5
    if ext == "mp4" and codec.startswith("hev"):
        return 4
    if ext == "mp4" and codec.startswith("av01"):
        return 3
    if ext == "webm" and codec.startswith("vp9"):
        return 2
    if ext == "webm":
        return 1
    return 0


def _proxy_audio_codec_priority(fmt: dict[str, Any]) -> int:
    ext = str(fmt.get("ext") or "").lower()
    codec = str(fmt.get("acodec") or "").lower()
    if ext == "m4a" or codec.startswith("mp4a"):
        return 4
    if ext == "webm" and codec.startswith("opus"):
        return 3
    if ext == "mp3":
        return 2
    return 0


def _proxy_size(fmt: dict[str, Any]) -> int:
    return int(fmt.get("filesize") or fmt.get("filesize_approx") or 0)


def _proxy_format_headers(info: dict[str, Any], fmt: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for source in (info.get("http_headers") or {}, fmt.get("http_headers") or {}):
        if isinstance(source, dict):
            for key, value in source.items():
                if value is not None:
                    headers[str(key)] = str(value)
    if "User-Agent" not in headers:
        headers["User-Agent"] = (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )
    return headers


def _proxy_pick_formats(info: dict[str, Any], requested_height: int | None, max_file_bytes: int | None) -> tuple[dict[str, Any], dict[str, Any]]:
    formats = info.get("formats") or []
    side_limit = _proxy_quality_side_limit(requested_height)

    video_candidates: list[dict[str, Any]] = []
    audio_candidates: list[dict[str, Any]] = []

    for fmt in formats:
        fmt_url = fmt.get("url")
        fmt_id = fmt.get("format_id")
        ext = str(fmt.get("ext") or "").lower()
        vcodec = fmt.get("vcodec")
        acodec = fmt.get("acodec")

        if not fmt_url or not fmt_id:
            continue
        if ext == "mhtml" or "storyboard" in str(fmt_id).lower():
            continue

        size = _proxy_size(fmt)
        if max_file_bytes and size and size > max_file_bytes:
            continue

        has_video = bool(vcodec and vcodec != "none")
        has_audio = bool(acodec and acodec != "none")

        if has_video and not has_audio:
            if side_limit:
                width = int(fmt.get("width") or 0)
                height = int(fmt.get("height") or 0)
                if (width and width > side_limit) or (height and height > side_limit):
                    continue
            video_candidates.append(fmt)
        elif has_audio and not has_video:
            audio_candidates.append(fmt)

    if not video_candidates:
        raise RuntimeError("Не удалось найти отдельную видеодорожку для прокси-скачивания. Используй обычное скачивание.")
    if not audio_candidates:
        raise RuntimeError("Не удалось найти отдельную аудиодорожку для прокси-скачивания. Используй обычное скачивание.")

    video_candidates.sort(
        key=lambda item: (
            _proxy_format_short_side(item),
            _proxy_video_codec_priority(item),
            int(item.get("fps") or 0),
            int(item.get("tbr") or 0),
            _proxy_size(item),
        ),
        reverse=True,
    )
    audio_candidates.sort(
        key=lambda item: (
            _proxy_audio_codec_priority(item),
            int(item.get("abr") or item.get("tbr") or 0),
            _proxy_size(item),
        ),
        reverse=True,
    )

    return video_candidates[0], audio_candidates[0]


def _proxy_filename(title: str, kind: str, fmt: dict[str, Any]) -> str:
    safe_title = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._ -]+", "_", title).strip(" ._")[:80]
    if not safe_title:
        safe_title = "clipsave"
    ext = str(fmt.get("ext") or ("m4a" if kind == "audio" else "mp4")).lower()
    suffix = "audio" if kind == "audio" else "video"
    return f"{safe_title}_{suffix}_{fmt.get('format_id')}.{ext}"


async def _proxy_cleanup_expired_tokens() -> None:
    now = now_utc()
    async with proxy_stream_lock:
        expired = [token for token, item in proxy_stream_tokens.items() if item.get("expires_at") <= now]
        for token in expired:
            proxy_stream_tokens.pop(token, None)


@web.post("/api/proxy-download")
async def api_proxy_download(
    request: Request,
    url: str = Form(...),
    mode: str = Form("safe"),
    quality_height: int | None = Form(None),
):
    user_id = await get_current_user_id(request)
    settings = await get_settings()
    if not setting_bool(settings, "experimental_proxy_download_enabled"):
        raise HTTPException(status_code=403, detail="Прокси-скачивание отключено администратором.")

    url = url.strip()
    if not re.match(r"https?://\S+", url):
        raise HTTPException(status_code=400, detail="Нужна корректная ссылка http/https.")

    await mark_user_seen(user_id, touch_activity=True)
    await _proxy_cleanup_expired_tokens()

    max_height = quality_height or setting_int(settings, "default_user_quality", 1080)
    if setting_bool(settings, "allow_unlimited_quality"):
        max_height = quality_height or 0

    opts = build_base_ydl_opts(user_id, skip_download=True, quiet=True)
    opts["format"] = "bv*+ba/b"

    max_file_bytes = None
    if not setting_bool(settings, "allow_unlimited_file_size"):
        max_file_bytes = setting_gb_to_bytes(settings, "experimental_proxy_max_file_gb", Decimal("2"))

    try:
        info = await asyncio.to_thread(ydl_extract, clean_youtube_url(url), opts, download=False)
        video_fmt, audio_fmt = _proxy_pick_formats(info, max_height, max_file_bytes)
        proxy_known_size = _proxy_size(video_fmt) + _proxy_size(audio_fmt)
        enforce_single_file_size_limit_by_value(proxy_known_size, max_file_bytes)
    except Exception as exc:
        logger.warning("Proxy prepare failed. err=%s", exc)
        raise HTTPException(status_code=500, detail=human_download_error(exc)) from exc

    title = info.get("title") or "video"
    ttl_minutes = setting_int(settings, "experimental_proxy_max_duration_minutes", 30)
    expires_at = now_utc() + dt.timedelta(minutes=max(1, ttl_minutes))

    video_token = secrets.token_urlsafe(32)
    audio_token = secrets.token_urlsafe(32)

    common_info = {
        "created_by_user_id": user_id,
        "source_url": clean_youtube_url(url),
        "title": title,
        "expires_at": expires_at,
        "max_file_bytes": max_file_bytes,
    }

    async with proxy_stream_lock:
        proxy_stream_tokens[video_token] = {
            **common_info,
            "kind": "video",
            "url": video_fmt["url"],
            "headers": _proxy_format_headers(info, video_fmt),
            "filename": _proxy_filename(title, "video", video_fmt),
            "media_type": video_fmt.get("acodec") != "none" and "video/mp4" or f"video/{video_fmt.get('ext') or 'mp4'}",
            "format_id": video_fmt.get("format_id"),
            "size": _proxy_size(video_fmt),
            "label": f"Видео {video_fmt.get('format_id')} · {video_fmt.get('width') or '?'}x{video_fmt.get('height') or '?'} · {video_fmt.get('ext') or ''}",
        }
        proxy_stream_tokens[audio_token] = {
            **common_info,
            "kind": "audio",
            "url": audio_fmt["url"],
            "headers": _proxy_format_headers(info, audio_fmt),
            "filename": _proxy_filename(title, "audio", audio_fmt),
            "media_type": "audio/mp4" if str(audio_fmt.get("ext") or "").lower() == "m4a" else f"audio/{audio_fmt.get('ext') or 'mpeg'}",
            "format_id": audio_fmt.get("format_id"),
            "size": _proxy_size(audio_fmt),
            "label": f"Аудио {audio_fmt.get('format_id')} · {audio_fmt.get('ext') or ''} · {audio_fmt.get('abr') or audio_fmt.get('tbr') or '?'} kbps",
        }

    return {
        "ok": True,
        "expires_at": iso(expires_at),
        "expires_in_minutes": max(1, ttl_minutes),
        "video": {
            "url": f"{WEB_BASE_PATH}/proxy/video/{video_token}",
            "filename": proxy_stream_tokens[video_token]["filename"],
            "label": proxy_stream_tokens[video_token]["label"],
            "size_text": fmt_size(proxy_stream_tokens[video_token]["size"]),
        },
        "audio": {
            "url": f"{WEB_BASE_PATH}/proxy/audio/{audio_token}",
            "filename": proxy_stream_tokens[audio_token]["filename"],
            "label": proxy_stream_tokens[audio_token]["label"],
            "size_text": fmt_size(proxy_stream_tokens[audio_token]["size"]),
        },
    }


@web.get("/proxy/{kind}/{token}")
async def proxy_stream_download(request: Request, kind: str, token: str):
    if kind not in {"video", "audio"}:
        raise HTTPException(status_code=404, detail="Прокси-ссылка не найдена.")

    await _proxy_cleanup_expired_tokens()
    async with proxy_stream_lock:
        item = proxy_stream_tokens.get(token)

    if not item or item.get("kind") != kind:
        raise HTTPException(status_code=404, detail="Прокси-ссылка не найдена или устарела.")
    if item.get("expires_at") <= now_utc():
        async with proxy_stream_lock:
            proxy_stream_tokens.pop(token, None)
        raise HTTPException(status_code=404, detail="Прокси-ссылка устарела. Нажми «Прокси-скачивание» ещё раз.")

    upstream_headers = dict(item.get("headers") or {})
    range_header = request.headers.get("range")
    if range_header:
        upstream_headers["Range"] = range_header

    timeout = httpx.Timeout(connect=30.0, read=None, write=30.0, pool=30.0)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10)
    client = httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True)

    try:
        upstream = await client.send(
            client.build_request("GET", item["url"], headers=upstream_headers),
            stream=True,
        )
        upstream.raise_for_status()
        max_file_bytes = item.get("max_file_bytes")
        content_length = int(upstream.headers.get("Content-Length") or 0)
        if max_file_bytes and content_length and content_length > max_file_bytes:
            raise HTTPException(status_code=413, detail=bytes_limit_error(max_file_bytes))
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response else 502
        await client.aclose()
        logger.warning("Proxy upstream HTTP error: status=%s token=%s", status_code, token)
        raise HTTPException(status_code=502, detail=f"Источник вернул ошибку HTTP {status_code}. Создай прокси-ссылку заново.") from exc
    except HTTPException:
        await client.aclose()
        raise
    except Exception as exc:
        await client.aclose()
        logger.warning("Proxy upstream open failed. err=%s", exc)
        raise HTTPException(status_code=502, detail="Не удалось открыть поток источника. Создай прокси-ссылку заново.") from exc

    async def iterator():
        started_at = time.monotonic()
        sent_bytes = 0
        try:
            async for chunk in upstream.aiter_bytes(chunk_size=4 * 1024 * 1024):
                if await request.is_disconnected():
                    logger.info("Proxy stream client disconnected: kind=%s token=%s bytes=%s", kind, token, sent_bytes)
                    break
                max_file_bytes = item.get("max_file_bytes")
                if max_file_bytes and sent_bytes + len(chunk) > max_file_bytes:
                    logger.warning("Proxy stream stopped by size limit: kind=%s token=%s limit=%s", kind, token, max_file_bytes)
                    break
                sent_bytes += len(chunk)
                yield chunk
        finally:
            duration = max(0.001, time.monotonic() - started_at)
            avg_speed = sent_bytes / duration
            logger.info(
                "Proxy stream finished: kind=%s token=%s bytes=%s duration=%.2fs avg=%s/s",
                kind,
                token,
                sent_bytes,
                duration,
                fmt_size(avg_speed),
            )
            await upstream.aclose()
            await client.aclose()

    headers = {
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(item['filename'])}",
        "Cache-Control": "no-store",
        "Accept-Ranges": upstream.headers.get("Accept-Ranges", "bytes"),
    }
    for header in ("Content-Length", "Content-Range"):
        value = upstream.headers.get(header)
        if value:
            headers[header] = value

    logger.info(
        "Proxy stream started: kind=%s token=%s status=%s length=%s range=%s",
        kind,
        token,
        upstream.status_code,
        headers.get("Content-Length", "unknown"),
        range_header or "no",
    )

    return StreamingResponse(
        iterator(),
        status_code=upstream.status_code,
        media_type=upstream.headers.get("Content-Type") or item.get("media_type") or "application/octet-stream",
        headers=headers,
    )
@web.post("/api/cleanup")
async def api_cleanup(request: Request):
    await get_current_user_id(request)
    result = await perform_cleanup()
    return {"ok": True, **result}


async def require_admin_user(request: Request) -> tuple[str, dict[str, Any]]:
    user_id = await get_current_user_id(request)
    meta = await get_user_by_id(user_id)
    if not meta or not meta.get("is_admin"):
        raise HTTPException(status_code=403, detail="Доступно только администратору.")
    return user_id, meta


@web.get("/api/admin/overview")
async def api_admin_overview(request: Request):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    return await get_admin_overview(request)


@web.post("/api/admin/tasks/{task_id}/cancel")
async def api_admin_task_cancel(request: Request, task_id: str):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    task = await cancel_download_task(task_id, user_id, is_admin=True)
    return {"ok": True, "task": task, "overview": await get_admin_overview(request)}


@web.get("/api/admin/settings")
async def api_admin_settings(request: Request):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    return {"ok": True, "settings": settings_public_view(await get_settings())}


@web.post("/api/admin/settings")
async def api_admin_update_settings(request: Request):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    form = await request.form()
    settings = await update_settings_from_form(dict(form))
    return {"ok": True, "settings": settings_public_view(settings), "overview": await get_admin_overview(request)}


@web.get("/api/admin/files")
async def api_admin_files(request: Request):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    return {"ok": True, "files": await list_active_downloaded_files(request), "overview": await get_admin_overview(request)}


@web.post("/api/admin/files/{file_id}/delete")
async def api_admin_delete_file(request: Request, file_id: int):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    await delete_downloaded_file(file_id, "admin_single")
    return {"ok": True, "files": await list_active_downloaded_files(request), "overview": await get_admin_overview(request)}


@web.post("/api/admin/files/cleanup-expired")
async def api_admin_cleanup_expired_files(request: Request):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    removed = await cleanup_expired_downloaded_files()
    return {"ok": True, "removed": removed, "files": await list_active_downloaded_files(request), "overview": await get_admin_overview(request)}


@web.post("/api/admin/files/cleanup-all")
async def api_admin_cleanup_all_files(request: Request):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    files = await db_fetchall("SELECT file_id FROM downloaded_files WHERE deleted_at IS NULL")
    removed = 0
    for item in files:
        if await delete_downloaded_file(int(item["file_id"]), "admin_cleanup_all"):
            removed += 1
    return {"ok": True, "removed": removed, "files": await list_active_downloaded_files(request), "overview": await get_admin_overview(request)}


@web.post("/api/admin/invites")
async def api_admin_create_invite(request: Request, label: str | None = Form(None)):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    invite = await create_invite_link(user_id, label)
    return {
        "ok": True,
        "invite": {
            "invite_id": invite["invite_id"],
            "label": invite.get("label") or "Без имени",
            "invite_url": build_public_url(request, f"/invite/{invite['token']}"),
            "created_at": invite.get("created_at"),
        },
        "overview": await get_admin_overview(request),
    }


@web.post("/api/admin/invites/{invite_id}/label")
async def api_admin_update_invite_label(request: Request, invite_id: int, label: str | None = Form(None)):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    if not await get_invite_by_id(invite_id):
        raise HTTPException(status_code=404, detail="Ссылка доступа не найдена")
    await update_invite_label(invite_id, label)
    return {"ok": True, "overview": await get_admin_overview(request)}


@web.post("/api/admin/invites/{invite_id}/revoke")
async def api_admin_revoke_invite(request: Request, invite_id: int):
    user_id, _ = await require_admin_user(request)
    await mark_user_seen(user_id, touch_activity=True)
    await revoke_invite_access(invite_id)
    return {"ok": True, "overview": await get_admin_overview(request)}


def parse_args() -> list[str]:
    return sys.argv[1:]


async def run_cleanup_cli() -> None:
    await db_init()
    result = await perform_cleanup()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    args = parse_args()
    if "--cleanup" in args:
        asyncio.run(run_cleanup_cli())
        raise SystemExit(0)

    import uvicorn

    uvicorn.run(app, host=WEB_HOST, port=WEB_PORT)
