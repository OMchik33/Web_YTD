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

import yt_dlp
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


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
CLEANUP_HOUR = int(os.getenv("CLEANUP_HOUR", "4"))
CLEANUP_MINUTE = int(os.getenv("CLEANUP_MINUTE", "10"))
MAX_ACTIVE_TASKS = max(1, int(os.getenv("MAX_ACTIVE_TASKS", "1")))
MAX_ACTIVE_TASKS_PER_USER = max(1, int(os.getenv("MAX_ACTIVE_TASKS_PER_USER", "1")))
REQUEST_TTL_HOURS = int(os.getenv("REQUEST_TTL_HOURS", "1"))
DOWNLOAD_PATH = resolve_path_env(os.getenv("DOWNLOAD_PATH", "/download"), Path("/download"))
COOKIES_PATH = resolve_path_env(os.getenv("COOKIES_PATH", "cookies"), BASE_DIR / "cookies")
DATA_PATH = resolve_path_env(os.getenv("DATA_PATH", "data"), BASE_DIR / "data")
LOG_PATH = resolve_path_env(os.getenv("LOG_PATH", "logs"), BASE_DIR / "logs")
DOWNLOAD_BASE_URL = os.getenv("DOWNLOAD_BASE_URL", "https://example.com/files")
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

CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_users_last_seen_at ON users(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_users_last_activity_at ON users(last_activity_at);
CREATE INDEX IF NOT EXISTS idx_users_invite_id ON users(invite_id);
CREATE INDEX IF NOT EXISTS idx_history_user_created_at ON download_history(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_invite_links_activated_at ON invite_links(activated_at);
CREATE INDEX IF NOT EXISTS idx_invite_links_revoked_at ON invite_links(revoked_at);
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


async def append_history(user_id: str, record: dict[str, Any]) -> None:
    await db_execute(
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
        "ignore_no_formats_error": True,
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


def get_format_string(mode: str, format_id: str | None) -> str:
    if mode == "pick":
        return f"{format_id}+bestaudio[ext=m4a]/best[ext=mp4]/best"
    if mode == "safe":
        return "best[ext=mp4]/best"
    if mode == "bestq":
        return "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
    if mode == "any":
        return "best"
    return "best"


def ydl_extract(url: str, opts: dict[str, Any], *, download: bool):
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=download)


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
) -> dict[str, Any]:
    if await count_user_active_tasks(user_id) >= MAX_ACTIVE_TASKS_PER_USER:
        raise HTTPException(status_code=429, detail="У вас уже есть активная задача. Дождитесь завершения.")

    task = init_task(user_id, url)
    task["mode"] = mode
    task["title"] = title_hint
    task["format_id"] = format_id

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
) -> dict[str, Any]:
    opts = build_base_ydl_opts(user_id, skip_download=False, quiet=False, task_id=task_id)
    title = title_hint or "Видео"
    last_progress_emit = 0.0

    def progress_hook(data: dict[str, Any]) -> None:
        nonlocal last_progress_emit

        try:
            status = data.get("status")
            now_monotonic = time.monotonic()

            if status == "downloading":
                if now_monotonic - last_progress_emit < 0.8:
                    return
                last_progress_emit = now_monotonic

                downloaded = data.get("downloaded_bytes") or 0
                total = data.get("total_bytes") or data.get("total_bytes_estimate") or 0
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
        except Exception:
            logger.exception("Progress hook error for task_id=%s", task_id)

    def postprocessor_hook(data: dict[str, Any]) -> None:
        try:
            status = data.get("status")
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
        opts["format"] = get_format_string(mode, format_id)

    logger.info("Downloading url=%s mode=%s format=%s", url, mode, opts.get("format"))

    try:
        info = ydl_extract(url, opts, download=True)
    except Exception as e1:
        logger.warning("Primary download failed, retry with ffmpeg downloader. err=%s", e1)
        schedule_task_update(
            loop,
            task_id,
            status="processing",
            status_label="Повторная попытка",
            detail="Переключаюсь на ffmpeg downloader",
        )
        opts_ff = dict(opts)
        opts_ff["downloader"] = "ffmpeg"
        info = ydl_extract(url, opts_ff, download=True)

    path = find_downloaded_file(info)
    if not path or not os.path.exists(path):
        raise RuntimeError("Файл не найден после скачивания.")

    title = info.get("title") or title
    thumbnail_url = info.get("thumbnail")
    ext = Path(path).suffix[1:] if Path(path).suffix else "bin"
    unique = f"{hashlib.md5((title + task_id).encode()).hexdigest()[:8]}_{int(time.time())}.{ext}"
    final = DOWNLOAD_PATH / unique
    os.replace(path, final)

    clean_title = sanitize_filename(title)
    dlink = f"{DOWNLOAD_BASE_URL}/{quote(unique)}?filename={quote(clean_title + '.' + ext)}"
    return {
        "title": title,
        "filename": final.name,
        "download_url": dlink,
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

            user_id = task["user_id"]
            url = task["url"]
            mode = task["mode"]
            format_id = task.get("format_id")
            title_hint = task.get("title")

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
            )

            await update_task(
                task_id,
                status="done",
                status_label="Готово",
                detail="Файл готов к скачиванию",
                title=result["title"],
                filename=result["filename"],
                download_url=result["download_url"],
                thumbnail_url=result.get("thumbnail_url"),
                error=None,
                done=True,
                queue_position=None,
            )

            await update_last_download_at(user_id)
            await append_history(
                user_id,
                {
                    "created_at": iso(now_utc()),
                    "title": result["title"],
                    "mode": mode,
                    "status": "done",
                    "download_url": result["download_url"],
                    "filename": result["filename"],
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

    logger.info(
        "Cleanup finished: removed_universal_users=%s removed_revoked_invites=%s removed_sessions=%s removed_finished_tasks=%s",
        removed_universal_users,
        removed_revoked_invites,
        removed_sessions,
        removed_finished_tasks,
    )

    return {
        "removed_universal_users": removed_universal_users,
        "removed_revoked_invites": removed_revoked_invites,
        "removed_sessions": removed_sessions,
        "removed_finished_tasks": removed_finished_tasks,
    }


async def cleanup_scheduler() -> None:
    await asyncio.sleep(2)
    while True:
        now = now_utc()
        next_run = now.astimezone().replace(hour=CLEANUP_HOUR, minute=CLEANUP_MINUTE, second=0, microsecond=0)
        if next_run <= now.astimezone():
            next_run = next_run + dt.timedelta(days=1)
        sleep_seconds = max(30, (next_run.astimezone(dt.timezone.utc) - now).total_seconds())
        logger.info("Next cleanup at %s", next_run.isoformat())
        await asyncio.sleep(sleep_seconds)
        try:
            await perform_cleanup()
        except Exception:
            logger.exception("Scheduled cleanup failed")


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
                "title": task.get("title") or "Видео",
                "detail": task.get("detail"),
                "updated_at": task.get("updated_at"),
            }
        )

    safe_to_restart = len(active_task_rows) == 0 and int((users_5 or {}).get("cnt", 0)) == 0

    return {
        "generated_at": iso(now),
        "stats": {
            "active_last_5m": int((users_5 or {}).get("cnt", 0)),
            "active_last_10m": int((users_10 or {}).get("cnt", 0)),
            "active_tasks": len(active_task_rows),
            "active_task_users": len(active_user_ids),
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
        download_url = row.get("download_url")
        if row.get("status") == "done" and download_url:
            try:
                stored_name = download_url.split("/files/", 1)[1].split("?", 1)[0]
                row["file_exists"] = (DOWNLOAD_PATH / stored_name).exists()
            except Exception:
                row["file_exists"] = False
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
):
    user_id = await get_current_user_id(request)
    url = url.strip()
    if mode not in {"safe", "bestq", "any", "audio", "pick"}:
        raise HTTPException(status_code=400, detail="Неизвестный режим скачивания.")

    await mark_user_seen(user_id, touch_activity=True)
    task = await start_download_task(user_id, url, mode, format_id=format_id, title_hint=title)
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
