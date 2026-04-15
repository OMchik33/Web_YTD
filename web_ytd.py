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
DOWNLOAD_PATH = Path(os.getenv("DOWNLOAD_PATH", "/download"))
COOKIES_PATH = Path(os.getenv("COOKIES_PATH", str(BASE_DIR / "cookies")))
DATA_PATH = Path(os.getenv("DATA_PATH", str(BASE_DIR / "data")))
LOG_PATH = Path(os.getenv("LOG_PATH", str(BASE_DIR / "logs")))
DOWNLOAD_BASE_URL = os.getenv("DOWNLOAD_BASE_URL", "https://example.com/files")
DEBUG_YTDLP = os.getenv("DEBUG_YTDLP", "0") == "1"

USERS_FILE = DATA_PATH / "users.json"
SESSIONS_FILE = DATA_PATH / "sessions.json"
HISTORY_DIR = DATA_PATH / "history"

DOWNLOAD_PATH.mkdir(parents=True, exist_ok=True)
COOKIES_PATH.mkdir(parents=True, exist_ok=True)
DATA_PATH.mkdir(parents=True, exist_ok=True)
LOG_PATH.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)

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

store_lock = asyncio.Lock()
analysis_lock = asyncio.Lock()
queue_state_lock = asyncio.Lock()

task_queue: asyncio.Queue[str] = asyncio.Queue()
queued_task_ids: list[str] = []
worker_tasks: list[asyncio.Task] = []

active_tasks: dict[str, dict[str, Any]] = {}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


async def read_json(path: Path, default: dict) -> dict:
    async with store_lock:
        if not path.exists():
            return json.loads(json.dumps(default))
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            logger.exception("Failed to read JSON from %s", path)
            return json.loads(json.dumps(default))


async def write_json(path: Path, data: dict) -> None:
    async with store_lock:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


async def read_users() -> dict:
    return await read_json(USERS_FILE, {})


async def write_users(data: dict) -> None:
    await write_json(USERS_FILE, data)


async def read_sessions() -> dict:
    return await read_json(SESSIONS_FILE, {})


async def write_sessions(data: dict) -> None:
    await write_json(SESSIONS_FILE, data)


async def append_history(user_id: str, record: dict[str, Any]) -> None:
    path = HISTORY_DIR / f"history_{user_id}.json"
    async with store_lock:
        data = []
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                logger.exception("Failed to read history %s", path)
        data.insert(0, record)
        data = data[:50]
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


async def get_history(user_id: str) -> list[dict[str, Any]]:
    path = HISTORY_DIR / f"history_{user_id}.json"
    async with store_lock:
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data[:20]
        except Exception:
            logger.exception("Failed to load history %s", path)
        return []


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


async def get_or_create_browser_user_id(request: Request, *, is_admin: bool = False) -> tuple[str, bool]:
    uid_cookie = request.cookies.get(WEB_COOKIE_UID)
    existing_user_id = parse_uid_cookie_value(uid_cookie)
    users = await read_users()

    if existing_user_id and existing_user_id in users:
        if is_admin and not users[existing_user_id].get("is_admin", False):
            users[existing_user_id]["is_admin"] = True
            users[existing_user_id]["last_seen_at"] = iso(now_utc())
            await write_users(users)
        return existing_user_id, False

    user_id = "u_" + secrets.token_hex(8)
    users[user_id] = {
        "created_at": iso(now_utc()),
        "last_seen_at": iso(now_utc()),
        "cookie_file": None,
        "cookie_uploaded_at": None,
        "last_download_at": None,
        "is_admin": is_admin,
    }
    await write_users(users)
    return user_id, True


async def create_session_for_user(user_id: str) -> str:
    sessions = await read_sessions()
    session_id = "sess_" + secrets.token_hex(16)
    created = now_utc()
    sessions[session_id] = {
        "user_id": user_id,
        "created_at": iso(created),
        "expires_at": iso(created + dt.timedelta(seconds=WEB_SESSION_MAX_AGE)),
    }
    await write_sessions(sessions)
    return session_id


async def get_current_user_id(request: Request, *, require_auth: bool = True) -> str | None:
    session_id = request.cookies.get(WEB_COOKIE_SESSION)
    sessions = await read_sessions()
    session = sessions.get(session_id or "")
    if not session:
        if require_auth:
            raise HTTPException(status_code=401, detail="Требуется вход")
        return None

    expires_at = from_iso(session.get("expires_at"))
    if not expires_at or expires_at <= now_utc():
        sessions.pop(session_id, None)
        await write_sessions(sessions)
        if require_auth:
            raise HTTPException(status_code=401, detail="Сессия истекла")
        return None

    user_id = session.get("user_id")
    users = await read_users()
    if not user_id or user_id not in users:
        sessions.pop(session_id, None)
        await write_sessions(sessions)
        if require_auth:
            raise HTTPException(status_code=401, detail="Пользователь не найден")
        return None

    users[user_id]["last_seen_at"] = iso(now_utc())
    await write_users(users)
    return user_id


async def get_user_meta(user_id: str) -> dict[str, Any]:
    users = await read_users()
    meta = users.get(user_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return meta


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
            "helper_text": f"Используется ваш cookies файл",
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
            "helper_text": f"Используется общий cookies администратора",
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
            "helper_text": f"Этот файл используется по умолчанию для всех пользователей без личного cookies",
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
            m = re.search(r"(\d{3,4})p", f.get("format", "") or "")
            if m:
                height = int(m.group(1))

        if not height:
            continue

        size = f.get("filesize") or f.get("filesize_approx") or 0
        label = f"{height}p {ext}"
        if fmt_size(size):
            label += f" ({fmt_size(size)})"

        if label in seen_labels:
            continue

        seen_labels.add(label)
        available.append({
            "label": label,
            "format_id": fid,
            "height": height,
            "ext": ext,
            "filesize": size,
        })

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
        opts["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]
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

            users = await read_users()
            if user_id in users:
                users[user_id]["last_download_at"] = iso(now_utc())
                await write_users(users)

            await append_history(user_id, {
                "created_at": iso(now_utc()),
                "title": result["title"],
                "mode": mode,
                "status": "done",
                "download_url": result["download_url"],
                "filename": result["filename"],
                "source_url": url,
            })

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
                await append_history(user_id, {
                    "created_at": iso(now_utc()),
                    "title": title_hint or "Видео",
                    "mode": mode,
                    "status": "error",
                    "error": str(exc),
                    "source_url": url,
                })

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


async def perform_cleanup() -> dict[str, int]:
    users = await read_users()
    sessions = await read_sessions()
    now = now_utc()
    removed_users = 0
    removed_sessions = 0
    removed_finished_tasks = 0

    expired_sessions = []
    for session_id, meta in sessions.items():
        expires_at = from_iso(meta.get("expires_at"))
        if not expires_at or expires_at <= now:
            expired_sessions.append(session_id)
    for session_id in expired_sessions:
        sessions.pop(session_id, None)
        removed_sessions += 1

    users_to_delete = []
    for user_id, meta in users.items():
        last_seen = from_iso(meta.get("last_seen_at")) or from_iso(meta.get("created_at")) or now
        if now - last_seen > USER_RETENTION:
            users_to_delete.append(user_id)

    for user_id in users_to_delete:
        cookie_file = get_cookie_file(user_id)
        if cookie_file:
            cookie_file.unlink(missing_ok=True)

        history_file = HISTORY_DIR / f"history_{user_id}.json"
        history_file.unlink(missing_ok=True)

        users.pop(user_id, None)
        removed_users += 1

        for session_id in list(sessions.keys()):
            if sessions[session_id].get("user_id") == user_id:
                sessions.pop(session_id, None)
                removed_sessions += 1

        async with queue_state_lock:
            for task_id in list(queued_task_ids):
                task = active_tasks.get(task_id)
                if task and task.get("user_id") == user_id:
                    queued_task_ids.remove(task_id)

        for task_id in list(active_tasks.keys()):
            if active_tasks[task_id].get("user_id") == user_id:
                active_tasks.pop(task_id, None)

    await refresh_queue_positions()
    removed_finished_tasks = await purge_old_finished_tasks()

    await write_users(users)
    await write_sessions(sessions)

    logger.info(
        "Cleanup finished: removed_users=%s removed_sessions=%s removed_finished_tasks=%s",
        removed_users,
        removed_sessions,
        removed_finished_tasks,
    )

    return {
        "removed_users": removed_users,
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


@app.on_event("startup")
async def on_startup() -> None:
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


@web.get("/logout")
async def logout(request: Request):
    session_id = request.cookies.get(WEB_COOKIE_SESSION)
    if session_id:
        sessions = await read_sessions()
        sessions.pop(session_id, None)
        await write_sessions(sessions)
    response = RedirectResponse(url=WEB_BASE_PATH + "/logged-out", status_code=303)
    response.delete_cookie(WEB_COOKIE_SESSION, path=WEB_BASE_PATH)
    return response


@web.get("/logged-out", response_class=HTMLResponse)
async def logged_out(request: Request):
    return templates.TemplateResponse(
        request,
        "logged_out.html",
        {
            "base_path": WEB_BASE_PATH,
        },
    )


@web.get("/", response_class=HTMLResponse, name="dashboard")
async def dashboard(request: Request):
    user_id = await get_current_user_id(request)
    meta = await get_user_meta(user_id)
    history = await get_history(user_id)

    cookie_state = build_effective_cookie_state(user_id, meta)
    admin_cookie_state = build_admin_cookie_state() if meta.get("is_admin", False) else None

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
    meta = await get_user_meta(user_id)

    return {
        "user_id": user_id,
        "is_admin": meta.get("is_admin", False),
        "last_seen_at": meta.get("last_seen_at"),
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state() if meta.get("is_admin", False) else None,
    }


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

    users = await read_users()
    if user_id in users:
        users[user_id]["cookie_file"] = target.name
        users[user_id]["cookie_uploaded_at"] = iso(now_utc())
        users[user_id]["last_seen_at"] = iso(now_utc())
        await write_users(users)

    meta = await get_user_meta(user_id)
    return {
        "ok": True,
        "filename": target.name,
        "uploaded_at": iso(now_utc()),
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state() if meta.get("is_admin", False) else None,
    }


@web.post("/api/cookies/delete")
async def delete_cookies(request: Request):
    user_id = await get_current_user_id(request)
    target = COOKIES_PATH / f"cookies_{user_id}.txt"

    try:
        if target.exists():
            target.unlink()
    except Exception as e:
        logger.exception("Failed to delete cookies for user_id=%s", user_id)
        raise HTTPException(status_code=500, detail=f"Не удалось удалить cookies.txt: {e}")

    users = await read_users()
    if user_id in users:
        users[user_id]["cookie_file"] = None
        users[user_id]["cookie_uploaded_at"] = None
        users[user_id]["last_seen_at"] = iso(now_utc())
        await write_users(users)

    meta = await get_user_meta(user_id)
    return {
        "ok": True,
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state() if meta.get("is_admin", False) else None,
    }


@web.post("/api/admin/cookies/upload")
async def upload_admin_cookies(request: Request, file: UploadFile = File(...)):
    user_id = await get_current_user_id(request)
    meta = await get_user_meta(user_id)

    if not meta.get("is_admin", False):
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

    users = await read_users()
    if user_id in users:
        users[user_id]["last_seen_at"] = iso(now_utc())
        await write_users(users)

    meta = await get_user_meta(user_id)
    return {
        "ok": True,
        "cookie_state": build_effective_cookie_state(user_id, meta),
        "admin_cookie_state": build_admin_cookie_state(),
    }


@web.post("/api/admin/cookies/delete")
async def delete_admin_cookies(request: Request):
    user_id = await get_current_user_id(request)
    meta = await get_user_meta(user_id)

    if not meta.get("is_admin", False):
        raise HTTPException(status_code=403, detail="Доступно только администратору.")

    target = COOKIES_PATH / ADMIN_COOKIES_FILE

    try:
        if target.exists():
            target.unlink()
    except Exception as e:
        logger.exception("Failed to delete admin cookies")
        raise HTTPException(status_code=500, detail=f"Не удалось удалить общий cookies.txt: {e}")

    users = await read_users()
    if user_id in users:
        users[user_id]["last_seen_at"] = iso(now_utc())
        await write_users(users)

    meta = await get_user_meta(user_id)
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

    users = await read_users()
    if user_id in users:
        users[user_id]["last_seen_at"] = iso(now_utc())
        await write_users(users)

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


def parse_args() -> list[str]:
    return sys.argv[1:]


async def run_cleanup_cli() -> None:
    result = await perform_cleanup()
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    args = parse_args()
    if "--cleanup" in args:
        asyncio.run(run_cleanup_cli())
        raise SystemExit(0)

    import uvicorn

    uvicorn.run(app, host=WEB_HOST, port=WEB_PORT)