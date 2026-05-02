#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL_REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -n "${INSTALL_VERSIONS_FILE:-}" ]]; then
  VERSIONS_FILE="${INSTALL_VERSIONS_FILE}"
elif [[ -f "${LOCAL_REPO_ROOT}/install-versions.env" ]]; then
  VERSIONS_FILE="${LOCAL_REPO_ROOT}/install-versions.env"
else
  VERSIONS_FILE=""
fi

UBUNTU_VERSION="24.04"
PYTHON_VERSION="3.12"
APP_USER="botrunner"
APP_GROUP="botrunner"
APP_HOME="/opt/telegram-bots"
APP_DIR="/opt/telegram-bots/clipsave"
VENV_DIR="/opt/telegram-bots/venv"
DOWNLOAD_DIR="/download"
APP_SERVICE="clipsave"
BACKUP_DIR="/opt/telegram-bots/clipsave/backups/db"
WEB_HOST="127.0.0.1"
WEB_PORT="8093"
APT_PACKAGES="git ca-certificates curl unzip ffmpeg python3 python3-venv python3-pip sqlite3 cron ufw rsync gnupg"
ANGIE_REPO_CHANNEL="main"
ACME_RESOLVER="1.1.1.1 1.0.0.1 valid=300s ipv6=off"
NODEJS_MAJOR="22"
BGUTIL_POT_PROVIDER_VERSION="1.3.1"
BGUTIL_POT_DIR="/opt/bgutil-ytdlp-pot-provider"
BGUTIL_POT_SERVICE="bgutil-pot"
BGUTIL_POT_REPO_URL="https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git"

if [[ -n "${VERSIONS_FILE}" && -f "${VERSIONS_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${VERSIONS_FILE}"
fi

INSTALL_MODE="${INSTALL_MODE:-auto}"
GIT_REPO_URL="${GIT_REPO_URL:-}"
GIT_BRANCH="${GIT_BRANCH:-main}"
LOCAL_SOURCE_DIR="${LOCAL_SOURCE_DIR:-${LOCAL_REPO_ROOT}}"
NONINTERACTIVE="${NONINTERACTIVE:-0}"
APP_DOMAIN="${APP_DOMAIN:-}"
WEB_BASE_PATH_INPUT="${WEB_BASE_PATH_INPUT:-}"
WEB_PUBLIC_BASE_URL_INPUT="${WEB_PUBLIC_BASE_URL_INPUT:-}"
WEB_SECRET_KEY_INPUT="${WEB_SECRET_KEY_INPUT:-}"
WEB_LOGIN_KEY_INPUT="${WEB_LOGIN_KEY_INPUT:-}"
WEB_ADMIN_LOGIN_KEY_INPUT="${WEB_ADMIN_LOGIN_KEY_INPUT:-}"
ACME_EMAIL_INPUT="${ACME_EMAIL_INPUT:-}"

usage() {
  cat <<USAGE
Использование:
  sudo bash scripts/install.sh
  sudo INSTALL_MODE=git GIT_REPO_URL="https://github.com/USER/REPO.git" GIT_BRANCH="main" bash scripts/install.sh

Поддерживаемые переменные окружения:
  INSTALL_MODE=auto|local|git
  GIT_REPO_URL=https://github.com/USER/REPO.git
  GIT_BRANCH=main
  LOCAL_SOURCE_DIR=/путь/к/локальным/файлам
  APP_DOMAIN=example.com
  WEB_BASE_PATH_INPUT=/hiddenpath
  WEB_PUBLIC_BASE_URL_INPUT=https://example.com
  WEB_SECRET_KEY_INPUT=...
  WEB_LOGIN_KEY_INPUT=...
  WEB_ADMIN_LOGIN_KEY_INPUT=...
  ACME_EMAIL_INPUT=admin@example.com
  NONINTERACTIVE=1
  INSTALL_VERSIONS_FILE=/путь/к/install-versions.env
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "${EUID}" -ne 0 ]]; then
  echo "Этот скрипт нужно запускать через sudo/root" >&2
  exit 1
fi

if [[ -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  source /etc/os-release
else
  echo "Не найден /etc/os-release" >&2
  exit 1
fi

if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "${UBUNTU_VERSION}" ]]; then
  echo "Скрипт рассчитан на Ubuntu ${UBUNTU_VERSION}. Обнаружено: ${ID:-unknown} ${VERSION_ID:-unknown}" >&2
  exit 1
fi

has_local_source() {
  local d="$1"
  [[ -f "${d}/clipsave.py" && -f "${d}/requirements.txt" && -d "${d}/templates" && -d "${d}/static" && -d "${d}/deploy" ]]
}

prompt_or_default() {
  local prompt="$1"
  local default="${2:-}"
  local value=""

  if [[ "${NONINTERACTIVE}" == "1" ]]; then
    printf '%s' "${default}"
    return 0
  fi

  read -r -p "${prompt} [${default}]: " value
  if [[ -z "${value}" ]]; then
    value="${default}"
  fi
  printf '%s' "${value}"
}

gen_hex() {
  local bytes="${1:-16}"
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex "${bytes}"
  else
    python3 - <<PY
import secrets
print(secrets.token_hex(${bytes}))
PY
  fi
}

gen_alnum() {
  local length="${1:-12}"
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<PY
import secrets
import string
alphabet = string.ascii_letters + string.digits
print(''.join(secrets.choice(alphabet) for _ in range(${length})))
PY
  elif command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 48 | tr -dc 'A-Za-z0-9' | head -c "${length}"
    printf '
'
  else
    echo 'Не найден python3 или openssl для генерации WEB_BASE_PATH' >&2
    return 1
  fi
}

ensure_angie_http_include_conf_d() {
  python3 - <<'PY'
from pathlib import Path
path = Path('/etc/angie/angie.conf')
text = path.read_text(encoding='utf-8')
needle = 'include /etc/angie/conf.d/*.conf;'
if needle in text:
    raise SystemExit(0)
marker = '    include /etc/angie/http.d/*.conf;'
if marker in text:
    text = text.replace(marker, '    include /etc/angie/conf.d/*.conf;\n' + marker, 1)
else:
    http_start = text.find('http {')
    if http_start == -1:
        raise SystemExit('Не найден блок http {} в /etc/angie/angie.conf')
    insert_pos = text.find('\n}', http_start)
    if insert_pos == -1:
        raise SystemExit('Не найден конец блока http {} в /etc/angie/angie.conf')
    text = text[:insert_pos] + '\n    include /etc/angie/conf.d/*.conf;' + text[insert_pos:]
path.write_text(text, encoding='utf-8')
PY
}

disable_angie_default_site() {
  if [[ -f /etc/angie/http.d/default.conf ]]; then
    mv -f /etc/angie/http.d/default.conf /etc/angie/http.d/default.conf.disabled
  fi
}

render_acme_conf() {
  if [[ -n "${ACME_EMAIL_INPUT}" ]]; then
    cat <<EOF2
resolver ${ACME_RESOLVER};
resolver_timeout 10s;

acme_client le https://acme-v02.api.letsencrypt.org/directory email=${ACME_EMAIL_INPUT};
EOF2
  else
    cat <<EOF2
resolver ${ACME_RESOLVER};
resolver_timeout 10s;

acme_client le https://acme-v02.api.letsencrypt.org/directory;
EOF2
  fi
}


install_nodejs_runtime() {
  echo "==> Установка Node.js ${NODEJS_MAJOR}.x для yt-dlp / YouTube EJS"
  install -d -m 0755 /etc/apt/keyrings

  if ! command -v node >/dev/null 2>&1 || ! node -v | grep -Eq "^v${NODEJS_MAJOR}\."; then
    curl -fsSL "https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key" \
      | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
    chmod 644 /etc/apt/keyrings/nodesource.gpg

    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_${NODEJS_MAJOR}.x nodistro main" \
      > /etc/apt/sources.list.d/nodesource.list

    apt-get update
    apt-get install -y nodejs
  else
    echo "Node.js уже установлен: $(node -v)"
  fi

  if ! command -v node >/dev/null 2>&1; then
    echo "Node.js не установлен или не найден в PATH" >&2
    exit 1
  fi

  local node_major
  node_major="$(node -v | sed -E 's/^v([0-9]+).*/\1/')"
  if [[ "${node_major}" -lt 20 ]]; then
    echo "Нужен Node.js 20+ для стабильной работы yt-dlp EJS. Сейчас: $(node -v)" >&2
    exit 1
  fi

  echo "Node.js: $(command -v node) $(node -v)"
}


install_bgutil_pot_provider() {
  echo "==> Установка PO Token Provider для yt-dlp / YouTube GVS"
  VENV_DIR="${VENV_DIR}" \
  BGUTIL_POT_PROVIDER_VERSION="${BGUTIL_POT_PROVIDER_VERSION}" \
  BGUTIL_POT_DIR="${BGUTIL_POT_DIR}" \
  BGUTIL_POT_SERVICE="${BGUTIL_POT_SERVICE}" \
  BGUTIL_POT_REPO_URL="${BGUTIL_POT_REPO_URL}" \
    bash "${APP_DIR}/scripts/install_bgutil_pot.sh"
}

echo "==> Подготовка базовых пакетов"
apt-get update
apt-get install -y ca-certificates curl git gnupg

if [[ "${INSTALL_MODE}" == "auto" ]]; then
  if [[ -n "${GIT_REPO_URL}" ]]; then
    INSTALL_MODE="git"
  else
    INSTALL_MODE="local"
  fi
fi

TMP_CLONE_DIR=""
cleanup() {
  if [[ -n "${TMP_CLONE_DIR}" && -d "${TMP_CLONE_DIR}" ]]; then
    rm -rf "${TMP_CLONE_DIR}"
  fi
}
trap cleanup EXIT

if [[ "${INSTALL_MODE}" == "git" ]]; then
  if [[ -z "${GIT_REPO_URL}" ]]; then
    echo "Для INSTALL_MODE=git нужно задать GIT_REPO_URL" >&2
    exit 1
  fi

  TMP_CLONE_DIR="$(mktemp -d /tmp/clipsave_src.XXXXXX)"
  echo "==> Клонирование репозитория: ${GIT_REPO_URL} (branch: ${GIT_BRANCH})"
  git clone --depth 1 --branch "${GIT_BRANCH}" "${GIT_REPO_URL}" "${TMP_CLONE_DIR}"
  SOURCE_ROOT="${TMP_CLONE_DIR}"

  if [[ -f "${SOURCE_ROOT}/install-versions.env" ]]; then
    # shellcheck disable=SC1090
    source "${SOURCE_ROOT}/install-versions.env"
  fi
elif [[ "${INSTALL_MODE}" == "local" ]]; then
  if ! has_local_source "${LOCAL_SOURCE_DIR}"; then
    echo "Не найден локальный набор файлов проекта в ${LOCAL_SOURCE_DIR}" >&2
    echo "Ожидаются: clipsave.py, requirements.txt, templates/, static/, deploy/" >&2
    exit 1
  fi
  SOURCE_ROOT="${LOCAL_SOURCE_DIR}"
else
  echo "Неизвестный INSTALL_MODE: ${INSTALL_MODE}" >&2
  exit 1
fi

DEFAULT_BASE_PATH="/$(gen_alnum 12)"
if [[ -z "${APP_DOMAIN}" ]]; then
  APP_DOMAIN="$(prompt_or_default 'Введите домен сервиса' 'example.com')"
fi
if [[ -z "${WEB_BASE_PATH_INPUT}" ]]; then
  WEB_BASE_PATH_INPUT="${DEFAULT_BASE_PATH}"
fi
if [[ "${WEB_BASE_PATH_INPUT}" != /* ]]; then
  WEB_BASE_PATH_INPUT="/${WEB_BASE_PATH_INPUT}"
fi
WEB_BASE_PATH_INPUT="${WEB_BASE_PATH_INPUT%/}"

if [[ -z "${WEB_PUBLIC_BASE_URL_INPUT}" ]]; then
  WEB_PUBLIC_BASE_URL_INPUT="https://${APP_DOMAIN}"
fi
if [[ -z "${WEB_SECRET_KEY_INPUT}" ]]; then
  WEB_SECRET_KEY_INPUT="$(gen_hex 32)"
fi
if [[ -z "${WEB_LOGIN_KEY_INPUT}" ]]; then
  WEB_LOGIN_KEY_INPUT="$(gen_hex 16)"
fi
if [[ -z "${WEB_ADMIN_LOGIN_KEY_INPUT}" ]]; then
  WEB_ADMIN_LOGIN_KEY_INPUT="$(gen_hex 16)"
fi
if [[ -z "${ACME_EMAIL_INPUT}" ]]; then
  ACME_EMAIL_INPUT="$(prompt_or_default "Введите email для ACME/Let's Encrypt (можно оставить пустым)" '')"
fi

echo "==> Установка пакетов ОС"
install_nodejs_runtime
apt-get update
apt-get install -y ${APT_PACKAGES}

install -d -m 0755 /etc/apt/keyrings
curl -fsSL https://angie.software/keys/angie-signing.gpg | gpg --dearmor -o /etc/apt/keyrings/angie-signing.gpg
chmod 644 /etc/apt/keyrings/angie-signing.gpg

echo "deb [signed-by=/etc/apt/keyrings/angie-signing.gpg] https://download.angie.software/angie/$(. /etc/os-release && echo "$ID/$VERSION_ID $VERSION_CODENAME") ${ANGIE_REPO_CHANNEL}" \
  > /etc/apt/sources.list.d/angie.list

apt-get update
apt-get install -y angie
systemctl enable --now angie
systemctl enable --now cron

echo "==> Создание пользователя и каталогов"
if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  adduser --home "${APP_HOME}" --shell /bin/bash --disabled-password --gecos "" "${APP_USER}"
fi

mkdir -p "${APP_HOME}"
mkdir -p "${APP_DIR}"
mkdir -p "${DOWNLOAD_DIR}"
mkdir -p \
  "${APP_DIR}/static" \
  "${APP_DIR}/templates" \
  "${APP_DIR}/cookies" \
  "${APP_DIR}/data" \
  "${APP_DIR}/logs" \
  "${APP_DIR}/deploy" \
  "${APP_DIR}/scripts" \
  "${APP_DIR}/backups/db"

echo "==> Копирование файлов проекта"
rsync -a \
  --exclude '.git/' \
  --exclude '.github/' \
  --exclude '.venv/' \
  --exclude 'venv/' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  --exclude 'data/' \
  --exclude 'logs/' \
  --exclude 'cookies/' \
  --exclude 'backups/' \
  --exclude '.env' \
  "${SOURCE_ROOT}/" "${APP_DIR}/"

chown -R "${APP_USER}:${APP_GROUP}" "${APP_HOME}"
chown -R "${APP_USER}:${APP_GROUP}" "${DOWNLOAD_DIR}"

echo "==> Создание виртуального окружения"
if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  sudo -u "${APP_USER}" -H python3 -m venv "${VENV_DIR}"
fi
sudo -u "${APP_USER}" -H bash -lc "source '${VENV_DIR}/bin/activate' && pip install --upgrade pip setuptools wheel && pip install -r '${APP_DIR}/requirements.txt'"
install_bgutil_pot_provider

ENV_FILE="${APP_DIR}/.env"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "==> Создание .env"
  cat > "${ENV_FILE}" <<ENVEOF
# =========================
# WEB SERVER
# =========================

WEB_HOST=${WEB_HOST}
# Порт локального FastAPI / Uvicorn
WEB_PORT=${WEB_PORT}

# Базовый путь сервиса без слеша в конце
WEB_BASE_PATH=${WEB_BASE_PATH_INPUT}

# Внешний базовый URL сервиса для генерации правильных ссылок в админке.
# Пример: https://example.com
# Если оставить пустым, сервис попробует собрать URL из входящего запроса.
WEB_PUBLIC_BASE_URL=${WEB_PUBLIC_BASE_URL_INPUT}

# =========================
# БЕЗОПАСНОСТЬ
# =========================

# Секрет для подписи cookies (ОБЯЗАТЕЛЬНО поменять)
WEB_SECRET_KEY=${WEB_SECRET_KEY_INPUT}

# Основная ссылка входа: /login?key=...
WEB_LOGIN_KEY=${WEB_LOGIN_KEY_INPUT}

# Админская ссылка входа: /login?key=...
WEB_ADMIN_LOGIN_KEY=${WEB_ADMIN_LOGIN_KEY_INPUT}

# Названия cookies
WEB_COOKIE_UID=clipsave_uid
WEB_COOKIE_SESSION=clipsave_session

# Время жизни cookies
WEB_UID_MAX_AGE=15552000
WEB_SESSION_MAX_AGE=604800

# =========================
# ОГРАНИЧЕНИЯ И ОЧЕРЕДЬ
# =========================

MAX_ACTIVE_TASKS=1
MAX_ACTIVE_TASKS_PER_USER=1

# =========================
# SQLITE / ХРАНЕНИЕ
# =========================

# Каталог данных сервиса
DATA_PATH=./data

# Можно указать либо имя файла БД внутри DATA_PATH...
SQLITE_DB_NAME=clipsave.sqlite3

# ...либо абсолютный/относительный путь целиком.
# Если SQLITE_PATH задан, он имеет приоритет над SQLITE_DB_NAME.
# SQLITE_PATH=./data/clipsave.sqlite3

# Через сколько дней чистить старых универсальных пользователей
USER_RETENTION_DAYS=30

# Через сколько часов удалять старые завершённые задачи из памяти
REQUEST_TTL_HOURS=1

# =========================
# ПУТИ
# =========================

DOWNLOAD_PATH=${DOWNLOAD_DIR}
COOKIES_PATH=./cookies
LOG_PATH=./logs

# =========================
# YT-DLP / DEBUG
# =========================

DEBUG_YTDLP=0

# =========================
# АДМИНИСТРАТОР
# =========================

ADMIN_COOKIES_FILE=admin_cookies.txt
ENVEOF
  chown "${APP_USER}:${APP_GROUP}" "${ENV_FILE}"
  chmod 640 "${ENV_FILE}"
else
  echo "==> .env уже существует, пропускаю создание"
fi

echo "==> Установка systemd-службы"
sed \
  -e "s|__APP_USER__|${APP_USER}|g" \
  -e "s|__APP_GROUP__|${APP_GROUP}|g" \
  -e "s|__APP_DIR__|${APP_DIR}|g" \
  -e "s|__VENV_DIR__|${VENV_DIR}|g" \
  "${APP_DIR}/deploy/systemd/clipsave.service" > "/etc/systemd/system/${APP_SERVICE}.service"

systemctl daemon-reload
systemctl enable --now "${APP_SERVICE}"

echo "==> Настройка Angie"
mkdir -p /etc/angie/conf.d /etc/angie/http.d
ensure_angie_http_include_conf_d
disable_angie_default_site
render_acme_conf > /etc/angie/http.d/00-acme.conf

sed \
  -e "s|__APP_DOMAIN__|${APP_DOMAIN}|g" \
  -e "s|__WEB_BASE_PATH__|${WEB_BASE_PATH_INPUT}|g" \
  -e "s|__WEB_PORT__|${WEB_PORT}|g" \
  "${APP_DIR}/deploy/angie/site.conf.example" > "/etc/angie/http.d/${APP_DOMAIN}.conf"

angie -t
systemctl reload angie

echo "==> Настройка UFW"
ufw allow OpenSSH >/dev/null 2>&1 || true
ufw allow 80/tcp >/dev/null 2>&1 || true
ufw allow 443/tcp >/dev/null 2>&1 || true
ufw --force enable >/dev/null 2>&1 || true

echo "==> Установка cron-заданий"
cat > /etc/cron.d/clipsave <<CRONEOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Автоудаление скачанных файлов выполняет сам веб-сервис ClipSave через SQLite-настройки.
0 4 * * * root before=\$(/usr/bin/sudo -u ${APP_USER} -H bash -lc 'source ${VENV_DIR}/bin/activate && python -c "import importlib.metadata as m; pkgs=m.packages_distributions(); names=(\"yt-dlp\",\"yt-dlp-ejs\",\"bgutil-ytdlp-pot-provider\"); [print(f\"{n}=\" + (m.version(n) if n in pkgs else \"NOT_INSTALLED\")) for n in names]"'); /usr/bin/sudo -u ${APP_USER} -H bash -lc 'source ${VENV_DIR}/bin/activate && pip install -U --no-deps yt-dlp yt-dlp-ejs bgutil-ytdlp-pot-provider'; after=\$(/usr/bin/sudo -u ${APP_USER} -H bash -lc 'source ${VENV_DIR}/bin/activate && python -c "import importlib.metadata as m; pkgs=m.packages_distributions(); names=(\"yt-dlp\",\"yt-dlp-ejs\",\"bgutil-ytdlp-pot-provider\"); [print(f\"{n}=\" + (m.version(n) if n in pkgs else \"NOT_INSTALLED\")) for n in names]"'); [ "\$before" != "\$after" ] && /usr/bin/systemctl restart ${APP_SERVICE} || true
CRONEOF
chmod 644 /etc/cron.d/clipsave

echo
if [[ "${INSTALL_MODE}" == "git" ]]; then
  echo "Исходники развернуты из Git-репозитория: ${GIT_REPO_URL}"
else
  echo "Исходники развернуты из локального набора файлов: ${SOURCE_ROOT}"
fi

echo "Готово."
echo "Домен:               ${APP_DOMAIN}"
echo "WEB_BASE_PATH:       ${WEB_BASE_PATH_INPUT}"
echo "Пользователь:        ${APP_USER}"
echo "Каталог проекта:     ${APP_DIR}"
echo "Виртуальное окруж.:  ${VENV_DIR}"
echo "Служба:              ${APP_SERVICE}"
echo
echo "Ссылки входа:"
echo "  Пользователь: ${WEB_PUBLIC_BASE_URL_INPUT}${WEB_BASE_PATH_INPUT}/login?key=${WEB_LOGIN_KEY_INPUT}"
echo "  Админ:        ${WEB_PUBLIC_BASE_URL_INPUT}${WEB_BASE_PATH_INPUT}/login?key=${WEB_ADMIN_LOGIN_KEY_INPUT}"
echo
echo "Полезные команды:"
echo "  systemctl status ${APP_SERVICE} --no-pager"
echo "  node -v"
echo "  systemctl status angie --no-pager"
echo "  systemctl status ${BGUTIL_POT_SERVICE} --no-pager"
echo "  curl -sS http://127.0.0.1:4416/ping"
echo "  ufw status verbose"
echo "  grep -i acme /var/log/angie/error.log | tail -n 50"
echo "  ls -la /var/lib/angie/acme/le/"
echo
echo "Если HTTPS сразу не поднимется, сначала проверь внешнюю доступность TCP/80 до сервера."
