#!/usr/bin/env bash
set -Eeuo pipefail

# ClipSave full migration and SQLite backup tool
# Designed for the ClipSave layout used by scripts/install.sh.

APP_USER="${APP_USER:-botrunner}"
APP_GROUP="${APP_GROUP:-botrunner}"
APP_HOME="${APP_HOME:-/opt/telegram-bots}"
APP_DIR="${APP_DIR:-/opt/telegram-bots/clipsave}"
VENV_DIR="${VENV_DIR:-/opt/telegram-bots/venv}"
DOWNLOAD_DIR="${DOWNLOAD_DIR:-/download}"
SERVICE_NAME="${SERVICE_NAME:-clipsave}"
BACKUP_ROOT="${BACKUP_ROOT:-/root/clipsave-transfer}"
FULL_BACKUP_DIR="${FULL_BACKUP_DIR:-${BACKUP_ROOT}/full}"
REMOTE_BACKUP_DIR="${REMOTE_BACKUP_DIR:-/root/clipsave-transfer}"
NODEJS_MAJOR="${NODEJS_MAJOR:-22}"
ANGIE_REPO_CHANNEL="${ANGIE_REPO_CHANNEL:-main}"
BGUTIL_POT_SERVICE="${BGUTIL_POT_SERVICE:-bgutil-pot}"
SELF_TARGET="${BACKUP_ROOT}/clipsave_migrate.sh"

if [[ -t 1 ]]; then
  C_RESET=$'\033[0m'
  C_RED=$'\033[31m'
  C_GREEN=$'\033[32m'
  C_YELLOW=$'\033[33m'
  C_BLUE=$'\033[34m'
  C_MAGENTA=$'\033[35m'
  C_CYAN=$'\033[36m'
  C_BOLD=$'\033[1m'
else
  C_RESET=""; C_RED=""; C_GREEN=""; C_YELLOW=""; C_BLUE=""; C_MAGENTA=""; C_CYAN=""; C_BOLD=""
fi

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null || printf '%s' "${BASH_SOURCE[0]}")"

print_title() {
  clear 2>/dev/null || true
  printf '%s\n' "${C_CYAN}${C_BOLD}========================================${C_RESET}"
  printf '%s\n' "${C_CYAN}${C_BOLD}  ClipSave migration / backup tool${C_RESET}"
  printf '%s\n' "${C_CYAN}${C_BOLD}========================================${C_RESET}"
  echo
}

section() {
  echo
  printf '%s\n' "${C_MAGENTA}${C_BOLD}==> $*${C_RESET}"
}

ok() { printf '%s\n' "${C_GREEN}Готово:${C_RESET} $*"; }
warn() { printf '%s\n' "${C_YELLOW}Внимание:${C_RESET} $*"; }
err() { printf '%s\n' "${C_RED}Ошибка:${C_RESET} $*" >&2; }
info() { printf '%s\n' "${C_BLUE}Инфо:${C_RESET} $*"; }

pause_enter() {
  echo
  read -r -p "Нажми Enter для продолжения..." _ || true
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "скрипт нужно запускать от root или через sudo."
    exit 1
  fi
}

confirm() {
  local prompt="${1:-Продолжить? [y/N]: }"
  local answer=""
  read -r -p "$prompt" answer || true
  [[ "$answer" =~ ^([Yy]|[Yy][Ee][Ss]|[Дд][Аа])$ ]]
}

safe_mkdir() {
  mkdir -p "$1"
  chmod 700 "$1" 2>/dev/null || true
}

format_bytes() {
  local bytes="${1:-0}"
  if [[ ! "$bytes" =~ ^[0-9]+$ ]]; then
    echo "?"
    return
  fi
  if (( bytes >= 1073741824 )); then
    awk -v b="$bytes" 'BEGIN { printf "%.1fG", b/1024/1024/1024 }'
  elif (( bytes >= 1048576 )); then
    awk -v b="$bytes" 'BEGIN { printf "%.1fM", b/1024/1024 }'
  elif (( bytes >= 1024 )); then
    awk -v b="$bytes" 'BEGIN { printf "%.1fK", b/1024 }'
  else
    printf '%sB' "$bytes"
  fi
}

copy_self_to_backup_root() {
  safe_mkdir "$BACKUP_ROOT"
  cp -f "$SCRIPT_PATH" "$SELF_TARGET"
  chmod 700 "$SELF_TARGET"
}

sha256_write() {
  local file="$1"
  (cd "$(dirname "$file")" && sha256sum "$(basename "$file")" > "$(basename "$file").sha256")
}

sha256_verify_strict() {
  local file="$1"
  local sum_file="${file}.sha256"
  if [[ ! -f "$sum_file" ]]; then
    err "рядом с архивом не найден файл контрольной суммы: $sum_file"
    err "восстановление остановлено, чтобы не распаковывать непроверенный архив."
    return 1
  fi
  (cd "$(dirname "$file")" && sha256sum -c "$(basename "$sum_file")")
}

sha256_verify_soft() {
  local file="$1"
  local sum_file="${file}.sha256"
  if [[ -f "$sum_file" ]]; then
    (cd "$(dirname "$file")" && sha256sum -c "$(basename "$sum_file")")
    return $?
  fi
  warn "для файла нет .sha256: $file"
  confirm "Продолжить без проверки контрольной суммы? [y/N]: "
}

path_exists_for_backup() {
  [[ -e "$1" || -L "$1" ]]
}

add_existing_path() {
  local abs="$1"
  local -n out_ref="$2"
  if path_exists_for_backup "$abs"; then
    out_ref+=("${abs#/}")
  fi
}

current_timestamp() {
  date -u +"%Y-%m-%d_%H-%M-%S_UTC"
}

make_full_backup() {
  require_root
  print_title
  section "Создание полного архива переноса"

  safe_mkdir "$FULL_BACKUP_DIR"
  copy_self_to_backup_root

  if [[ ! -d "$APP_DIR" ]]; then
    err "каталог сервиса не найден: $APP_DIR"
    return 1
  fi

  local include_download="yes"
  if [[ -d "$DOWNLOAD_DIR" ]]; then
    local dl_size="0"
    dl_size="$(du -sb "$DOWNLOAD_DIR" 2>/dev/null | awk '{print $1}' || echo 0)"
    echo "Каталог загрузок: $DOWNLOAD_DIR ($(format_bytes "$dl_size"))"
    if ! confirm "Включить каталог загрузок в полный архив? [y/N]: "; then
      include_download="no"
    fi
  else
    include_download="no"
  fi

  local ts archive manifest paths_file
  ts="$(current_timestamp)"
  archive="${FULL_BACKUP_DIR}/clipsave_full_${ts}.tar.gz"
  manifest="$(mktemp /tmp/clipsave_manifest.XXXXXX)"
  paths_file="$(mktemp /tmp/clipsave_paths.XXXXXX)"

  local paths=()
  add_existing_path "$APP_DIR" paths
  add_existing_path "/etc/angie" paths
  add_existing_path "/var/lib/angie/acme" paths
  add_existing_path "/etc/letsencrypt" paths
  add_existing_path "/etc/systemd/system/${SERVICE_NAME}.service" paths
  add_existing_path "/etc/systemd/system/${BGUTIL_POT_SERVICE}.service" paths
  add_existing_path "/etc/cron.d/clipsave" paths
  add_existing_path "/opt/bgutil-ytdlp-pot-provider" paths
  add_existing_path "$SELF_TARGET" paths

  if [[ "$include_download" == "yes" ]]; then
    add_existing_path "$DOWNLOAD_DIR" paths
  fi

  if [[ "${#paths[@]}" -eq 0 ]]; then
    rm -f "$manifest" "$paths_file"
    err "не найдено ни одного пути для архивации."
    return 1
  fi

  {
    echo "CLIPSAVE_BACKUP_FORMAT=1"
    echo "CREATED_UTC=${ts}"
    echo "HOSTNAME=$(hostname -f 2>/dev/null || hostname)"
    echo "APP_DIR=${APP_DIR}"
    echo "VENV_DIR=${VENV_DIR}"
    echo "DOWNLOAD_DIR=${DOWNLOAD_DIR}"
    echo "SERVICE_NAME=${SERVICE_NAME}"
    echo "APP_USER=${APP_USER}"
    echo "APP_GROUP=${APP_GROUP}"
    echo "INCLUDES_DOWNLOAD=${include_download}"
  } > "$manifest"

  printf '%s\n' "${paths[@]}" > "$paths_file"

  echo
  echo "В архив попадут:"
  sed 's|^|  /|' "$paths_file"
  echo
  echo "Архив: $archive"

  tar -czpf "$archive" \
    -C "$(dirname "$manifest")" "$(basename "$manifest")" \
    -C "$(dirname "$paths_file")" "$(basename "$paths_file")" \
    -C / -T "$paths_file"

  mv -f "$manifest" "${archive}.manifest.txt"
  mv -f "$paths_file" "${archive}.paths.txt"
  sha256_write "$archive"

  ok "полный архив создан"
  echo "  $archive"
  echo "  ${archive}.sha256"
  echo "  ${archive}.manifest.txt"
  echo "  ${archive}.paths.txt"
}

transfer_latest_backup() {
  require_root
  print_title
  section "Отправка архива на новый сервер"

  safe_mkdir "$FULL_BACKUP_DIR"
  copy_self_to_backup_root

  local latest=""
  latest="$(find "$FULL_BACKUP_DIR" -maxdepth 1 -type f -name 'clipsave_full_*.tar.gz' -printf '%T@ %p\n' 2>/dev/null | sort -nr | awk 'NR==1 {print $2}')"
  if [[ -z "$latest" ]]; then
    err "полные архивы не найдены. Сначала создай полный архив."
    return 1
  fi

  if [[ ! -f "${latest}.sha256" ]]; then
    err "для последнего архива нет контрольной суммы: ${latest}.sha256"
    return 1
  fi

  echo "Будет отправлен архив:"
  echo "  $latest"
  echo "  ${latest}.sha256"
  echo "  $SELF_TARGET"
  echo

  local remote_host remote_port remote_user key_path ssh_target scp_opts=()
  read -r -p "IP или домен нового сервера: " remote_host
  read -r -p "SSH-порт [22]: " remote_port
  remote_port="${remote_port:-22}"
  read -r -p "SSH-пользователь [root]: " remote_user
  remote_user="${remote_user:-root}"
  read -r -p "Путь к SSH-ключу, если нужен. Можно оставить пустым: " key_path

  ssh_target="${remote_user}@${remote_host}"
  scp_opts=(-P "$remote_port")
  if [[ -n "$key_path" ]]; then
    scp_opts+=(-i "$key_path")
  fi

  info "если используется вход по паролю, SSH сам запросит пароль."
  ssh "${scp_opts[@]/-P/-p}" "$ssh_target" "mkdir -p '$REMOTE_BACKUP_DIR/full' && chmod 700 '$REMOTE_BACKUP_DIR' '$REMOTE_BACKUP_DIR/full'" || return 1
  scp "${scp_opts[@]}" "$latest" "${latest}.sha256" "$SELF_TARGET" "${ssh_target}:${REMOTE_BACKUP_DIR}/" || return 1
  ssh "${scp_opts[@]/-P/-p}" "$ssh_target" "chmod 700 '${REMOTE_BACKUP_DIR}/clipsave_migrate.sh' && mv -f '${REMOTE_BACKUP_DIR}/$(basename "$latest")' '${REMOTE_BACKUP_DIR}/full/' && mv -f '${REMOTE_BACKUP_DIR}/$(basename "$latest").sha256' '${REMOTE_BACKUP_DIR}/full/'" || return 1

  ok "архив и скрипт отправлены на новый сервер"
  echo "На новом сервере запусти:"
  echo "  sudo bash ${REMOTE_BACKUP_DIR}/clipsave_migrate.sh"
}

ensure_ubuntu_2404() {
  if [[ ! -r /etc/os-release ]]; then
    err "не найден /etc/os-release."
    return 1
  fi
  # shellcheck disable=SC1091
  source /etc/os-release
  if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != "24.04" ]]; then
    warn "install.sh проекта рассчитан на Ubuntu 24.04. Обнаружено: ${ID:-unknown} ${VERSION_ID:-unknown}"
    confirm "Продолжить восстановление на этой системе? [y/N]: " || return 1
  fi
}

install_base_packages() {
  section "Установка недостающих системных пакетов"
  apt-get update
  apt-get install -y ca-certificates curl git gnupg rsync sqlite3 cron ufw unzip tar ffmpeg python3 python3-venv python3-pip openssh-client
}

install_nodejs_runtime() {
  section "Проверка Node.js ${NODEJS_MAJOR}.x"
  install -d -m 0755 /etc/apt/keyrings
  if ! command -v node >/dev/null 2>&1 || ! node -v | grep -Eq "^v${NODEJS_MAJOR}\."; then
    curl -fsSL "https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key" | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg
    chmod 644 /etc/apt/keyrings/nodesource.gpg
    echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_${NODEJS_MAJOR}.x nodistro main" > /etc/apt/sources.list.d/nodesource.list
    apt-get update
    apt-get install -y nodejs
  fi
  ok "Node.js: $(command -v node 2>/dev/null || echo unknown) $(node -v 2>/dev/null || echo unknown)"
}

install_angie_runtime() {
  section "Проверка Angie"
  install -d -m 0755 /etc/apt/keyrings
  if ! command -v angie >/dev/null 2>&1; then
    curl -fsSL https://angie.software/keys/angie-signing.gpg | gpg --dearmor -o /etc/apt/keyrings/angie-signing.gpg
    chmod 644 /etc/apt/keyrings/angie-signing.gpg
    echo "deb [signed-by=/etc/apt/keyrings/angie-signing.gpg] https://download.angie.software/angie/$(. /etc/os-release && echo "$ID/$VERSION_ID $VERSION_CODENAME") ${ANGIE_REPO_CHANNEL}" > /etc/apt/sources.list.d/angie.list
    apt-get update
    apt-get install -y angie
  fi
  systemctl enable angie >/dev/null 2>&1 || true
  ok "Angie установлен"
}

ensure_app_user_and_dirs() {
  section "Пользователь, каталоги и права"
  if ! id -u "$APP_USER" >/dev/null 2>&1; then
    adduser --home "$APP_HOME" --shell /bin/bash --disabled-password --gecos "" "$APP_USER"
  fi
  mkdir -p "$APP_HOME" "$APP_DIR" "$DOWNLOAD_DIR" "$APP_DIR/data" "$APP_DIR/logs" "$APP_DIR/cookies" "$APP_DIR/backups/db"
  chown -R "${APP_USER}:${APP_GROUP}" "$APP_HOME" "$DOWNLOAD_DIR" 2>/dev/null || true
  chmod 750 "$APP_HOME" 2>/dev/null || true
  ok "права обновлены"
}

rebuild_python_env() {
  section "Виртуальное окружение Python"
  if [[ ! -f "${APP_DIR}/requirements.txt" ]]; then
    err "не найден requirements.txt: ${APP_DIR}/requirements.txt"
    return 1
  fi
  if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
    sudo -u "$APP_USER" -H python3 -m venv "$VENV_DIR"
  fi
  sudo -u "$APP_USER" -H bash -lc "source '${VENV_DIR}/bin/activate' && pip install --upgrade pip setuptools wheel && pip install -r '${APP_DIR}/requirements.txt'"
  ok "зависимости Python установлены"
}

reinstall_bgutil_if_possible() {
  section "PO Token Provider для yt-dlp"
  if [[ -f "${APP_DIR}/scripts/install_bgutil_pot.sh" ]]; then
    VENV_DIR="$VENV_DIR" bash "${APP_DIR}/scripts/install_bgutil_pot.sh" || warn "install_bgutil_pot.sh завершился с ошибкой. Проверь отдельно: systemctl status ${BGUTIL_POT_SERVICE} --no-pager"
  else
    warn "скрипт install_bgutil_pot.sh не найден, этап пропущен."
  fi
}

validate_and_start_services() {
  section "Проверка и запуск сервисов"
  systemctl daemon-reload
  if [[ -f "/etc/systemd/system/${SERVICE_NAME}.service" ]]; then
    systemctl enable "$SERVICE_NAME" >/dev/null 2>&1 || true
  else
    warn "systemd-файл не найден: /etc/systemd/system/${SERVICE_NAME}.service"
  fi

  if [[ -f "${APP_DIR}/clipsave.py" ]]; then
    sudo -u "$APP_USER" -H "${VENV_DIR}/bin/python" -m py_compile "${APP_DIR}/clipsave.py"
    ok "clipsave.py прошёл проверку синтаксиса"
  fi

  if command -v angie >/dev/null 2>&1; then
    angie -t
    systemctl restart angie
    ok "Angie проверен и перезапущен"
  fi

  if [[ -f "/etc/systemd/system/${SERVICE_NAME}.service" ]]; then
    systemctl restart "$SERVICE_NAME"
    systemctl is-active "$SERVICE_NAME" >/dev/null
    ok "служба ${SERVICE_NAME} запущена"
  fi

  systemctl enable --now cron >/dev/null 2>&1 || true
  ufw allow OpenSSH >/dev/null 2>&1 || true
  ufw allow 80/tcp >/dev/null 2>&1 || true
  ufw allow 443/tcp >/dev/null 2>&1 || true
}

find_full_archives() {
  mapfile -t FULL_ARCHIVES < <(
    find "$BACKUP_ROOT" "$FULL_BACKUP_DIR" /root "$(pwd)" -maxdepth 3 -type f -name 'clipsave_full_*.tar.gz' 2>/dev/null | sort -r -u
  )
}

choose_full_archive() {
  find_full_archives
  if [[ "${#FULL_ARCHIVES[@]}" -eq 0 ]]; then
    err "архивы переноса не найдены. Ожидал clipsave_full_*.tar.gz в ${BACKUP_ROOT}, /root или текущем каталоге."
    return 1
  fi

  echo "Найденные архивы переноса:"
  local i=1 f size mtime
  for f in "${FULL_ARCHIVES[@]}"; do
    size="$(stat -c '%s' "$f" 2>/dev/null || echo 0)"
    mtime="$(stat -c '%y' "$f" 2>/dev/null | cut -d'.' -f1 || true)"
    printf '  %s) %s | %s | %s\n' "$i" "$f" "$(format_bytes "$size")" "$mtime"
    i=$((i + 1))
  done
  echo

  local choice
  read -r -p "Выбери архив для восстановления: " choice
  if [[ ! "$choice" =~ ^[0-9]+$ ]] || (( choice < 1 || choice > ${#FULL_ARCHIVES[@]} )); then
    err "неверный номер архива."
    return 1
  fi
  SELECTED_FULL_ARCHIVE="${FULL_ARCHIVES[$((choice - 1))]}"
}

make_pre_restore_snapshot() {
  local ts target paths=()
  ts="$(current_timestamp)"
  target="${BACKUP_ROOT}/pre_restore_${ts}.tar.gz"
  safe_mkdir "$BACKUP_ROOT"
  add_existing_path "$APP_DIR" paths
  add_existing_path "/etc/angie" paths
  add_existing_path "/etc/systemd/system/${SERVICE_NAME}.service" paths
  add_existing_path "/etc/cron.d/clipsave" paths
  if [[ "${#paths[@]}" -gt 0 ]]; then
    tar -czpf "$target" -C / "${paths[@]}" || warn "не удалось создать аварийный снимок текущего состояния."
    if [[ -f "$target" ]]; then
      sha256_write "$target"
      info "аварийный снимок перед восстановлением: $target"
    fi
  fi
}

restore_full_backup() {
  require_root
  print_title
  section "Восстановление полного архива ClipSave"

  choose_full_archive || return 1
  local archive="$SELECTED_FULL_ARCHIVE"

  section "Проверка контрольной суммы"
  sha256_verify_strict "$archive" || return 1
  ok "контрольная сумма совпала"

  echo
  echo "Будет восстановлен архив:"
  echo "  $archive"
  echo
  warn "текущие файлы сервиса, Angie, сертификаты и systemd-настройки могут быть заменены содержимым архива."
  confirm "Подтвердить восстановление? [y/N]: " || { echo "Отменено."; return 0; }

  ensure_ubuntu_2404
  install_base_packages
  install_nodejs_runtime
  install_angie_runtime

  section "Остановка служб перед распаковкой"
  systemctl stop "$SERVICE_NAME" >/dev/null 2>&1 || true
  systemctl stop angie >/dev/null 2>&1 || true
  ok "службы остановлены или не были активны"

  make_pre_restore_snapshot

  section "Распаковка архива"
  tar -xzpf "$archive" -C / --exclude='tmp.*' --exclude='clipsave_manifest.*' --exclude='clipsave_paths.*'
  ok "файлы распакованы"

  ensure_app_user_and_dirs
  rebuild_python_env
  reinstall_bgutil_if_possible
  validate_and_start_services
  copy_self_to_backup_root

  section "Итог восстановления"
  ok "восстановление завершено"
  echo "Проверь DNS домена: он должен указывать на IP нового сервера."
  echo "Полезные проверки:"
  echo "  systemctl status ${SERVICE_NAME} --no-pager -l"
  echo "  systemctl status angie --no-pager -l"
  echo "  angie -t"
}

resolve_env_file() {
  if [[ -n "${ENV_FILE:-}" && -f "$ENV_FILE" ]]; then
    printf '%s\n' "$ENV_FILE"
  elif [[ -f "${APP_DIR}/.env" ]]; then
    printf '%s\n' "${APP_DIR}/.env"
  else
    return 1
  fi
}

resolve_service_paths_from_env() {
  local env_file project_dir data_path_value download_path_value
  env_file="$(resolve_env_file)" || { err "не найден .env: ${APP_DIR}/.env"; return 1; }
  project_dir="$(cd "$(dirname "$env_file")" && pwd)"

  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a

  DATA_PATH_VALUE="${DATA_PATH:-./data}"
  if [[ "$DATA_PATH_VALUE" = /* ]]; then
    DATA_PATH_RESOLVED="$DATA_PATH_VALUE"
  else
    DATA_PATH_RESOLVED="${project_dir}/${DATA_PATH_VALUE}"
  fi

  if [[ -n "${SQLITE_PATH:-}" ]]; then
    if [[ "$SQLITE_PATH" = /* ]]; then
      DB_PATH="$SQLITE_PATH"
    else
      DB_PATH="${project_dir}/${SQLITE_PATH}"
    fi
  else
    SQLITE_DB_NAME="${SQLITE_DB_NAME:-clipsave.sqlite3}"
    DB_PATH="${DATA_PATH_RESOLVED%/}/${SQLITE_DB_NAME}"
  fi

  DOWNLOAD_PATH_VALUE="${DOWNLOAD_PATH:-$DOWNLOAD_DIR}"
  if [[ "$DOWNLOAD_PATH_VALUE" = /* ]]; then
    DOWNLOAD_PATH_RESOLVED="$DOWNLOAD_PATH_VALUE"
  else
    DOWNLOAD_PATH_RESOLVED="${project_dir}/${DOWNLOAD_PATH_VALUE}"
  fi

  DB_BACKUP_DIR="${APP_DIR}/backups/db"
  mkdir -p "$DB_BACKUP_DIR"
}

sqlite_backup_to_file() {
  local source_db="$1" target_db="$2"
  sqlite3 "$source_db" <<SQL
.timeout 10000
.backup '${target_db}'
SQL
}

local_db_backup() {
  require_root
  print_title
  section "Локальный backup SQLite"
  resolve_service_paths_from_env || return 1
  if [[ ! -f "$DB_PATH" ]]; then
    err "БД не найдена: $DB_PATH"
    return 1
  fi
  local target
  target="${DB_BACKUP_DIR}/clipsave_$(current_timestamp).sqlite3"
  sqlite_backup_to_file "$DB_PATH" "$target"
  chown "${APP_USER}:${APP_GROUP}" "$target" 2>/dev/null || true
  chmod 640 "$target" 2>/dev/null || true
  sha256_write "$target"
  ok "backup БД создан"
  echo "  $target"
  echo "  ${target}.sha256"
}

find_db_backups() {
  resolve_service_paths_from_env || return 1
  mapfile -t DB_BACKUPS < <(
    find "$DB_BACKUP_DIR" -maxdepth 1 -type f \( -name '*.sqlite3' -o -name '*.db' -o -name '*.bak' \) 2>/dev/null | sort -r
  )
}

choose_db_backup() {
  find_db_backups || return 1
  if [[ "${#DB_BACKUPS[@]}" -eq 0 ]]; then
    err "локальные backup-файлы БД не найдены в ${DB_BACKUP_DIR}."
    return 1
  fi
  echo "Доступные backup-файлы БД:"
  local i=1 f size mtime
  for f in "${DB_BACKUPS[@]}"; do
    size="$(stat -c '%s' "$f" 2>/dev/null || echo 0)"
    mtime="$(stat -c '%y' "$f" 2>/dev/null | cut -d'.' -f1 || true)"
    printf '  %s) %s | %s | %s\n' "$i" "$(basename "$f")" "$(format_bytes "$size")" "$mtime"
    i=$((i + 1))
  done
  echo
  local choice
  read -r -p "Выбери backup БД для восстановления: " choice
  if [[ ! "$choice" =~ ^[0-9]+$ ]] || (( choice < 1 || choice > ${#DB_BACKUPS[@]} )); then
    err "неверный номер backup-файла."
    return 1
  fi
  SELECTED_DB_BACKUP="${DB_BACKUPS[$((choice - 1))]}"
}

local_db_restore() {
  require_root
  print_title
  section "Восстановление SQLite из локального backup"
  choose_db_backup || return 1
  local backup="$SELECTED_DB_BACKUP"
  sha256_verify_soft "$backup" || return 1

  if [[ ! -f "$DB_PATH" ]]; then
    err "текущая БД не найдена: $DB_PATH"
    return 1
  fi

  echo
  echo "Будет восстановлен backup:"
  echo "  $backup"
  echo "В текущую БД:"
  echo "  $DB_PATH"
  echo
  confirm "Подтвердить восстановление БД? [y/N]: " || { echo "Отменено."; return 0; }

  local emergency temp_db owner group mode was_active
  emergency="${DB_BACKUP_DIR}/emergency_before_restore_$(current_timestamp).sqlite3"
  sqlite_backup_to_file "$DB_PATH" "$emergency"
  sha256_write "$emergency"
  ok "аварийный backup текущей БД создан: $emergency"

  was_active="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
  if [[ "$was_active" == "active" ]]; then
    systemctl stop "$SERVICE_NAME"
  fi

  owner="$(stat -c '%u' "$DB_PATH" 2>/dev/null || echo 0)"
  group="$(stat -c '%g' "$DB_PATH" 2>/dev/null || echo 0)"
  mode="$(stat -c '%a' "$DB_PATH" 2>/dev/null || echo 640)"
  temp_db="${DB_PATH}.restore_tmp"
  rm -f "$temp_db"
  cp -f "$backup" "$temp_db"
  sqlite3 -readonly "$temp_db" "PRAGMA quick_check;" >/dev/null
  mv -f "$temp_db" "$DB_PATH"
  chown "$owner:$group" "$DB_PATH" 2>/dev/null || true
  chmod "$mode" "$DB_PATH" 2>/dev/null || true
  rm -f "${DB_PATH}-wal" "${DB_PATH}-shm" 2>/dev/null || true

  if [[ "$was_active" == "active" ]]; then
    systemctl start "$SERVICE_NAME"
  fi
  ok "БД восстановлена"
}

local_db_check() {
  require_root
  print_title
  section "Проверка SQLite"
  resolve_service_paths_from_env || return 1
  if [[ ! -f "$DB_PATH" ]]; then
    err "БД не найдена: $DB_PATH"
    return 1
  fi
  echo "Файл БД: $DB_PATH"
  echo
  sqlite3 -readonly "$DB_PATH" "PRAGMA quick_check;"
}

show_status() {
  print_title
  echo "Сервис:             $SERVICE_NAME"
  echo "Каталог сервиса:    $APP_DIR"
  echo "VENV:               $VENV_DIR"
  echo "Каталог загрузок:   $DOWNLOAD_DIR"
  echo "Каталог backup:     $BACKUP_ROOT"
  echo
  if systemctl list-unit-files "${SERVICE_NAME}.service" >/dev/null 2>&1; then
    echo "Статус сервиса:     $(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
  else
    echo "Статус сервиса:     systemd unit не найден"
  fi
  if command -v angie >/dev/null 2>&1; then
    echo "Angie:              установлен"
  else
    echo "Angie:              не найден"
  fi
  if [[ -f "${APP_DIR}/.env" ]]; then
    echo ".env:               ${APP_DIR}/.env"
  else
    echo ".env:               не найден"
  fi
  if resolve_service_paths_from_env >/dev/null 2>&1; then
    echo "SQLite:             $DB_PATH"
    if [[ -f "$DB_PATH" ]]; then
      echo "Размер SQLite:      $(format_bytes "$(stat -c '%s' "$DB_PATH" 2>/dev/null || echo 0)")"
    fi
  fi
  echo
  echo "Последние полные архивы:"
  find "$FULL_BACKUP_DIR" -maxdepth 1 -type f -name 'clipsave_full_*.tar.gz' -printf '  %TY-%Tm-%Td %TH:%TM  %p\n' 2>/dev/null | sort -r | head -n 5 || true
}

menu_loop() {
  require_root
  while true; do
    print_title
    echo "1) Показать статус"
    echo "2) Создать полный архив переноса"
    echo "3) Отправить последний полный архив на новый сервер"
    echo "4) Восстановить полный архив на этом сервере"
    echo "5) Создать локальный backup SQLite"
    echo "6) Восстановить SQLite из локального backup"
    echo "7) Проверить SQLite"
    echo "0) Выход"
    echo
    local choice
    read -r -p "Выбери пункт: " choice

    case "$choice" in
      1) show_status; pause_enter ;;
      2) make_full_backup; pause_enter ;;
      3) transfer_latest_backup; pause_enter ;;
      4) restore_full_backup; pause_enter ;;
      5) local_db_backup; pause_enter ;;
      6) local_db_restore; pause_enter ;;
      7) local_db_check; pause_enter ;;
      0) exit 0 ;;
      *) echo "Неизвестный пункт."; pause_enter ;;
    esac
  done
}

case "${1:-}" in
  "") menu_loop ;;
  full-backup) make_full_backup ;;
  transfer) transfer_latest_backup ;;
  restore-full) restore_full_backup ;;
  db-backup) local_db_backup ;;
  db-restore) local_db_restore ;;
  db-check) local_db_check ;;
  status) show_status ;;
  -h|--help|help)
    cat <<EOF
Использование:
  sudo bash clipsave_migrate.sh
  sudo bash clipsave_migrate.sh full-backup
  sudo bash clipsave_migrate.sh transfer
  sudo bash clipsave_migrate.sh restore-full
  sudo bash clipsave_migrate.sh db-backup
  sudo bash clipsave_migrate.sh db-restore
  sudo bash clipsave_migrate.sh db-check
EOF
    ;;
  *) err "неизвестная команда: $1"; exit 1 ;;
esac
