#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${ENV_FILE:-${APP_DIR}/.env}"
SERVICE_NAME="${SERVICE_NAME:-ytd_web}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Не найден .env: ${ENV_FILE}" >&2
  exit 1
fi

# shellcheck disable=SC1090
set -a
source "${ENV_FILE}"
set +a

resolve_path() {
  local p="$1"
  if [[ "${p}" = /* ]]; then
    printf '%s' "${p}"
  else
    printf '%s' "${APP_DIR}/${p#./}"
  fi
}

DATA_ROOT="$(resolve_path "${DATA_PATH:-./data}")"
if [[ -n "${SQLITE_PATH:-}" ]]; then
  DB_PATH="$(resolve_path "${SQLITE_PATH}")"
else
  DB_PATH="${DATA_ROOT}/${SQLITE_DB_NAME:-web_ytd.sqlite3}"
fi

DOWNLOAD_DIR="${DOWNLOAD_PATH:-/download}"
BACKUP_DIR_DEFAULT="${APP_DIR}/backups/db"
BACKUP_DIR="${BACKUP_DIR:-${BACKUP_DIR_DEFAULT}}"
mkdir -p "${BACKUP_DIR}"

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "Не найден sqlite3. Установи пакет sqlite3" >&2
  exit 1
fi

if [[ ! -f "${DB_PATH}" ]]; then
  echo "Не найдена база: ${DB_PATH}" >&2
  exit 1
fi

now_utc() { date -u +"%Y-%m-%dT%H:%M:%S+00:00"; }
ts_minus() { date -u -d "$1" +"%Y-%m-%dT%H:%M:%S+00:00"; }

sql_one() {
  sqlite3 -readonly "${DB_PATH}" "$1"
}

service_state() {
  if command -v systemctl >/dev/null 2>&1; then
    systemctl is-active "${SERVICE_NAME}" 2>/dev/null || true
  else
    echo "unknown"
  fi
}

list_backup_files() {
  find "${BACKUP_DIR}" -maxdepth 1 -type f \( -name '*.sqlite3' -o -name '*.db' \) -printf '%f\n' | sort -r
}

print_backup_list() {
  local i=0
  mapfile -t BACKUP_FILES < <(list_backup_files)

  echo "Архивы в ${BACKUP_DIR}:"
  if [[ ${#BACKUP_FILES[@]} -eq 0 ]]; then
    echo "  (архивов пока нет)"
    return 1
  fi

  for file in "${BACKUP_FILES[@]}"; do
    i=$((i + 1))
    echo "  ${i}) ${file}"
  done
  return 0
}

choose_backup_file() {
  local choice=""
  local index=""

  print_backup_list || return 1
  echo
  read -r -p "Введите номер архива для восстановления: " choice

  if [[ ! "${choice}" =~ ^[0-9]+$ ]]; then
    echo "Нужно ввести номер архива цифрой." >&2
    return 1
  fi

  index=$((choice - 1))
  if (( index < 0 || index >= ${#BACKUP_FILES[@]} )); then
    echo "Нет архива с таким номером." >&2
    return 1
  fi

  printf '%s' "${BACKUP_DIR}/${BACKUP_FILES[$index]}"
}

print_status() {
  local now cutoff5 cutoff10 active5 active10 sessions valid_users db_size db_mtime files_recent backup_count
  now="$(now_utc)"
  cutoff5="$(ts_minus '-5 minutes')"
  cutoff10="$(ts_minus '-10 minutes')"

  active5="$(sql_one "SELECT COUNT(*) FROM users WHERE is_admin=0 AND is_disabled=0 AND last_activity_at IS NOT NULL AND last_activity_at >= '${cutoff5}';")"
  active10="$(sql_one "SELECT COUNT(*) FROM users WHERE is_admin=0 AND is_disabled=0 AND last_activity_at IS NOT NULL AND last_activity_at >= '${cutoff10}';")"
  sessions="$(sql_one "SELECT COUNT(*) FROM sessions WHERE expires_at IS NOT NULL AND expires_at > '${now}';")"
  valid_users="$(sql_one "SELECT COUNT(*) FROM users WHERE is_disabled=0;")"
  backup_count="$(list_backup_files | wc -l | awk '{print $1}')"

  if [[ -d "${DOWNLOAD_DIR}" ]]; then
    files_recent="$(find "${DOWNLOAD_DIR}" -type f -mmin -30 | wc -l | awk '{print $1}')"
  else
    files_recent="0"
  fi

  db_size="$(du -h "${DB_PATH}" | awk '{print $1}')"
  db_mtime="$(date -d "@$(stat -c %Y "${DB_PATH}")" '+%F %T %Z')"

  echo "=============================="
  echo "Web YTD / SQLite DB tool"
  echo "=============================="
  echo "Служба:                    ${SERVICE_NAME}"
  echo "Статус службы:             $(service_state)"
  echo "Файл БД:                   ${DB_PATH}"
  echo "Размер БД:                 ${db_size}"
  echo "Последнее изменение БД:    ${db_mtime}"
  echo "Активны за 5 минут:        ${active5}"
  echo "Активны за 10 минут:       ${active10}"
  echo "Неистёкшие сессии:         ${sessions}"
  echo "Активные учётки:           ${valid_users}"
  echo "Файлов в /download <30м:   ${files_recent}"
  echo "Архивов БД:                ${backup_count}"
  if [[ "${active5}" != "0" || "${files_recent}" != "0" ]]; then
    echo "ВНИМАНИЕ: по данным heartbeat/файлов сервис мог использоваться недавно."
  else
    echo "По данным БД свежей активности не видно."
  fi
  echo
}

backup_db() {
  local target
  target="${1:-${BACKUP_DIR}/backup_$(date +%F_%H-%M-%S).sqlite3}"
  mkdir -p "$(dirname "${target}")"
  print_status
  echo "Создаю backup: ${target}"
  sqlite3 "${DB_PATH}" <<SQL
.timeout 10000
.backup '${target}'
SQL
  chown --reference="${DB_PATH}" "${target}" 2>/dev/null || true
  chmod 640 "${target}" 2>/dev/null || true
  echo "Backup завершён: ${target}"
}

quick_check() {
  print_status
  echo "PRAGMA quick_check;"
  sqlite3 -readonly "${DB_PATH}" "PRAGMA quick_check;"
}

integrity_check() {
  print_status
  echo "PRAGMA integrity_check;"
  sqlite3 -readonly "${DB_PATH}" "PRAGMA integrity_check;"
}

vacuum_into() {
  local target
  target="${1:-${BACKUP_DIR}/vacuum_$(date +%F_%H-%M-%S).sqlite3}"
  mkdir -p "$(dirname "${target}")"
  print_status
  echo "Создаю vacuum copy: ${target}"
  sqlite3 "${DB_PATH}" "VACUUM INTO '${target}';"
  chown --reference="${DB_PATH}" "${target}" 2>/dev/null || true
  chmod 640 "${target}" 2>/dev/null || true
  echo "VACUUM INTO завершён: ${target}"
}

restore_db() {
  local source="${1:-}"
  local rollback="${BACKUP_DIR}/pre_restore_$(date +%F_%H-%M-%S).sqlite3"

  if [[ -z "${source}" ]]; then
    source="$(choose_backup_file)" || exit 1
    echo
    echo "Выбран архив: ${source}"
  elif [[ "${source}" =~ ^[0-9]+$ ]]; then
    mapfile -t BACKUP_FILES < <(list_backup_files)
    local idx=$((source - 1))
    if (( idx < 0 || idx >= ${#BACKUP_FILES[@]} )); then
      echo "Нет архива с номером ${source}" >&2
      exit 1
    fi
    source="${BACKUP_DIR}/${BACKUP_FILES[$idx]}"
    echo "Выбран архив по номеру: ${source}"
  fi

  if [[ ! -f "${source}" ]]; then
    echo "Файл backup не найден: ${source}" >&2
    exit 1
  fi

  print_status
  echo "Перед восстановлением будет создан backup текущей БД: ${rollback}"
  sqlite3 "${DB_PATH}" <<SQL
.timeout 10000
.backup '${rollback}'
SQL

  if command -v systemctl >/dev/null 2>&1; then
    echo "Останавливаю службу ${SERVICE_NAME}"
    systemctl stop "${SERVICE_NAME}"
  fi

  cp -f "${source}" "${DB_PATH}"
  rm -f "${DB_PATH}-wal" "${DB_PATH}-shm"
  chown --reference="${rollback}" "${DB_PATH}" 2>/dev/null || true
  chmod 640 "${DB_PATH}" 2>/dev/null || true

  if command -v systemctl >/dev/null 2>&1; then
    echo "Запускаю службу ${SERVICE_NAME}"
    systemctl start "${SERVICE_NAME}"
  fi
  echo "Восстановление завершено."
}

usage() {
  cat <<USAGE
Использование:
  sudo bash ${0} status
  sudo bash ${0} backup [ПУТЬ_К_ФАЙЛУ]
  sudo bash ${0} restore [ПУТЬ_К_ФАЙЛУ_ИЛИ_НОМЕР]
  sudo bash ${0} quick-check
  sudo bash ${0} integrity-check
  sudo bash ${0} vacuum-into [ПУТЬ_К_ФАЙЛУ]

Команды:
  status           показать сводку по БД и активности
  backup           сделать штатный backup SQLite через .backup
  restore          восстановить БД из backup-файла или выбрать архив по номеру
  quick-check      PRAGMA quick_check
  integrity-check  PRAGMA integrity_check
  vacuum-into      сделать vacuum-копию SQLite через VACUUM INTO
USAGE
}

cmd="${1:-status}"
case "${cmd}" in
  status)
    print_status
    print_backup_list || true
    ;;
  backup)
    backup_db "${2:-}"
    ;;
  restore)
    restore_db "${2:-}"
    ;;
  quick-check)
    quick_check
    ;;
  integrity-check)
    integrity_check
    ;;
  vacuum-into)
    vacuum_into "${2:-}"
    ;;
  *)
    usage
    exit 1
    ;;
esac
