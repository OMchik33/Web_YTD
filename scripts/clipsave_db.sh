#!/usr/bin/env bash

set -u
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# =========================
# Поиск .env
# =========================

if [[ -n "${ENV_FILE:-}" ]]; then
  CANDIDATE_ENV="$ENV_FILE"
elif [[ -f "${SCRIPT_DIR}/.env" ]]; then
  CANDIDATE_ENV="${SCRIPT_DIR}/.env"
elif [[ -f "${SCRIPT_DIR}/../.env" ]]; then
  CANDIDATE_ENV="${SCRIPT_DIR}/../.env"
else
  echo "Не найден .env рядом со скриптом и уровнем выше:"
  echo "  ${SCRIPT_DIR}/.env"
  echo "  ${SCRIPT_DIR}/../.env"
  exit 1
fi

if ! ENV_FILE="$(readlink -f "$CANDIDATE_ENV" 2>/dev/null)"; then
  ENV_FILE="$CANDIDATE_ENV"
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Не найден .env: $ENV_FILE"
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# =========================
# Базовые переменные
# =========================

PROJECT_DIR="$(cd "$(dirname "$ENV_FILE")" && pwd)"
SERVICE_NAME="${SERVICE_NAME:-clipsave}"

resolve_path() {
  local value="$1"
  if [[ "$value" = /* ]]; then
    printf '%s
' "$value"
  else
    printf '%s
' "${PROJECT_DIR}/${value}"
  fi
}

DATA_PATH_VALUE="${DATA_PATH:-./data}"
DATA_PATH_RESOLVED="$(resolve_path "$DATA_PATH_VALUE")"

if [[ -n "${SQLITE_PATH:-}" ]]; then
  DB_PATH="$(resolve_path "$SQLITE_PATH")"
else
  DB_NAME="${SQLITE_DB_NAME:-clipsave.sqlite3}"
  DB_PATH="${DATA_PATH_RESOLVED%/}/${DB_NAME}"
fi

DOWNLOAD_PATH_VALUE="${DOWNLOAD_PATH:-/download}"
DOWNLOAD_PATH_RESOLVED="$(resolve_path "$DOWNLOAD_PATH_VALUE")"

BACKUP_DIR="${PROJECT_DIR}/backups/db"
mkdir -p "$BACKUP_DIR"

# =========================
# Проверки
# =========================

if ! command -v sqlite3 >/dev/null 2>&1; then
  echo "Не найден sqlite3. Установи пакет sqlite3."
  exit 1
fi

if [[ ! -f "$DB_PATH" ]]; then
  echo "Файл БД не найден: $DB_PATH"
  exit 1
fi

# =========================
# Вспомогательные функции
# =========================

hr() {
  echo "=============================="
}

print_header() {
  hr
  echo "ClipSave / SQLite DB tool"
  hr
}

print_error() {
  echo "Ошибка: $*" >&2
}

print_warn() {
  echo "ВНИМАНИЕ: $*"
}

confirm() {
  local prompt="${1:-Продолжить? [y/N]: }"
  local answer
  read -r -p "$prompt" answer
  [[ "$answer" =~ ^[YyАа]([Ee]|[Yy])?$ ]]
}

sql_escape() {
  printf "%s" "$1" | sed "s/'/''/g"
}

sqlite_scalar() {
  local query="$1"
  sqlite3 -readonly "$DB_PATH" "$query" 2>/dev/null | head -n 1
}

table_exists() {
  local table_name="$1"
  local result
  result="$(sqlite_scalar "SELECT name FROM sqlite_master WHERE type='table' AND name='$(sql_escape "$table_name")';")"
  [[ "$result" == "$table_name" ]]
}

format_bytes() {
  local bytes="$1"
  if [[ -z "$bytes" || ! "$bytes" =~ ^[0-9]+$ ]]; then
    echo "?"
    return
  fi

  local kib=$((1024))
  local mib=$((1024 * 1024))
  local gib=$((1024 * 1024 * 1024))

  if (( bytes >= gib )); then
    awk -v b="$bytes" 'BEGIN { printf "%.1fG", b/1024/1024/1024 }'
  elif (( bytes >= mib )); then
    awk -v b="$bytes" 'BEGIN { printf "%.1fM", b/1024/1024 }'
  elif (( bytes >= kib )); then
    awk -v b="$bytes" 'BEGIN { printf "%.1fK", b/1024 }'
  else
    printf "%sB" "$bytes"
  fi
}

service_state() {
  if command -v systemctl >/dev/null 2>&1; then
    systemctl is-active "$SERVICE_NAME" 2>/dev/null || true
  else
    echo "unknown"
  fi
}

db_mtime() {
  if command -v stat >/dev/null 2>&1; then
    stat -c '%y' "$DB_PATH" 2>/dev/null | cut -d'.' -f1 || true
  fi
}

db_size() {
  if command -v stat >/dev/null 2>&1; then
    local sz
    sz="$(stat -c '%s' "$DB_PATH" 2>/dev/null || echo 0)"
    format_bytes "$sz"
  else
    echo "?"
  fi
}

recent_download_files_count() {
  if [[ -d "$DOWNLOAD_PATH_RESOLVED" ]]; then
    find "$DOWNLOAD_PATH_RESOLVED" -maxdepth 1 -type f -mmin -30 2>/dev/null | wc -l | awk '{print $1}'
  else
    echo "0"
  fi
}

backup_files_array() {
  mapfile -t BACKUP_FILES < <(
    find "$BACKUP_DIR" -maxdepth 1 -type f       \( -name '*.sqlite3' -o -name '*.db' -o -name '*.bak' \)       2>/dev/null | sort -r
  )
}

print_backup_list() {
  backup_files_array
  echo "Архивы в ${BACKUP_DIR}:"
  if [[ "${#BACKUP_FILES[@]}" -eq 0 ]]; then
    echo "  (архивов пока нет)"
    return
  fi

  local i=1
  local f size mtime
  for f in "${BACKUP_FILES[@]}"; do
    size="$(stat -c '%s' "$f" 2>/dev/null || echo 0)"
    mtime="$(stat -c '%y' "$f" 2>/dev/null | cut -d'.' -f1 || true)"
    printf "  %d) %s | %s | %s
" "$i" "$(basename "$f")" "$(format_bytes "$size")" "$mtime"
    i=$((i + 1))
  done
}

get_backup_by_index() {
  local index="$1"
  backup_files_array
  if [[ ! "$index" =~ ^[0-9]+$ ]]; then
    return 1
  fi
  if (( index < 1 || index > ${#BACKUP_FILES[@]} )); then
    return 1
  fi
  printf '%s
' "${BACKUP_FILES[$((index - 1))]}"
}

sqlite_backup_to_file() {
  local target_file="$1"
  local escaped
  escaped="$(sql_escape "$target_file")"

  sqlite3 "$DB_PATH" <<SQL
.timeout 10000
.backup '${escaped}'
SQL
}

emergency_backup_name() {
  date -u +"emergency_before_restore_%Y-%m-%d_%H-%M-%S.sqlite3"
}

regular_backup_name() {
  date -u +"clipsave_%Y-%m-%d_%H-%M-%S.sqlite3"
}

vacuum_backup_name() {
  date -u +"vacuum_%Y-%m-%d_%H-%M-%S.sqlite3"
}

active_users_5m() {
  if table_exists users; then
    sqlite_scalar "
      SELECT COUNT(*)
      FROM users
      WHERE COALESCE(is_admin, 0) = 0
        AND COALESCE(is_disabled, 0) = 0
        AND last_activity_at IS NOT NULL
        AND datetime(last_activity_at) >= datetime('now', '-5 minutes');
    "
  else
    echo "0"
  fi
}

active_users_10m() {
  if table_exists users; then
    sqlite_scalar "
      SELECT COUNT(*)
      FROM users
      WHERE COALESCE(is_admin, 0) = 0
        AND COALESCE(is_disabled, 0) = 0
        AND last_activity_at IS NOT NULL
        AND datetime(last_activity_at) >= datetime('now', '-10 minutes');
    "
  else
    echo "0"
  fi
}

nonexpired_sessions() {
  if table_exists sessions; then
    sqlite_scalar "
      SELECT COUNT(*)
      FROM sessions
      WHERE expires_at IS NOT NULL
        AND datetime(expires_at) > datetime('now');
    "
  else
    echo "0"
  fi
}

active_accounts() {
  if table_exists users; then
    sqlite_scalar "
      SELECT COUNT(*)
      FROM users
      WHERE COALESCE(is_admin, 0) = 0
        AND COALESCE(is_disabled, 0) = 0;
    "
  else
    echo "0"
  fi
}

show_status() {
  print_header

  local service_status
  service_status="$(service_state)"
  local recent_files
  recent_files="$(recent_download_files_count)"

  local backups_count
  backup_files_array
  backups_count="${#BACKUP_FILES[@]}"

  printf "%-26s %s
" "Служба:" "$SERVICE_NAME"
  printf "%-26s %s
" "Статус службы:" "${service_status:-unknown}"
  printf "%-26s %s
" "Файл БД:" "$DB_PATH"
  printf "%-26s %s
" "Размер БД:" "$(db_size)"
  printf "%-26s %s
" "Последнее изменение БД:" "$(db_mtime)"
  printf "%-26s %s
" "Активны за 5 минут:" "$(active_users_5m)"
  printf "%-26s %s
" "Активны за 10 минут:" "$(active_users_10m)"
  printf "%-26s %s
" "Неистёкшие сессии:" "$(nonexpired_sessions)"
  printf "%-26s %s
" "Активные учётки:" "$(active_accounts)"
  printf "%-26s %s
" "Файлов в /download <30м:" "$recent_files"
  printf "%-26s %s
" "Архивов БД:" "$backups_count"

  if [[ "$(active_users_5m)" != "0" || "$recent_files" != "0" ]]; then
    print_warn "по данным heartbeat/файлов сервис мог использоваться недавно."
  fi

  echo
  print_backup_list
}

do_backup() {
  local backup_file="${BACKUP_DIR}/$(regular_backup_name)"

  echo "Создаю backup:"
  echo "  $backup_file"

  if ! sqlite_backup_to_file "$backup_file"; then
    print_error "не удалось создать backup."
    return 1
  fi

  echo "Готово."
  print_backup_list
}

choose_backup_interactive() {
  backup_files_array
  if [[ "${#BACKUP_FILES[@]}" -eq 0 ]]; then
    print_error "архивы не найдены."
    return 1
  fi

  echo
  echo "Доступные архивы:"
  local i=1
  local f size mtime
  for f in "${BACKUP_FILES[@]}"; do
    size="$(stat -c '%s' "$f" 2>/dev/null || echo 0)"
    mtime="$(stat -c '%y' "$f" 2>/dev/null | cut -d'.' -f1 || true)"
    printf "  %d) %s | %s | %s\n" "$i" "$(basename "$f")" "$(format_bytes "$size")" "$mtime"
    i=$((i + 1))
  done

  echo
  local choice
  read -r -p "Введите номер архива для восстановления: " choice

  local selected
  if ! selected="$(get_backup_by_index "$choice")"; then
    print_error "неверный номер архива."
    return 1
  fi

  REPLY="$selected"
  return 0
}

restore_backup() {
  local source_file=""
  local selected_service_state
  local emergency_file
  local owner group mode
  local temp_db

  if [[ -n "${1:-}" ]]; then
    if [[ -f "$1" ]]; then
      source_file="$1"
    else
      if ! source_file="$(get_backup_by_index "$1")"; then
        print_error "архив не найден по номеру: $1"
        return 1
      fi
    fi
  else
    if ! choose_backup_interactive; then
      return 1
    fi
    source_file="$REPLY"
  fi

  if [[ ! -f "$source_file" ]]; then
    print_error "файл архива не найден: $source_file"
    return 1
  fi

  echo
  echo "Будет восстановлен архив:"
  echo "  $source_file"
  echo "В текущую БД:"
  echo "  $DB_PATH"
  echo

  if ! confirm "Подтвердить восстановление? [y/N]: "; then
    echo "Отменено."
    return 0
  fi

  emergency_file="${BACKUP_DIR}/$(emergency_backup_name)"
  echo
  echo "Сначала создаю аварийный backup текущей БД:"
  echo "  $emergency_file"

  if ! sqlite_backup_to_file "$emergency_file"; then
    print_error "не удалось создать аварийный backup перед restore."
    return 1
  fi

  selected_service_state="$(service_state)"

  if [[ "$selected_service_state" == "active" ]]; then
    echo "Останавливаю службу $SERVICE_NAME..."
    if ! systemctl stop "$SERVICE_NAME"; then
      print_error "не удалось остановить службу $SERVICE_NAME."
      return 1
    fi
  fi

  owner="$(stat -c '%u' "$DB_PATH" 2>/dev/null || echo 0)"
  group="$(stat -c '%g' "$DB_PATH" 2>/dev/null || echo 0)"
  mode="$(stat -c '%a' "$DB_PATH" 2>/dev/null || echo 640)"

  temp_db="${DB_PATH}.restore_tmp"
  rm -f "$temp_db"

  if ! cp -f "$source_file" "$temp_db"; then
    print_error "не удалось скопировать архив во временный файл."
    [[ "$selected_service_state" == "active" ]] && systemctl start "$SERVICE_NAME" || true
    return 1
  fi

  if ! sqlite3 -readonly "$temp_db" "PRAGMA quick_check;" >/dev/null 2>&1; then
    rm -f "$temp_db"
    print_error "проверка временной восстановленной БД не прошла."
    [[ "$selected_service_state" == "active" ]] && systemctl start "$SERVICE_NAME" || true
    return 1
  fi

  if ! mv -f "$temp_db" "$DB_PATH"; then
    rm -f "$temp_db"
    print_error "не удалось заменить файл БД."
    [[ "$selected_service_state" == "active" ]] && systemctl start "$SERVICE_NAME" || true
    return 1
  fi

  chown "$owner:$group" "$DB_PATH" 2>/dev/null || true
  chmod "$mode" "$DB_PATH" 2>/dev/null || true

  rm -f "${DB_PATH}-wal" "${DB_PATH}-shm" 2>/dev/null || true

  if [[ "$selected_service_state" == "active" ]]; then
    echo "Запускаю службу $SERVICE_NAME..."
    if ! systemctl start "$SERVICE_NAME"; then
      print_error "БД восстановлена, но служба $SERVICE_NAME не запустилась."
      return 1
    fi
  fi

  echo "Восстановление завершено."
  echo "Аварийный backup:"
  echo "  $emergency_file"
}

do_quick_check() {
  print_header
  echo "Запускаю PRAGMA quick_check;"
  echo

  sqlite3 -readonly "$DB_PATH" "PRAGMA quick_check;"
}

do_integrity_check() {
  print_header
  echo "Запускаю PRAGMA integrity_check;"
  echo

  sqlite3 -readonly "$DB_PATH" "PRAGMA integrity_check;"
}

do_vacuum_into() {
  local output_file="${BACKUP_DIR}/$(vacuum_backup_name)"
  local escaped
  escaped="$(sql_escape "$output_file")"

  echo "Создаю компактную копию БД через VACUUM INTO:"
  echo "  $output_file"

  if ! sqlite3 "$DB_PATH" "VACUUM INTO '${escaped}';"; then
    print_error "не удалось выполнить VACUUM INTO."
    return 1
  fi

  echo "Готово."
  print_backup_list
}

show_help() {
  cat <<'EOF'
Использование:
  bash clipsave_db.sh
  bash clipsave_db.sh status
  bash clipsave_db.sh backup
  bash clipsave_db.sh restore
  bash clipsave_db.sh restore 2
  bash clipsave_db.sh restore /путь/к/архиву.sqlite3
  bash clipsave_db.sh quick-check
  bash clipsave_db.sh integrity-check
  bash clipsave_db.sh vacuum-into
  bash clipsave_db.sh list

Дополнительно:
  ENV_FILE=/путь/к/.env bash clipsave_db.sh status
EOF
}

pause_enter() {
  echo
  read -r -p "Нажми Enter для продолжения..." _
}

menu_loop() {
  while true; do
    clear 2>/dev/null || true
    print_header
    echo "1) Показать статус БД и активности"
    echo "2) Создать backup"
    echo "3) Восстановить backup"
    echo "4) Быстрая проверка БД (quick_check)"
    echo "5) Полная проверка БД (integrity_check)"
    echo "6) VACUUM INTO"
    echo "7) Показать список архивов"
    echo "8) Справка"
    echo "0) Выход"
    echo

    local choice
    read -r -p "Выбери пункт: " choice
    echo

    case "$choice" in
      1)
        show_status
        pause_enter
        ;;
      2)
        do_backup
        pause_enter
        ;;
      3)
        restore_backup
        pause_enter
        ;;
      4)
        do_quick_check
        pause_enter
        ;;
      5)
        do_integrity_check
        pause_enter
        ;;
      6)
        do_vacuum_into
        pause_enter
        ;;
      7)
        print_header
        print_backup_list
        pause_enter
        ;;
      8)
        show_help
        pause_enter
        ;;
      0)
        exit 0
        ;;
      *)
        echo "Неизвестный пункт."
        pause_enter
        ;;
    esac
  done
}

# =========================
# Точка входа
# =========================

case "${1:-}" in
  "")
    menu_loop
    ;;
  status)
    show_status
    ;;
  backup)
    do_backup
    ;;
  restore)
    shift || true
    restore_backup "${1:-}"
    ;;
  quick-check)
    do_quick_check
    ;;
  integrity-check)
    do_integrity_check
    ;;
  vacuum-into)
    do_vacuum_into
    ;;
  list)
    print_header
    print_backup_list
    ;;
  help|-h|--help)
    show_help
    ;;
  *)
    print_error "неизвестная команда: $1"
    echo
    show_help
    exit 1
    ;;
esac
