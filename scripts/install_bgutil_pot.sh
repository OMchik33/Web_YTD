#!/usr/bin/env bash
set -Eeuo pipefail

VENV_DIR="${VENV_DIR:-/opt/telegram-bots/venv}"
BGUTIL_POT_DIR="${BGUTIL_POT_DIR:-/opt/bgutil-ytdlp-pot-provider}"
BGUTIL_POT_SERVICE="${BGUTIL_POT_SERVICE:-bgutil-pot}"
BGUTIL_POT_REPO_URL="${BGUTIL_POT_REPO_URL:-https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git}"
BGUTIL_POT_PROVIDER_VERSION="${BGUTIL_POT_PROVIDER_VERSION:-1.3.1}"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Этот скрипт нужно запускать от root" >&2
  exit 1
fi

if [[ ! -x "${VENV_DIR}/bin/python" ]]; then
  echo "Не найден Python в виртуальном окружении: ${VENV_DIR}/bin/python" >&2
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "Не найден node. Сначала установи Node.js 20+." >&2
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "Не найден npm. Проверь установку Node.js." >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update
    apt-get install -y git
  else
    echo "Не найден git и apt-get. Установи git вручную." >&2
    exit 1
  fi
fi

echo "==> Установка Python-плагина bgutil-ytdlp-pot-provider в ${VENV_DIR}"
"${VENV_DIR}/bin/python" -m pip install -U "bgutil-ytdlp-pot-provider==${BGUTIL_POT_PROVIDER_VERSION}"

echo "==> Подготовка POT-server в ${BGUTIL_POT_DIR}"
mkdir -p "$(dirname "${BGUTIL_POT_DIR}")"
if [[ -d "${BGUTIL_POT_DIR}/.git" ]]; then
  git -C "${BGUTIL_POT_DIR}" fetch --depth 1 origin main || true
  git -C "${BGUTIL_POT_DIR}" reset --hard origin/main || true
else
  rm -rf "${BGUTIL_POT_DIR}"
  git clone --depth 1 "${BGUTIL_POT_REPO_URL}" "${BGUTIL_POT_DIR}"
fi

cd "${BGUTIL_POT_DIR}/server"
npm install
npx tsc

cat > "/etc/systemd/system/${BGUTIL_POT_SERVICE}.service" <<EOF
[Unit]
Description=bgutil yt-dlp PO Token Provider
After=network.target

[Service]
Type=simple
WorkingDirectory=${BGUTIL_POT_DIR}/server
ExecStart=$(command -v node) ${BGUTIL_POT_DIR}/server/build/main.js
Restart=always
RestartSec=3
User=root
Environment=NODE_ENV=production

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${BGUTIL_POT_SERVICE}.service"

if ! curl -sS --connect-timeout 5 --max-time 10 http://127.0.0.1:4416/ping >/dev/null; then
  echo "POT-server не ответил на http://127.0.0.1:4416/ping" >&2
  systemctl status "${BGUTIL_POT_SERVICE}.service" --no-pager -l || true
  exit 1
fi

systemctl status "${BGUTIL_POT_SERVICE}.service" --no-pager -l || true

echo "==> POT-server готов: http://127.0.0.1:4416"
