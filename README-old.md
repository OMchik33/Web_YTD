# ClipSave — закрытый веб-сервис для скачивания медиа

ClipSave — это приватный веб-сервис на FastAPI для скачивания видео, аудио и обложек по ссылке из различных интернет-ресурсов, если это допускается правилами соответствующего сервиса или если вы скачиваете собственный контент.

<img width="1733" height="1029" alt="2026-04-15_20-41-31" src="https://github.com/user-attachments/assets/fc76b8c8-0a30-46d8-bb06-7531df8f9263" />


## Возможности

- предварительный анализ ссылки перед загрузкой файла;
- скачивание в нескольких режимах: лучшее качество, вручную выбранный формат, MP3 или обложка;
- встроенная очередь задач с ограничением количества одновременных загрузок;
- персональная история скачиваний для каждого пользователя;
- поддержка личного `cookies.txt` пользователя и общего `cookies.txt` администратора;
- два варианта доступа: постоянная ссылка входа и одноразовые ссылки-приглашения;
- ручные метки для одноразовых ссылок, чтобы администратору было удобно различать выданные доступы;
- возможность в любой момент отозвать доступ у конкретного пользователя;
- контроль активности пользователей за последние 5 и 10 минут;
- хранение пользователей, сессий, истории и ссылок доступа в SQLite;
- раздача готовых файлов через Angie из каталога `/download`;
- автоматическое удаление устаревших файлов из `/download`;
- автоматическое ночное обновление `yt-dlp` и `yt-dlp-ejs`;
- готовый автоустановщик: запуск из локального набора файлов или развёртывание из GitHub/Git;
- отдельный bash-скрипт для резервного копирования, восстановления и проверки базы SQLite.


## Что есть в репозитории

- `web_ytd.py` — основной backend;
- `templates/` — HTML-шаблоны;
- `static/` — JS и CSS;
- `.env.example` — понятный пример конфигурации;
- `requirements.txt` — Python-зависимости;
- `install-versions.env` — переменные для автоустановки;
- `scripts/install.sh` — bash-скрипт автоустановки;
- `scripts/ytd_db.sh` — bash-скрипт для backup / restore / проверки SQLite;
- `deploy/systemd/ytd_web.service` — пример systemd-службы;
- `deploy/angie/site.conf.example` — пример конфига сайта для Angie;
- `deploy/angie/download_filename_map.conf` — `map` для корректного имени скачиваемого файла;
- `deploy/angie/00-acme.conf.example` — пример глобальной ACME-настройки для Angie;
- `deploy/cron/crontab.example` — пример системного cron-файла.

---

## Быстрый старт - установка из GitHub/Git

```
sudo apt update
sudo apt install -y git
git clone https://github.com/OMchik33/Web_YTD.git /root/web-ytd-src
cd /root/web-ytd-src
sudo bash scripts/install.sh
```
В этом варианте скрипт разворачивает сервис из уже клонированного репозитория.

---

## Что делает `install.sh`

Скрипт:

- проверяет, что ОС — Ubuntu 24.04;
- ставит системные пакеты;
- подключает официальный репозиторий Angie и ставит Angie;
- создаёт пользователя `botrunner`, каталог проекта и `/download`;
- разворачивает проект из локальной папки или из GitHub/Git;
- создаёт виртуальное окружение и ставит Python-зависимости;
- создаёт `.env`, если его ещё нет;
- раскладывает systemd-службу;
- создаёт необходимые файлы для Angie:
  - `download_filename_map.conf`,
  - `00-acme.conf`,
  - конфиг сайта;
- включает UFW и открывает `22`, `80`, `443`;
- создаёт системный cron-файл `/etc/cron.d/ytd_web`:
  - удаление файлов старше 30 минут из `/download` каждые 5 минут;
  - обновление `yt-dlp` и `yt-dlp-ejs` в `04:00` по времени сервера;
- в конце выводит:
  - путь к проекту,
  - путь к `.env`,
  - обычную ссылку входа,
  - админскую ссылку входа,
  - команды для проверки службы.

После установки проверь:

```bash
sudo systemctl status ytd_web --no-pager
sudo systemctl status angie --no-pager
sudo ufw status verbose
```

---

## Полная установка с нуля для новичка

### 1. Что нужно заранее

- Ubuntu Server 24.04 LTS;
- домен с A‑записью на IP сервера;
- доступ по SSH с `sudo`;
- открытые порты `22`, `80`, `443`.

### 2. Подготовка DNS и сервера

Сделай A‑запись домена на IP сервера и дождись, пока домен начнёт резолвиться на этот IP.

Обнови систему:

```bash
sudo apt update
sudo apt upgrade -y
```

### 3. Установка пакетов ОС

```bash
sudo apt update
sudo apt install -y git ca-certificates curl unzip ffmpeg nodejs python3 python3-venv python3-pip sqlite3 cron ufw rsync
```

### 4. Установка Angie из официального репозитория

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo curl -fsSL -o /etc/apt/trusted.gpg.d/angie-signing.gpg https://angie.software/keys/angie-signing.gpg

echo "deb https://download.angie.software/angie/$(. /etc/os-release && echo "$ID/$VERSION_ID $VERSION_CODENAME") main" \
| sudo tee /etc/apt/sources.list.d/angie.list > /dev/null

sudo apt-get update
sudo apt-get install -y angie
sudo systemctl enable --now angie
```

### 5. Настройка UFW

```bash
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable
sudo ufw status verbose
```

### 6. Создание пользователя и каталогов

```bash
sudo adduser --home /opt/telegram-bots --shell /bin/bash --disabled-password --gecos "" botrunner
sudo mkdir -p /opt/telegram-bots/ytd_web
sudo mkdir -p /download
sudo mkdir -p /opt/telegram-bots/ytd_web/{static,templates,cookies,data,logs,deploy,backups,scripts}
sudo chown -R botrunner:botrunner /opt/telegram-bots
sudo chown -R botrunner:botrunner /download
```

### 7. Размещение файлов проекта

Скопируй в `/opt/telegram-bots/ytd_web`:

- `web_ytd.py`
- `requirements.txt`
- `.env.example`
- `templates/index.html`
- `templates/logged_out.html`
- `static/app.js`
- `static/style.css`
- `deploy/systemd/ytd_web.service`
- `deploy/angie/site.conf.example`
- `deploy/angie/download_filename_map.conf`
- `deploy/cron/crontab.example`
- `scripts/ytd_db.sh`

### 8. Создание виртуального окружения и установка зависимостей

```bash
sudo -u botrunner -H python3 -m venv /opt/telegram-bots/venv
sudo -u botrunner -H bash -lc 'source /opt/telegram-bots/venv/bin/activate && pip install --upgrade pip setuptools wheel && pip install -r /opt/telegram-bots/ytd_web/requirements.txt'
```

### 9. Создание `.env`

Скопируй `.env.example` в `.env` и отредактируй значения:

```bash
sudo -u botrunner cp /opt/telegram-bots/ytd_web/.env.example /opt/telegram-bots/ytd_web/.env
sudo -u botrunner nano /opt/telegram-bots/ytd_web/.env
```

Обязательно поменяй:

- `WEB_BASE_PATH`
- `WEB_PUBLIC_BASE_URL`
- `WEB_SECRET_KEY`
- `WEB_LOGIN_KEY`
- `WEB_ADMIN_LOGIN_KEY`
- `DOWNLOAD_BASE_URL`

### 10. Настройка Angie под домен

Убедись, что в `/etc/angie/angie.conf` внутри блока `http {}` есть оба include:

```nginx
include /etc/angie/conf.d/*.conf;
include /etc/angie/http.d/*.conf;
```

Скопируй:

deploy/angie/download_filename_map.conf → /etc/angie/conf.d/download_filename_map.conf
deploy/angie/00-acme.conf.example → /etc/angie/http.d/00-acme.conf
deploy/angie/site.conf.example → /etc/angie/http.d/your-domain.conf

Проверка и перезагрузка:

```bash
sudo angie -t
sudo systemctl reload angie
```

### 11. Получение и обновление сертификатов

В примере конфига используется встроенный ACME Angie (`acme le;`).

Для его работы необходимо:

- чтобы домен уже смотрел на IP сервера;
- чтобы входящий `80/tcp` был реально доступен извне;
- чтобы в Angie были настроены:
  - `resolver`,
  - `acme_client le ...`,
  - конфиг домена с `acme le;`.

Если сертификат не выпускается, проверь:

```bash
sudo angie -t
sudo systemctl status angie --no-pager
sudo grep -RniE 'acme|challenge|certificate|letsencrypt' /var/log/angie/*.log
```

### 12. Создание службы systemd

```bash
sudo cp /opt/telegram-bots/ytd_web/deploy/systemd/ytd_web.service /etc/systemd/system/ytd_web.service
sudo systemctl daemon-reload
sudo systemctl enable --now ytd_web
sudo systemctl status ytd_web --no-pager
```

### 13. Cron: автообновление `yt-dlp` и очистка `/download`

В проекте используется **не пользовательский `crontab -e`**, а отдельный системный cron-файл:

```bash
/etc/cron.d/ytd_web
```

Это значит:

* crontab -l может быть пустым — это нормально;
* задания нужно смотреть и редактировать в файле /etc/cron.d/ytd_web;
* после расписания в таком файле обязательно указывается пользователь, от которого выполняется команда.

**Посмотреть текущие задания:**

```bash
cat /etc/cron.d/ytd_web
```

**Отредактировать**

```bash

sudo nano /etc/cron.d/ytd_web

```

**Пример содержимого**

```bash

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

*/5 * * * * root find /download -type f -mmin +30 -delete
0 4 * * * root before=$(/usr/bin/sudo -u botrunner -H bash -lc 'source /opt/telegram-bots/venv/bin/activate && python -c "import importlib.metadata as m; print(\"yt-dlp=\"+m.version(\"yt-dlp\") if \"yt-dlp\" in m.packages_distributions() else \"yt-dlp=NOT_INSTALLED\"); print(\"yt-dlp-ejs=\"+m.version(\"yt-dlp-ejs\") if \"yt-dlp-ejs\" in m.packages_distributions() else \"yt-dlp-ejs=NOT_INSTALLED\")"'); /usr/bin/sudo -u botrunner -H bash -lc 'source /opt/telegram-bots/venv/bin/activate && pip install -U --no-deps yt-dlp yt-dlp-ejs'; after=$(/usr/bin/sudo -u botrunner -H bash -lc 'source /opt/telegram-bots/venv/bin/activate && python -c "import importlib.metadata as m; print(\"yt-dlp=\"+m.version(\"yt-dlp\") if \"yt-dlp\" in m.packages_distributions() else \"yt-dlp=NOT_INSTALLED\"); print(\"yt-dlp-ejs=\"+m.version(\"yt-dlp-ejs\") if \"yt-dlp-ejs\" in m.packages_distributions() else \"yt-dlp-ejs=NOT_INSTALLED\")"'); [ "$before" != "$after" ] && /usr/bin/systemctl restart ytd_web || true

```

**После редактирования можно перезапустить cron**

```bash

sudo systemctl restart cron
sudo systemctl status cron --no-pager

```

**Проверить, что задания действительно выполняются**

```bash

grep CRON /var/log/syslog | tail -n 50

```

### 14. Проверка после установки

Проверь:

```bash
source /opt/telegram-bots/ytd_web/.env
curl -I "http://127.0.0.1:${WEB_PORT}${WEB_BASE_PATH}/"
sudo systemctl status ytd_web --no-pager
sudo systemctl status angie --no-pager
sudo ufw status verbose
```

---

## Работа с SQLite: backup / restore / проверка

Скрипт `scripts/ytd_db.sh` умеет:

* status
* backup
* restore
* quick-check
* integrity-check


Достаточно просто запустить `bash ytd_db.sh` и скрипт предложит меню выбора действий (на русском языке)


Что важно:

- перед любым restore скрипт сам создаёт аварийный backup текущей БД;

---

## Структура проекта в репозитории

```text
web-ytd/
├── web_ytd.py
├── requirements.txt
├── .env.example
├── README.md
├── install-versions.env
├── static/
│   ├── app.js
│   └── style.css
├── templates/
│   ├── index.html
│   └── logged_out.html
├── deploy/
│   ├── systemd/
│   │   └── ytd_web.service
│   ├── angie/
│   │   ├── site.conf.example
│   │   ├── download_filename_map.conf
│   │   └── 00-acme.conf.example
│   └── cron/
│       └── crontab.example
└── scripts/
    ├── install.sh
    └── ytd_db.sh
```

---

## Примечания

- Сам сервис работает не в корне домена, а под `WEB_BASE_PATH`.
- `WEB_PORT` наружу открывать не нужно, потому что FastAPI слушает `127.0.0.1`.
- SQLite хранит только метаданные. Видео, логи и `cookies.txt` остаются файлами на диске.
- Для YouTube и `yt-dlp-ejs` нужен установленный `node`.
