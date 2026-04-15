# Web YTD — закрытый веб‑сервис для скачивания медиа

Web YTD — это приватный веб‑сервис на FastAPI для скачивания видео и аудио по ссылке через `yt-dlp`, с очередью задач, пользовательскими и общими `cookies.txt`, одноразовыми ссылками доступа, SQLite и обратным прокси через Angie.

<img width="1733" height="1029" alt="2026-04-15_20-41-31" src="https://github.com/user-attachments/assets/fc76b8c8-0a30-46d8-bb06-7531df8f9263" />


## Возможности

- анализ ссылки перед скачиванием;
- скачивание в нескольких режимах: лучшее качество, любой формат, MP3, конкретный формат;
- очередь задач и ограничение количества одновременных скачиваний;
- история скачиваний по пользователям;
- личные `cookies.txt` пользователей и общий `cookies.txt` администратора;
- отдельный вход по постоянной ссылке и по одноразовым ссылкам;
- ручные метки/имена для одноразовых ссылок доступа;
- отзыв доступа администратором;
- отслеживание активности пользователей за последние 5 и 10 минут;
- хранение пользователей, сессий, истории и доступов в SQLite;
- отдача готовых файлов через Angie из `/download`;
- автоматическая очистка старых файлов из `/download`;
- автоматическое ночное обновление `yt-dlp` и `yt-dlp-ejs`;
- автоустановщик в двух режимах: из локального набора файлов или напрямую из GitHub/Git;
- bash‑скрипт для backup / restore / проверки SQLite.

## Что есть в репозитории

- `web_ytd.py` — основной backend;
- `templates/` — HTML‑шаблоны;
- `static/` — JS и CSS;
- `.env.example` — понятный пример конфигурации;
- `requirements.txt` — Python‑зависимости;
- `install-versions.env` — переменные для автоустановки;
- `scripts/install.sh` — bash‑скрипт автоустановки;
- `scripts/ytd_db.sh` — bash‑скрипт для backup / restore / проверки SQLite;
- `deploy/systemd/ytd_web.service` — пример systemd‑службы;
- `deploy/angie/site.conf.example` — пример конфига сайта для Angie;
- `deploy/angie/download_filename_map.conf` — `map` для корректного имени скачиваемого файла;
- `deploy/cron/crontab.example` — готовые строки для cron.

---

## Быстрый старт

### Вариант A — установка из уже скачанного набора файлов

Если у тебя есть ZIP‑архив проекта или локальная папка с файлами:

```bash
unzip ytd_repo_bundle_v2.zip -d /root/web-ytd-src
cd /root/web-ytd-src
sudo bash scripts/install.sh
```

Скрипт сам определит локальный режим и скопирует файлы проекта в рабочий каталог сервиса.

### Вариант B — установка из GitHub/Git

Если проект уже лежит в репозитории GitHub:

```bash
unzip ytd_repo_bundle_v2.zip -d /root/web-ytd-installer
cd /root/web-ytd-installer
sudo INSTALL_MODE=git \
  GIT_REPO_URL="https://github.com/USERNAME/REPO.git" \
  GIT_BRANCH="main" \
  bash scripts/install.sh
```

В этом режиме `install.sh` сам клонирует проект из GitHub и развернёт его в рабочий каталог.

---

## Что делает `install.sh`

Скрипт:

- проверяет, что ОС — Ubuntu 24.04;
- ставит системные пакеты;
- подключает официальный репозиторий Angie и ставит Angie;
- **в самом начале создаёт `/opt/telegram-bots`**, чтобы не было ошибки на пустом родительском каталоге;
- создаёт пользователя `botrunner`, каталог проекта и `/download`;
- копирует проект из локальной папки или клонирует его из Git;
- создаёт виртуальное окружение и ставит Python‑зависимости;
- создаёт `.env`, если его ещё нет;
- раскладывает systemd‑службу;
- раскладывает конфиг Angie;
- включает UFW и открывает `22`, `80`, `443`;
- создаёт cron‑задания:
  - удаление файлов старше 30 минут из `/download` каждые 5 минут;
  - обновление `yt-dlp` и `yt-dlp-ejs` в `04:00` по времени сервера.

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
sudo mkdir -p /opt/telegram-bots
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

Скопируй `deploy/angie/download_filename_map.conf` в `/etc/angie/conf.d/`, а `deploy/angie/site.conf.example` — в `/etc/angie/http.d/` и подставь свой домен и `WEB_BASE_PATH`.

Проверка и перезагрузка:

```bash
sudo angie -t
sudo systemctl reload angie
```

### 11. Получение и обновление сертификатов

В примере конфига используется встроенный ACME Angie (`acme le;`). Для его работы домен должен уже смотреть на сервер, а порты `80` и `443` должны быть доступны снаружи.

### 12. Создание службы systemd

```bash
sudo cp /opt/telegram-bots/ytd_web/deploy/systemd/ytd_web.service /etc/systemd/system/ytd_web.service
sudo systemctl daemon-reload
sudo systemctl enable --now ytd_web
sudo systemctl status ytd_web --no-pager
```

### 13. Cron: автообновление `yt-dlp` и очистка `/download`

Готовые строки:

```cron
*/5 * * * * find /download -type f -mmin +30 -delete
0 4 * * * before=$(/usr/bin/sudo -u botrunner -H bash -lc 'source /opt/telegram-bots/venv/bin/activate && python -c "import importlib.metadata as m; print(\"yt-dlp=\"+m.version(\"yt-dlp\") if \"yt-dlp\" in m.packages_distributions() else \"yt-dlp=NOT_INSTALLED\"); print(\"yt-dlp-ejs=\"+m.version(\"yt-dlp-ejs\") if \"yt-dlp-ejs\" in m.packages_distributions() else \"yt-dlp-ejs=NOT_INSTALLED\")"'); /usr/bin/sudo -u botrunner -H bash -lc 'source /opt/telegram-bots/venv/bin/activate && pip install -U --no-deps yt-dlp yt-dlp-ejs'; after=$(/usr/bin/sudo -u botrunner -H bash -lc 'source /opt/telegram-bots/venv/bin/activate && python -c "import importlib.metadata as m; print(\"yt-dlp=\"+m.version(\"yt-dlp\") if \"yt-dlp\" in m.packages_distributions() else \"yt-dlp=NOT_INSTALLED\"); print(\"yt-dlp-ejs=\"+m.version(\"yt-dlp-ejs\") if \"yt-dlp-ejs\" in m.packages_distributions() else \"yt-dlp-ejs=NOT_INSTALLED\")"'); [ "$before" != "$after" ] && /usr/bin/systemctl restart ytd_web || true
```

### 14. Проверка после установки

Проверь:

```bash
curl -I http://127.0.0.1:8093
sudo systemctl status ytd_web --no-pager
sudo systemctl status angie --no-pager
sudo ufw status verbose
```

---

## Работа с SQLite: backup / restore / проверка

Скрипт `scripts/ytd_db.sh` умеет:

```bash
sudo bash scripts/ytd_db.sh status
sudo bash scripts/ytd_db.sh backup
sudo bash scripts/ytd_db.sh restore
sudo bash scripts/ytd_db.sh restore 2
sudo bash scripts/ytd_db.sh quick-check
sudo bash scripts/ytd_db.sh integrity-check
sudo bash scripts/ytd_db.sh vacuum-into
```

Что важно:

- перед любым restore скрипт сам создаёт аварийный backup текущей БД;
- если запустить `restore` без аргумента, он покажет список найденных архивов и попросит ввести **номер** архива цифрой;
- если запустить `restore 2`, он восстановит второй архив из списка;
- в начале работы скрипт показывает свежую активность по БД и наличие недавних файлов в `/download`.

---

## Как взять `REPO.git` в GitHub через веб‑интерфейс

Ничего отдельно “создавать” не нужно. У каждого обычного GitHub‑репозитория уже есть clone URL.

Делается так:

1. Открой главную страницу репозитория на GitHub.
2. Нажми кнопку **Code**.
3. Выбери вкладку **HTTPS**.
4. Скопируй адрес вида:

```text
https://github.com/USERNAME/REPO.git
```

Именно этот адрес и есть `REPO.git`, который нужен для `git clone` и для `GIT_REPO_URL` в `install.sh`.

---

## Как загрузить на GitHub мой ZIP‑архив, если ты работаешь только через веб

### Важное ограничение

GitHub **не умеет** в обычном веб‑интерфейсе взять ZIP‑архив и распаковать его сразу внутрь репозитория одной кнопкой.

Если ты загрузишь ZIP через web UI, он загрузится как **один обычный файл ZIP**, а не как распакованный проект.

### Что делать правильно

1. Скачай ZIP‑архив к себе на компьютер.
2. Распакуй его **локально**.
3. Открой свой репозиторий на GitHub.
4. Нажми **Add file** → **Upload files**.
5. Перетащи в браузер **распакованные файлы и папки**, а не ZIP.
6. Сделай commit.

### Ограничения web‑загрузки GitHub

- через браузер можно загрузить до **100 файлов за один раз**;
- размер одного файла через web UI ограничен **25 MiB**.

Если файлов больше, загружай их несколькими партиями.

---

## Рекомендуемая структура проекта в репозитории

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
│   │   └── download_filename_map.conf
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
