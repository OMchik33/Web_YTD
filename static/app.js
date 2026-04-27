const basePath = (window.APP_CONFIG && window.APP_CONFIG.BASE_PATH) || document.body.dataset.basePath || '';
const isAdmin = Boolean(window.APP_CONFIG && window.APP_CONFIG.IS_ADMIN);

const analyzeForm = document.getElementById('analyze-form');
const analyzeSubmitBtn = document.getElementById('analyze-submit');
const analysisBox = document.getElementById('analysis-box');
const analysisEmpty = document.getElementById('analysis-empty');
const analysisLoading = document.getElementById('analysis-loading');

const videoUrlInput = document.getElementById('video-url');
const videoTitle = document.getElementById('video-title');
const videoUrlView = document.getElementById('video-url-view');
const formatsList = document.getElementById('formats-list');
const thumbnailBtn = document.getElementById('thumbnail-btn');
const qualitySelect = document.getElementById('quality-select');
const proxyDownloadBtn = document.getElementById('proxy-download-btn');
const proxyHelp = document.getElementById('proxy-help');
const proxyResult = document.getElementById('proxy-result');
const proxyVideoLink = document.getElementById('proxy-video-link');
const proxyAudioLink = document.getElementById('proxy-audio-link');
const proxyResultMeta = document.getElementById('proxy-result-meta');

const cookiesForm = document.getElementById('cookies-form');
const cookiesStatus = document.getElementById('cookies-status');
const cookiesHelperText = document.getElementById('cookies-helper-text');
const cookiesMetaText = document.getElementById('cookies-meta-text');
const deleteCookiesBtn = document.getElementById('delete-cookies-btn');

const adminCookiesForm = document.getElementById('admin-cookies-form');
const adminCookiesStatus = document.getElementById('admin-cookies-status');
const adminCookiesHelperText = document.getElementById('admin-cookies-helper-text');
const adminCookiesMetaText = document.getElementById('admin-cookies-meta-text');
const deleteAdminCookiesBtn = document.getElementById('delete-admin-cookies-btn');

const taskPlaceholder = document.getElementById('task-placeholder');
const taskBox = document.getElementById('task-box');
const taskStatusBadge = document.getElementById('task-status-badge');
const taskTitle = document.getElementById('task-title');
const taskDetail = document.getElementById('task-detail');
const taskResult = document.getElementById('task-result');
const taskDownloadLink = document.getElementById('task-download-link');
const taskWatchLink = document.getElementById('task-watch-link');
const taskError = document.getElementById('task-error');
const taskProgress = document.getElementById('task-progress');
const taskCancelBtn = document.getElementById('task-cancel-btn');

const inviteCreateForm = document.getElementById('invite-create-form');
const inviteCreateBtn = document.getElementById('invite-create-btn');
const inviteLabelInput = document.getElementById('invite-label');
const latestInviteBox = document.getElementById('latest-invite-box');
const latestInviteUrl = document.getElementById('latest-invite-url');
const copyLatestInviteBtn = document.getElementById('copy-latest-invite-btn');
const openLatestInviteBtn = document.getElementById('open-latest-invite-btn');
const copyUniversalLoginBtn = document.getElementById('copy-universal-login-btn');
const universalLoginUrl = document.getElementById('universal-login-url');
const invitesList = document.getElementById('invites-list');
const invitesEmpty = document.getElementById('invites-empty');
const adminActiveTasks = document.getElementById('admin-active-tasks');
const statActive5 = document.getElementById('stat-active-5');
const statActive10 = document.getElementById('stat-active-10');
const statActiveTasks = document.getElementById('stat-active-tasks');
const statActiveFiles = document.getElementById('stat-active-files');
const statDownloadUsage = document.getElementById('stat-download-usage');
const statDiskFree = document.getElementById('stat-disk-free');
const statSafeRestart = document.getElementById('stat-safe-restart');

const adminSettingsForm = document.getElementById('admin-settings-form');
const adminFilesList = document.getElementById('admin-files-list');
const adminFilesEmpty = document.getElementById('admin-files-empty');
const adminCleanupExpiredBtn = document.getElementById('admin-cleanup-expired-btn');
const adminCleanupAllFilesBtn = document.getElementById('admin-cleanup-all-files-btn');

const toastContainer = document.getElementById('toast-container');
const themeToggleBtn = document.getElementById('theme-toggle');

let currentAnalysis = null;
let currentTaskId = null;
let pollingTimer = null;
let heartbeatTimer = null;
let adminRefreshTimer = null;
let completedNotifiedTaskIds = new Set();
let errorNotifiedTaskIds = new Set();
let lastAdminOverview = null;

function apiUrl(path) {
  return `${basePath}${path}`;
}

function setCookie(name, value, days = 180) {
  const maxAge = days * 24 * 60 * 60;
  document.cookie = `${encodeURIComponent(name)}=${encodeURIComponent(value)}; path=/; max-age=${maxAge}; SameSite=Lax`;
}

function getCookie(name) {
  const target = `${encodeURIComponent(name)}=`;
  const parts = document.cookie.split(';');
  for (const raw of parts) {
    const part = raw.trim();
    if (part.startsWith(target)) {
      return decodeURIComponent(part.slice(target.length));
    }
  }
  return null;
}

function applyTheme(theme) {
  const normalized = theme === 'light' ? 'light' : 'dark';
  document.body.setAttribute('data-theme', normalized);
  if (themeToggleBtn) {
    themeToggleBtn.textContent = normalized === 'dark' ? 'Светлая тема' : 'Тёмная тема';
  }
}

function initTheme() {
  const savedTheme = getCookie('clipsave_theme') || 'dark';
  applyTheme(savedTheme);
}

function toggleTheme() {
  const current = document.body.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
  const next = current === 'dark' ? 'light' : 'dark';
  applyTheme(next);
  setCookie('clipsave_theme', next, 180);
}

function showToast(message, type = 'info', timeout = 3800) {
  if (!toastContainer) {
    window.alert(message);
    return;
  }

  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  toastContainer.appendChild(toast);

  window.setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(8px)';
    window.setTimeout(() => toast.remove(), 220);
  }, timeout);
}

function playSuccessSound() {
  try {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) {
      return;
    }

    const ctx = new AudioCtx();
    const now = ctx.currentTime;

    const osc1 = ctx.createOscillator();
    const osc2 = ctx.createOscillator();
    const gain = ctx.createGain();

    osc1.type = 'sine';
    osc2.type = 'triangle';
    osc1.frequency.setValueAtTime(880, now);
    osc2.frequency.setValueAtTime(1174, now);

    gain.gain.setValueAtTime(0.0001, now);
    gain.gain.exponentialRampToValueAtTime(0.10, now + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.26);

    osc1.connect(gain);
    osc2.connect(gain);
    gain.connect(ctx.destination);

    osc1.start(now);
    osc2.start(now);
    osc1.stop(now + 0.28);
    osc2.stop(now + 0.28);

    window.setTimeout(() => {
      ctx.close().catch(() => {});
    }, 400);
  } catch (err) {
    // без звука
  }
}

function formatDateToLocal(isoString) {
  if (!isoString) {
    return '';
  }

  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) {
    return isoString;
  }

  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(date.getDate())}-${pad(date.getMonth() + 1)}-${date.getFullYear()} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function applyHistoryDates() {
  document.querySelectorAll('.js-date').forEach((el) => {
    const iso = el.dataset.date || '';
    el.textContent = formatDateToLocal(iso);
  });
}

function applyCookieBlockState(statusEl, helperEl, metaEl, state) {
  if (!statusEl || !helperEl || !metaEl || !state) {
    return;
  }

  statusEl.textContent = state.status_text || 'Статус неизвестен';
  statusEl.classList.remove('ok', 'warn', 'error');
  statusEl.classList.add(state.status_class || 'warn');

  helperEl.textContent = state.helper_text || '';

  if (state.uploaded_at) {
    metaEl.textContent = `Последнее обновление: ${formatDateToLocal(state.uploaded_at)}`;
  } else {
    metaEl.textContent = '';
  }
}

function applyCookieStatesFromResponse(data) {
  if (data && data.cookie_state) {
    applyCookieBlockState(cookiesStatus, cookiesHelperText, cookiesMetaText, data.cookie_state);
  }

  if (data && data.admin_cookie_state) {
    applyCookieBlockState(adminCookiesStatus, adminCookiesHelperText, adminCookiesMetaText, data.admin_cookie_state);
  }
}

function setAnalyzeLoading(isLoading) {
  if (analysisLoading) {
    analysisLoading.classList.toggle('hidden', !isLoading);
  }

  if (analysisEmpty && isLoading) {
    analysisEmpty.classList.add('hidden');
  }

  if (analyzeSubmitBtn) {
    analyzeSubmitBtn.disabled = isLoading;
    analyzeSubmitBtn.textContent = isLoading ? 'Анализ...' : 'Анализировать';
  }

  if (videoUrlInput) {
    videoUrlInput.disabled = isLoading;
  }
}

function setTaskVisible() {
  if (taskPlaceholder) {
    taskPlaceholder.classList.add('hidden');
  }
  if (taskBox) {
    taskBox.classList.remove('hidden');
  }
}

function setTaskProgressState(state) {
  if (!taskProgress) {
    return;
  }

  taskProgress.classList.remove('is-active', 'is-done', 'is-error');

  if (state === 'done') {
    taskProgress.classList.add('is-done');
  } else if (state === 'error') {
    taskProgress.classList.add('is-error');
  } else {
    taskProgress.classList.add('is-active');
  }
}

function setTaskBadgeState(state) {
  if (!taskStatusBadge) {
    return;
  }

  taskStatusBadge.classList.remove('is-active', 'is-done', 'is-error');

  if (state === 'done') {
    taskStatusBadge.classList.add('is-done');
  } else if (state === 'error') {
    taskStatusBadge.classList.add('is-error');
  } else {
    taskStatusBadge.classList.add('is-active');
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetch(url, options);
  const text = await response.text();

  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { detail: text || 'Ошибка ответа сервера' };
  }

  if (!response.ok) {
    throw new Error(data.detail || `Ошибка запроса (${response.status})`);
  }

  return data;
}

async function postForm(url, formData) {
  return fetchJson(url, {
    method: 'POST',
    body: formData,
  });
}

function downloadThumbnail() {
  if (!currentAnalysis || !currentAnalysis.thumbnail_url) {
    showToast('Обложка для этого видео недоступна.', 'error');
    return;
  }

  const link = document.createElement('a');
  link.href = currentAnalysis.thumbnail_url;
  link.target = '_blank';
  link.rel = 'noopener';
  link.download = '';
  document.body.appendChild(link);
  link.click();
  link.remove();

  showToast('Открываю обложку.', 'info');
}


function updateQualitySelect(settings) {
  if (!qualitySelect || !settings) {
    return;
  }
  const options = settings.quality_options || [720, 1080];
  qualitySelect.innerHTML = '';
  options.forEach((height) => {
    const option = document.createElement('option');
    option.value = String(height);
    option.textContent = height === 2160 ? '4K / 2160p' : `${height}p`;
    qualitySelect.appendChild(option);
  });
  const defaultValue = String(settings.default_user_quality || 1080);
  if ([...qualitySelect.options].some((opt) => opt.value === defaultValue)) {
    qualitySelect.value = defaultValue;
  }
}

function selectedQualityHeight() {
  if (!qualitySelect || !qualitySelect.value) {
    return null;
  }
  const parsed = Number.parseInt(qualitySelect.value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function renderAnalysis(data) {
  currentAnalysis = data;

  if (analysisEmpty) {
    analysisEmpty.classList.add('hidden');
  }
  if (analysisBox) {
    analysisBox.classList.remove('hidden');
  }

  if (videoTitle) {
    videoTitle.textContent = data.title || 'Видео';
  }
  if (videoUrlView) {
    videoUrlView.textContent = data.url || '';
  }
  if (formatsList) {
    formatsList.innerHTML = '';
  }

  updateQualitySelect(data.settings || {});
  if (proxyDownloadBtn) {
    const enabled = Boolean(data.settings && data.settings.experimental_proxy_download_enabled);
    proxyDownloadBtn.classList.toggle('hidden', !enabled);
    proxyDownloadBtn.disabled = !enabled;
  }
  if (proxyHelp) {
    proxyHelp.classList.toggle('hidden', !(data.settings && data.settings.experimental_proxy_download_enabled));
  }
  if (proxyResult) {
    proxyResult.classList.add('hidden');
  }

  if (thumbnailBtn) {
    if (data.thumbnail_url) {
      thumbnailBtn.classList.remove('hidden');
      thumbnailBtn.disabled = false;
    } else {
      thumbnailBtn.classList.add('hidden');
      thumbnailBtn.disabled = true;
    }
  }

  if (Array.isArray(data.formats) && data.formats.length) {
    data.formats.forEach((item) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'secondary-btn format-btn';
      btn.textContent = item.label;
      btn.addEventListener('click', () => startDownload('pick', item.format_id));
      formatsList.appendChild(btn);
    });
  } else {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.textContent = 'Форматы не удалось отобразить. Можно использовать режимы скачивания выше.';
    formatsList.appendChild(empty);
  }

  showToast('Анализ завершён. Можно выбирать формат.', 'info');
}

function updateTaskUi(task) {
  setTaskVisible();

  const statusText = task.status_label || task.status || 'Статус';
  const detailText = task.detail || 'Обработка';
  const titleText = task.title || (currentAnalysis && currentAnalysis.title) || 'Видео';
  const notifyKey = task.task_id || currentTaskId || 'task';

  taskStatusBadge.textContent = statusText;
  taskTitle.textContent = titleText;
  taskDetail.textContent = detailText;

  if (task.status === 'done') {
    setTaskBadgeState('done');
    setTaskProgressState('done');
    taskResult.classList.remove('hidden');
    taskError.classList.add('hidden');
    if (taskCancelBtn) {
      taskCancelBtn.classList.add('hidden');
      taskCancelBtn.disabled = false;
      taskCancelBtn.textContent = 'Отмена';
    }
    taskDownloadLink.href = task.download_url;
    if (taskWatchLink) {
      taskWatchLink.href = task.watch_url || task.download_url;
      taskWatchLink.classList.toggle('hidden', !(task.watch_url || task.download_url));
    }

    if (!completedNotifiedTaskIds.has(notifyKey)) {
      completedNotifiedTaskIds.add(notifyKey);
      playSuccessSound();
      showToast('Файл готов. Можно скачивать.', 'success', 5000);
    }
  } else if (task.status === 'error') {
    setTaskBadgeState('error');
    setTaskProgressState('error');
    taskResult.classList.add('hidden');
    taskError.classList.remove('hidden');
    taskError.textContent = task.error || task.detail || 'Неизвестная ошибка';
    if (taskCancelBtn) {
      taskCancelBtn.classList.add('hidden');
      taskCancelBtn.disabled = false;
      taskCancelBtn.textContent = 'Отмена';
    }

    if (!errorNotifiedTaskIds.has(notifyKey)) {
      errorNotifiedTaskIds.add(notifyKey);
      showToast(`Ошибка: ${task.error || task.detail || 'неизвестная ошибка'}`, 'error', 5500);
    }
  } else if (task.status === 'cancelled') {
    setTaskBadgeState('error');
    setTaskProgressState('error');
    taskResult.classList.add('hidden');
    taskError.classList.remove('hidden');
    taskError.textContent = task.error || task.detail || 'Скачивание отменено.';
    if (taskCancelBtn) {
      taskCancelBtn.classList.add('hidden');
      taskCancelBtn.disabled = false;
      taskCancelBtn.textContent = 'Отмена';
    }
  } else {
    setTaskBadgeState('active');
    setTaskProgressState('active');
    taskResult.classList.add('hidden');
    taskError.classList.add('hidden');
    if (taskCancelBtn) {
      taskCancelBtn.classList.remove('hidden');
      taskCancelBtn.disabled = task.status === 'cancelling' || Boolean(task.cancel_requested);
      taskCancelBtn.textContent = taskCancelBtn.disabled ? 'Отменяю...' : 'Отмена';
    }
  }
}

async function cancelCurrentTask() {
  if (!currentTaskId) {
    return;
  }

  try {
    if (taskCancelBtn) {
      taskCancelBtn.disabled = true;
      taskCancelBtn.textContent = 'Отменяю...';
    }
    const data = await fetchJson(apiUrl(`/api/task/${currentTaskId}/cancel`), { method: 'POST' });
    if (data.task) {
      updateTaskUi(data.task);
    }
    showToast('Отмена запрошена.', 'info');
  } catch (err) {
    if (taskCancelBtn) {
      taskCancelBtn.disabled = false;
      taskCancelBtn.textContent = 'Отмена';
    }
    showToast(err.message, 'error');
  }
}

async function pollTask(taskId) {
  try {
    const task = await fetchJson(apiUrl(`/api/task/${taskId}`));
    updateTaskUi(task);

    if (task.done) {
      currentTaskId = null;
      if (pollingTimer) {
        clearInterval(pollingTimer);
        pollingTimer = null;
      }
    }
  } catch (err) {
    if (pollingTimer) {
      clearInterval(pollingTimer);
      pollingTimer = null;
    }
    setTaskProgressState('error');
    setTaskBadgeState('error');
    showToast(err.message, 'error');
  }
}

async function startDownload(mode, formatId = null) {
  if (!currentAnalysis) {
    showToast('Сначала выполни анализ ссылки.', 'error');
    return;
  }

  const form = new FormData();
  form.append('url', currentAnalysis.url);
  form.append('mode', mode);
  form.append('title', currentAnalysis.title || 'Видео');
  const qualityHeight = selectedQualityHeight();
  if (qualityHeight && mode !== 'audio' && mode !== 'pick') {
    form.append('quality_height', String(qualityHeight));
  }

  if (formatId) {
    form.append('format_id', formatId);
  }

  try {
    const data = await postForm(apiUrl('/api/download'), form);

    currentTaskId = data.task_id;

    setTaskVisible();
    setTaskBadgeState('active');
    setTaskProgressState('active');

    taskStatusBadge.textContent = data.status_label || 'Создано';
    taskTitle.textContent = currentAnalysis.title || 'Видео';
    taskDetail.textContent = data.detail || 'Задача создана, ожидаю выполнение';
    taskResult.classList.add('hidden');
    taskError.classList.add('hidden');
    if (taskCancelBtn) {
      taskCancelBtn.classList.remove('hidden');
      taskCancelBtn.disabled = false;
      taskCancelBtn.textContent = 'Отмена';
    }

    completedNotifiedTaskIds.delete(currentTaskId);
    errorNotifiedTaskIds.delete(currentTaskId);

    if (pollingTimer) {
      clearInterval(pollingTimer);
    }

    showToast('Задача запущена.', 'info');
    await pollTask(currentTaskId);
    pollingTimer = setInterval(() => pollTask(currentTaskId), 2000);
  } catch (err) {
    setTaskProgressState('error');
    setTaskBadgeState('error');
    showToast(err.message, 'error');
  }
}


async function startProxyDownload() {
  if (!currentAnalysis) {
    showToast('Сначала выполни анализ ссылки.', 'error');
    return;
  }

  if (proxyResult) {
    proxyResult.classList.add('hidden');
  }

  const form = new FormData();
  form.append('url', currentAnalysis.url);
  form.append('mode', 'safe');
  const qualityHeight = selectedQualityHeight();
  if (qualityHeight) {
    form.append('quality_height', String(qualityHeight));
  }

  try {
    setButtonLoading(proxyDownloadBtn, true, 'Прокси-скачивание', 'Готовлю ссылки...');
    const data = await fetchJson(apiUrl('/api/proxy-download'), {
      method: 'POST',
      body: form,
    });

    if (proxyVideoLink && data.video && data.video.url) {
      proxyVideoLink.href = data.video.url;
      proxyVideoLink.download = data.video.filename || '';
      proxyVideoLink.textContent = data.video.size_text
        ? `Скачать видеодорожку (${data.video.size_text})`
        : 'Скачать видеодорожку';
    }

    if (proxyAudioLink && data.audio && data.audio.url) {
      proxyAudioLink.href = data.audio.url;
      proxyAudioLink.download = data.audio.filename || '';
      proxyAudioLink.textContent = data.audio.size_text
        ? `Скачать аудиодорожку (${data.audio.size_text})`
        : 'Скачать аудиодорожку';
    }

    if (proxyResultMeta) {
      const videoLabel = data.video && data.video.label ? data.video.label : 'видеодорожка';
      const audioLabel = data.audio && data.audio.label ? data.audio.label : 'аудиодорожка';
      const ttl = data.expires_in_minutes || 15;
      proxyResultMeta.textContent = `${videoLabel} · ${audioLabel}. Ссылки действуют ${ttl} мин.`;
    }

    if (proxyResult) {
      proxyResult.classList.remove('hidden');
    }
    showToast('Прокси-ссылки подготовлены.', 'success');
  } catch (err) {
    showToast(err.message, 'error', 6000);
  } finally {
    setButtonLoading(proxyDownloadBtn, false, 'Прокси-скачивание', 'Готовлю ссылки...');
  }
}

function setButtonLoading(button, loading, idleText, loadingText) {
  if (!button) {
    return;
  }
  button.disabled = loading;
  button.textContent = loading ? loadingText : idleText;
}

async function copyTextToClipboard(text, successMessage = 'Скопировано.') {
  if (!text) {
    showToast('Нечего копировать.', 'error');
    return;
  }

  try {
    await navigator.clipboard.writeText(text);
    showToast(successMessage, 'success');
  } catch (err) {
    showToast('Не удалось скопировать в буфер обмена.', 'error');
  }
}


function setInputValue(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.value = value == null ? '' : String(value);
  }
}

function setCheckboxValue(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.checked = Boolean(value);
  }
}

function applySettingsToForm(settings) {
  if (!settings || !adminSettingsForm) {
    return;
  }
  setInputValue('setting-download-retention', settings.download_retention_minutes);
  setInputValue('setting-watch-extend', settings.watch_extend_minutes);
  setInputValue('setting-max-file-gb', settings.max_single_file_gb);
  setInputValue('setting-max-dir-gb', settings.max_download_dir_gb);
  setInputValue('setting-min-free-gb', settings.min_free_disk_gb);
  setInputValue('setting-max-video-height', settings.max_video_height);
  setInputValue('setting-default-quality', settings.default_user_quality);
  setCheckboxValue('setting-extend-watch', settings.extend_expiry_on_watch);
  setCheckboxValue('setting-user-quality', settings.user_quality_selection_enabled);
  setCheckboxValue('setting-unlimited-file', settings.allow_unlimited_file_size);
  setCheckboxValue('setting-unlimited-dir', settings.allow_unlimited_download_dir);
  setCheckboxValue('setting-unlimited-quality', settings.allow_unlimited_quality);
  setCheckboxValue('setting-proxy-enabled', settings.experimental_proxy_download_enabled);
  setInputValue('setting-proxy-max-gb', settings.experimental_proxy_max_file_gb);
  setInputValue('setting-proxy-minutes', settings.experimental_proxy_max_duration_minutes);
}

function renderAdminFiles(files) {
  if (!adminFilesList || !adminFilesEmpty) {
    return;
  }

  adminFilesList.innerHTML = '';

  if (!Array.isArray(files) || !files.length) {
    adminFilesEmpty.classList.remove('hidden');
    return;
  }

  adminFilesEmpty.classList.add('hidden');

  files.forEach((file) => {
    const item = document.createElement('article');
    item.className = 'admin-file-item';
    item.innerHTML = `
      <div class="admin-file-main">
        <strong class="mono-input">${escapeHtml(file.stored_filename || 'файл')}</strong>
        <div class="muted admin-file-meta">
          Пользователь: ${escapeHtml(file.user_label || file.user_id || '—')} ·
          Размер: ${escapeHtml(file.file_size_text || '—')} ·
          Качество: ${escapeHtml(file.quality_label || '—')} ·
          Осталось: ${escapeHtml(file.time_left_text || '—')} ·
          Просмотров: ${escapeHtml(file.access_count ?? 0)}
        </div>
      </div>
      <div class="admin-file-actions-row">
        <button class="ghost-btn small-btn danger-btn admin-file-delete-btn" type="button">Удалить</button>
      </div>
    `;

    item.querySelector('.admin-file-delete-btn')?.addEventListener('click', async () => {
      if (!window.confirm(`Удалить файл ${file.stored_filename || ''}?`)) {
        return;
      }
      try {
        const data = await fetchJson(apiUrl(`/api/admin/files/${file.file_id}/delete`), { method: 'POST' });
        renderAdminFiles(data.files || []);
        applyAdminOverview(data.overview);
        showToast('Файл удалён.', 'success');
      } catch (err) {
        showToast(err.message, 'error');
      }
    });

    adminFilesList.appendChild(item);
  });
}

function renderActiveTasks(tasks) {
  if (!adminActiveTasks) {
    return;
  }

  adminActiveTasks.innerHTML = '';

  if (!Array.isArray(tasks) || !tasks.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-state compact-empty';
    empty.textContent = 'Пока нет активных задач.';
    adminActiveTasks.appendChild(empty);
    return;
  }

  tasks.forEach((task) => {
    const item = document.createElement('article');
    item.className = 'mini-task-card';
    item.innerHTML = `
      <div class="mini-task-head">
        <strong>${escapeHtml(task.user_label || task.user_id || 'Пользователь')}</strong>
        <span class="status-inline status-mini">${escapeHtml(task.status_label || 'Статус')}</span>
      </div>
      <div class="mini-task-title">${escapeHtml(task.title || 'Видео')}</div>
      <div class="mini-task-detail muted">${escapeHtml(task.detail || '')}</div>
      <div class="mini-task-meta muted">Обновлено: ${escapeHtml(formatDateToLocal(task.updated_at))}</div>
      <div class="mini-task-actions">
        <button class="ghost-btn small-btn danger-btn admin-task-cancel-btn" type="button" ${task.cancel_requested ? 'disabled' : ''}>
          ${task.cancel_requested ? 'Отменяю...' : 'Отмена'}
        </button>
      </div>
    `;

    item.querySelector('.admin-task-cancel-btn')?.addEventListener('click', async () => {
      if (!window.confirm('Отменить это скачивание?')) {
        return;
      }
      try {
        const data = await fetchJson(apiUrl(`/api/admin/tasks/${task.task_id}/cancel`), { method: 'POST' });
        if (data.overview) {
          applyAdminOverview(data.overview);
        }
        showToast('Отмена скачивания запрошена.', 'success');
      } catch (err) {
        showToast(err.message, 'error');
      }
    });

    adminActiveTasks.appendChild(item);
  });
}

function statusChipClass(status) {
  if (status === 'activated') {
    return 'status-chip status-chip-ok';
  }
  if (status === 'revoked') {
    return 'status-chip status-chip-error';
  }
  return 'status-chip status-chip-warn';
}

function escapeHtml(value) {
  return String(value == null ? '' : value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function renderInvites(invites) {
  if (!invitesList || !invitesEmpty) {
    return;
  }

  invitesList.innerHTML = '';

  if (!Array.isArray(invites) || !invites.length) {
    invitesEmpty.classList.remove('hidden');
    return;
  }

  invitesEmpty.classList.add('hidden');

  invites.forEach((invite) => {
    const article = document.createElement('article');
    article.className = 'invite-item';
    article.innerHTML = `
      <div class="invite-main">
        <div class="invite-main-head">
          <div>
            <div class="invite-title-row">
              <strong class="invite-title">${escapeHtml(invite.access_label || invite.label || 'Без имени')}</strong>
              <span class="${statusChipClass(invite.status)}">${escapeHtml(invite.status_label || 'Статус')}</span>
            </div>
            <div class="muted invite-subline">Создано: ${escapeHtml(formatDateToLocal(invite.created_at))}</div>
          </div>
        </div>

        <div class="invite-info-grid">
          <div class="invite-info-box">
            <div class="invite-info-label">Ссылка</div>
            <div class="copy-line copy-line-tight">
              <input class="text-input mono-input invite-url-input" type="text" value="${escapeHtml(invite.invite_url || '')}" readonly>
              <button class="ghost-btn small-btn invite-copy-btn" type="button">Копировать</button>
            </div>
          </div>

          <div class="invite-meta-grid">
            <div class="invite-meta-item">
              <span class="muted">Активирован</span>
              <strong>${escapeHtml(invite.activated_at ? formatDateToLocal(invite.activated_at) : 'Нет')}</strong>
            </div>
            <div class="invite-meta-item">
              <span class="muted">Последний вход</span>
              <strong>${escapeHtml(invite.last_seen_at ? formatDateToLocal(invite.last_seen_at) : '—')}</strong>
            </div>
            <div class="invite-meta-item">
              <span class="muted">Активность</span>
              <strong>${escapeHtml(invite.last_activity_at ? formatDateToLocal(invite.last_activity_at) : '—')}</strong>
            </div>
            <div class="invite-meta-item">
              <span class="muted">Скачивание</span>
              <strong>${escapeHtml(invite.last_download_at ? formatDateToLocal(invite.last_download_at) : '—')}</strong>
            </div>
          </div>
        </div>
      </div>

      <div class="invite-actions">
        <button class="ghost-btn small-btn invite-rename-btn" type="button">Переименовать</button>
        <button class="ghost-btn small-btn invite-copy-btn-secondary" type="button">Скопировать ссылку</button>
        <button class="ghost-btn small-btn danger-btn invite-revoke-btn" type="button" ${invite.status === 'revoked' ? 'disabled' : ''}>Удалить доступ</button>
      </div>
    `;

    const copyButtons = article.querySelectorAll('.invite-copy-btn, .invite-copy-btn-secondary');
    copyButtons.forEach((btn) => {
      btn.addEventListener('click', () => copyTextToClipboard(invite.invite_url || '', 'Ссылка скопирована.'));
    });

    const renameBtn = article.querySelector('.invite-rename-btn');
    renameBtn?.addEventListener('click', async () => {
      const currentLabel = invite.access_label || invite.label || '';
      const nextLabel = window.prompt('Новое имя / тег доступа:', currentLabel);
      if (nextLabel === null) {
        return;
      }
      const form = new FormData();
      form.append('label', nextLabel);
      try {
        const data = await postForm(apiUrl(`/api/admin/invites/${invite.invite_id}/label`), form);
        applyAdminOverview(data.overview);
        showToast('Имя доступа обновлено.', 'success');
      } catch (err) {
        showToast(err.message, 'error');
      }
    });

    const revokeBtn = article.querySelector('.invite-revoke-btn');
    revokeBtn?.addEventListener('click', async () => {
      if (!window.confirm(`Удалить доступ для «${invite.access_label || invite.label || 'Без имени'}»?`)) {
        return;
      }
      try {
        const data = await fetchJson(apiUrl(`/api/admin/invites/${invite.invite_id}/revoke`), { method: 'POST' });
        applyAdminOverview(data.overview);
        showToast('Доступ удалён.', 'success');
      } catch (err) {
        showToast(err.message, 'error');
      }
    });

    invitesList.appendChild(article);
  });
}

function applyAdminOverview(overview) {
  if (!overview) {
    return;
  }

  lastAdminOverview = overview;

  if (statActive5) {
    statActive5.textContent = String(overview.stats?.active_last_5m ?? '—');
  }
  if (statActive10) {
    statActive10.textContent = String(overview.stats?.active_last_10m ?? '—');
  }
  if (statActiveTasks) {
    statActiveTasks.textContent = String(overview.stats?.active_tasks ?? '—');
  }
  if (statActiveFiles) {
    statActiveFiles.textContent = String(overview.stats?.active_files ?? '—');
  }
  if (statDownloadUsage) {
    statDownloadUsage.textContent = overview.disk?.download_usage_text || '—';
  }
  if (statDiskFree) {
    statDiskFree.textContent = overview.disk?.disk_free_text || '—';
  }
  if (statSafeRestart) {
    statSafeRestart.textContent = overview.stats?.safe_to_restart ? 'Да' : 'Нет';
    statSafeRestart.classList.toggle('safe-yes', Boolean(overview.stats?.safe_to_restart));
    statSafeRestart.classList.toggle('safe-no', !overview.stats?.safe_to_restart);
  }

  if (universalLoginUrl) {
    universalLoginUrl.value = overview.links?.universal_login_url || '';
  }

  applySettingsToForm(overview.settings || {});
  renderAdminFiles(overview.files || []);
  renderInvites(overview.invites || []);
  renderActiveTasks(overview.active_tasks || []);
}

async function loadAdminOverview(showErrors = false) {
  if (!isAdmin) {
    return;
  }

  try {
    const overview = await fetchJson(apiUrl('/api/admin/overview'));
    applyAdminOverview(overview);
  } catch (err) {
    if (showErrors) {
      showToast(err.message, 'error');
    }
  }
}

async function sendHeartbeat() {
  try {
    await fetchJson(apiUrl('/api/heartbeat'), { method: 'POST' });
  } catch {
    // молча, чтобы не засорять интерфейс
  }
}

function startHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
  }

  sendHeartbeat();
  heartbeatTimer = setInterval(() => {
    if (!document.hidden) {
      sendHeartbeat();
    }
  }, 60000);
}

function startAdminAutoRefresh() {
  if (!isAdmin) {
    return;
  }
  if (adminRefreshTimer) {
    clearInterval(adminRefreshTimer);
  }
  loadAdminOverview(false);
  adminRefreshTimer = setInterval(() => {
    if (!document.hidden) {
      loadAdminOverview(false);
    }
  }, 30000);
}

analyzeForm?.addEventListener('submit', async (event) => {
  event.preventDefault();

  const form = new FormData(analyzeForm);

  try {
    setAnalyzeLoading(true);

    if (analysisBox) {
      analysisBox.classList.add('hidden');
    }
    if (analysisEmpty) {
      analysisEmpty.classList.add('hidden');
    }

    if (thumbnailBtn) {
      thumbnailBtn.classList.add('hidden');
      thumbnailBtn.disabled = true;
    }

    const data = await postForm(apiUrl('/api/analyze'), form);
    renderAnalysis(data);
  } catch (err) {
    if (analysisEmpty) {
      analysisEmpty.classList.remove('hidden');
      analysisEmpty.textContent = err.message;
    }
    showToast(err.message, 'error');
  } finally {
    setAnalyzeLoading(false);
  }
});

document.querySelectorAll('[data-mode]').forEach((btn) => {
  btn.addEventListener('click', () => startDownload(btn.dataset.mode));
});

thumbnailBtn?.addEventListener('click', downloadThumbnail);

cookiesForm?.addEventListener('submit', async (event) => {
  event.preventDefault();

  const form = new FormData(cookiesForm);

  try {
    const data = await postForm(apiUrl('/api/cookies/upload'), form);
    applyCookieStatesFromResponse(data);
    cookiesForm.reset();
    showToast('Ваш cookies.txt успешно загружен.', 'success');
  } catch (err) {
    showToast(err.message, 'error');
  }
});

deleteCookiesBtn?.addEventListener('click', async () => {
  if (!window.confirm('Удалить ваш cookies.txt?')) {
    return;
  }

  try {
    const data = await fetchJson(apiUrl('/api/cookies/delete'), { method: 'POST' });
    applyCookieStatesFromResponse(data);
    showToast('Ваш cookies.txt удалён.', 'info');
  } catch (err) {
    showToast(err.message, 'error');
  }
});

adminCookiesForm?.addEventListener('submit', async (event) => {
  event.preventDefault();

  const form = new FormData(adminCookiesForm);

  try {
    const data = await postForm(apiUrl('/api/admin/cookies/upload'), form);
    applyCookieStatesFromResponse(data);
    adminCookiesForm.reset();
    showToast('Общий cookies.txt администратора загружен.', 'success');
  } catch (err) {
    showToast(err.message, 'error');
  }
});

deleteAdminCookiesBtn?.addEventListener('click', async () => {
  if (!window.confirm('Удалить общий cookies.txt администратора?')) {
    return;
  }

  try {
    const data = await fetchJson(apiUrl('/api/admin/cookies/delete'), { method: 'POST' });
    applyCookieStatesFromResponse(data);
    showToast('Общий cookies.txt удалён.', 'info');
  } catch (err) {
    showToast(err.message, 'error');
  }
});

inviteCreateForm?.addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = new FormData(inviteCreateForm);

  try {
    setButtonLoading(inviteCreateBtn, true, 'Создать ссылку', 'Создаю...');
    const data = await postForm(apiUrl('/api/admin/invites'), form);
    applyAdminOverview(data.overview);

    if (latestInviteBox && latestInviteUrl && openLatestInviteBtn) {
      latestInviteBox.classList.remove('hidden');
      latestInviteUrl.value = data.invite?.invite_url || '';
      openLatestInviteBtn.href = data.invite?.invite_url || '#';
    }

    inviteCreateForm.reset();
    showToast('Разовая ссылка создана.', 'success');
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    setButtonLoading(inviteCreateBtn, false, 'Создать ссылку', 'Создаю...');
  }
});

copyLatestInviteBtn?.addEventListener('click', () => {
  copyTextToClipboard(latestInviteUrl?.value || '', 'Ссылка скопирована.');
});

copyUniversalLoginBtn?.addEventListener('click', () => {
  copyTextToClipboard(universalLoginUrl?.value || '', 'Основная ссылка скопирована.');
});



proxyDownloadBtn?.addEventListener('click', startProxyDownload);

adminSettingsForm?.addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = new FormData(adminSettingsForm);

  try {
    const data = await postForm(apiUrl('/api/admin/settings'), form);
    applySettingsToForm(data.settings || {});
    applyAdminOverview(data.overview);
    showToast('Настройки сохранены.', 'success');
  } catch (err) {
    showToast(err.message, 'error');
  }
});

adminCleanupExpiredBtn?.addEventListener('click', async () => {
  try {
    const data = await fetchJson(apiUrl('/api/admin/files/cleanup-expired'), { method: 'POST' });
    renderAdminFiles(data.files || []);
    applyAdminOverview(data.overview);
    showToast(`Просроченные файлы удалены: ${data.removed || 0}.`, 'success');
  } catch (err) {
    showToast(err.message, 'error');
  }
});

adminCleanupAllFilesBtn?.addEventListener('click', async () => {
  if (!window.confirm('Удалить все загруженные файлы? Это действие нельзя отменить.')) {
    return;
  }
  try {
    const data = await fetchJson(apiUrl('/api/admin/files/cleanup-all'), { method: 'POST' });
    renderAdminFiles(data.files || []);
    applyAdminOverview(data.overview);
    showToast(`Файлы удалены: ${data.removed || 0}.`, 'success');
  } catch (err) {
    showToast(err.message, 'error');
  }
});

taskCancelBtn?.addEventListener('click', cancelCurrentTask);

themeToggleBtn?.addEventListener('click', toggleTheme);

document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    sendHeartbeat();
    if (isAdmin) {
      loadAdminOverview(false);
    }
  }
});

initTheme();
applyHistoryDates();
startHeartbeat();
startAdminAutoRefresh();
