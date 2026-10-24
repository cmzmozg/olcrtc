// ai-generated: monitoring views, bounded CPU/RAM chart and searchable administrator journal.
'use strict';

let activeView = 'manage';
let metricHistory = [];
let auditEntries = [];
let refreshing = false;
let viewGeneration = 0;
const ACTIONS = {
  login: 'Вход', logout: 'Выход', 'room-create': 'Добавление комнаты',
  'room-update': 'Изменение комнаты', 'room-delete': 'Удаление комнаты',
  'client-create': 'Добавление клиента', 'client-update': 'Изменение клиента',
  'client-delete': 'Удаление клиента', 'client-links': 'Просмотр ссылок',
};

// ai-generated: clear private data when the authenticated session ends.
function resetMonitoring() {
  viewGeneration++;
  metricHistory = [];
  auditEntries = [];
  $('audit-entries').replaceChildren();
  $('metrics-cards').replaceChildren();
  $('metrics-chart').getContext('2d').clearRect(0, 0, 960, 200);
  $('audit-filter').value = '';
  selectView('manage');
}

// ai-generated: poll only the selected view, discarding replies after logout or navigation.
async function refreshView() {
  if (refreshing) return;
  refreshing = true;
  const generation = viewGeneration;
  try {
    const view = activeView;
    const data = await api('GET', { manage: 'state', monitor: 'metrics', audit: 'audit' }[view]);
    if (generation !== viewGeneration) return;
    if (view === 'manage') render(data);
    if (view === 'monitor') renderMetrics(data);
    if (view === 'audit') {
      auditEntries = data.entries || [];
      $('audit-error').textContent = data.error ? 'Журнал не сохраняется на диск. Проверьте место и права каталога данных.' : '';
      renderAudit();
    }
    $('app-error').textContent = '';
  } finally { refreshing = false; }
}

// ai-generated: switch sections with keyboard-accessible native buttons.
function selectView(view) {
  activeView = view;
  viewGeneration++;
  for (const button of document.querySelectorAll('[data-view]')) {
    const selected = button.dataset.view === view;
    button.classList.toggle('btn-primary', selected);
    button.setAttribute('aria-pressed', String(selected));
    $('view-' + button.dataset.view).classList.toggle('hidden', !selected);
  }
  if (state) refreshView().catch(fail);
}

// ai-generated: format process and host uptime without date conversion.
function duration(seconds) {
  const n = Math.floor(seconds);
  return Math.floor(n / 86400) + 'д ' + Math.floor(n % 86400 / 3600) + 'ч ' + Math.floor(n % 3600 / 60) + 'м';
}

// ai-generated: display independently available host metrics and panel counters.
function renderMetrics(m) {
  const h = m.host;
  const memory = h.mem_total ? h.mem_used / h.mem_total * 100 : null;
  const cpu = h.cpu_available ? h.cpu_percent : null;
  const cards = [
    ['CPU хоста', cpu === null ? 'ожидание замера' : cpu.toFixed(1) + ' %', m.cpus + ' логических CPU'],
    ['RAM хоста', memory === null ? 'нет данных' : memory.toFixed(1) + ' %', h.mem_total ? bytes(h.mem_used) + ' / ' + bytes(h.mem_total) : ''],
    ['Диск данных', h.disk_total ? bytes(h.disk_free) : 'нет данных', h.disk_total ? 'свободно из ' + bytes(h.disk_total) : ''],
    ['Нагрузка 1 / 5 / 15 мин', h.load ? h.load.map((v) => v.toFixed(2)).join(' / ') : 'нет данных', 'load average хоста'],
    ['Время работы', duration(m.uptime_sec), 'хост: ' + (h.uptime_sec ? duration(h.uptime_sec) : 'нет данных')],
    ['Процесс olcrtc', bytes(m.heap_bytes), m.goroutines + ' горутин / Go heap'],
    ['Комнаты в звонке', m.rooms_up + ' / ' + m.rooms, 'всего комнат'],
    ['Включённые клиенты', m.clients_enabled + ' / ' + m.clients, 'всего клиентов'],
    ['Трафик ↓ / ↑', bytes(m.traffic.down) + ' / ' + bytes(m.traffic.up), 'завершённые потоки, с запуска'],
  ];
  $('metrics-cards').replaceChildren(...cards.map(([label, value, detail]) =>
    el('div', { class: 'card metric' }, el('div', { class: 'micro' }, label),
      el('div', { class: 'metric-value' }, value), el('div', { class: 'hint' }, detail))));
  $('metrics-at').textContent = 'Обновлено ' + new Date(m.at).toLocaleTimeString();
  metricHistory.push({ at: Date.parse(m.at), cpu, memory });
  metricHistory = metricHistory.filter((v) => v.at >= Date.parse(m.at) - 300000).slice(-60);
  drawMetrics();
}

// ai-generated: draw a bounded time chart using local canvas only.
function drawMetrics() {
  const canvas = $('metrics-chart');
  const ctx = canvas.getContext('2d');
  const colors = getComputedStyle(document.documentElement);
  const end = metricHistory.at(-1)?.at || Date.now();
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.font = '12px monospace';
  for (const percent of [0, 50, 100]) {
    const y = 180 - percent * 1.6;
    ctx.strokeStyle = colors.getPropertyValue('--hairline');
    ctx.beginPath(); ctx.moveTo(40, y); ctx.lineTo(950, y); ctx.stroke();
    ctx.fillStyle = colors.getPropertyValue('--ink-mute'); ctx.fillText(percent + '%', 0, y + 4);
  }
  for (const [key, color] of [['cpu', '--signal'], ['memory', '--good']]) {
    ctx.strokeStyle = colors.getPropertyValue(color);
    ctx.fillStyle = ctx.strokeStyle;
    ctx.lineWidth = 2;
    ctx.beginPath();
    let connected = false;
    for (const sample of metricHistory) {
      if (sample[key] === null) { connected = false; continue; }
      const x = 40 + (sample.at - end + 300000) / 300000 * 910;
      const y = 180 - sample[key] * 1.6;
      if (connected) ctx.lineTo(x, y); else ctx.moveTo(x, y);
      connected = true;
      ctx.fillRect(x - 2, y - 2, 4, 4);
    }
    ctx.stroke();
  }
}

// ai-generated: filter and render audit values as text, never HTML.
function renderAudit() {
  const query = $('audit-filter').value.toLocaleLowerCase().trim();
  const entries = auditEntries.filter((e) =>
    [e.ip, e.action, ACTIONS[e.action] || '', e.target || '', String(e.status)].join(' ').toLocaleLowerCase().includes(query));
  $('audit-entries').replaceChildren(...entries.map((e) =>
    el('tr', {}, el('td', {}, new Date(e.at).toLocaleString()), el('td', {}, e.ip),
      el('td', {}, ACTIONS[e.action] || e.action), el('td', { class: 'mono' }, e.target || '-'),
      el('td', { class: e.status < 400 ? 'audit-ok' : 'audit-failed' },
        (e.status < 400 ? 'Успешно' : 'Ошибка') + ' (' + e.status + ')'))));
  if (!entries.length) $('audit-entries').append(el('tr', {}, el('td', { colspan: '5', class: 'hint' }, 'Нет событий')));
}

// ai-generated: export the currently loaded bounded journal without credentials.
function exportAudit() {
  const url = URL.createObjectURL(new Blob([JSON.stringify(auditEntries, null, 2)], { type: 'application/json' }));
  const link = el('a', { href: url, download: 'olcrtc-audit.json' });
  link.click();
  URL.revokeObjectURL(url);
}

document.addEventListener('DOMContentLoaded', () => {
  for (const button of document.querySelectorAll('[data-view]')) {
    button.addEventListener('click', () => selectView(button.dataset.view));
  }
  $('audit-filter').addEventListener('input', renderAudit);
  $('audit-refresh').addEventListener('click', () => refreshView().catch(fail));
  $('audit-export').addEventListener('click', exportAudit);
});
