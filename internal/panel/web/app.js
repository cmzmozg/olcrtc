// ai-generated: the whole file (panel).
'use strict';

const $ = (id) => document.getElementById(id);
const STATES = { up: 'в звонке', connecting: 'подключается', down: 'переподключается', error: 'ошибка', stopped: 'выключена' };
const ERRORS = [
  ['wrong password', 'неверный пароль'],
  ['too many attempts', 'слишком много попыток, попробуйте через 15 минут'],
  ['login required', 'нужно войти'],
  ['name must be', 'название: от 1 до 64 печатных символов, без #$%@?<>'],
  ['room must be', 'ссылка на комнату: без пробелов, # и $'],
  ['unknown provider or transport', 'неизвестный провайдер или транспорт'],
  ['does not create rooms', 'этот провайдер не создаёт комнаты'],
  ['not found', 'не найдено'],
];
let state = null;
let shownClient = null;

async function api(method, path, body) {
  const opts = { method, headers: { 'X-Olcrtc-Panel': '1' }, credentials: 'same-origin' };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch('api/' + path, opts);
  const data = await res.json().catch(() => ({}));
  if (res.status === 401 && path !== 'login') {
    showLogin();
    throw new Error('нужно войти');
  }
  if (!res.ok) throw new Error(russian(data.error || res.statusText));
  return data;
}

function russian(msg) {
  const hit = ERRORS.find(([en]) => msg.includes(en));
  return hit ? hit[1] : msg;
}

function el(tag, attrs, ...children) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (k === 'class') node.className = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v);
  }
  for (const c of children) node.append(c);
  return node;
}

function bytes(n) {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i ? n.toFixed(1) : n) + ' ' + units[i];
}

function fail(err) { $('app-error').textContent = err.message; }

async function act(fn) {
  $('app-error').textContent = '';
  try { render(await fn()); return true; } catch (err) { fail(err); return false; }
}

function showLogin() {
  state = null;
  hideLinks();
  resetMonitoring();
  $('app').classList.add('hidden');
  $('login-screen').classList.remove('hidden');
  $('login-pass').focus();
}

function fillSelect(select, items, chosen) {
  if (select.options.length === items.length) return;
  select.replaceChildren(...items.map((v) => el('option', { value: v }, v)));
  select.value = chosen;
}

function render(s) {
  state = s;
  $('login-screen').classList.add('hidden');
  $('app').classList.remove('hidden');
  fillSelect($('room-provider'), s.providers, s.provider);
  fillSelect($('room-transport'), s.transports, s.transport);
  updateCreate();
  $('rooms').replaceChildren(...s.rooms.map(roomRow));
  $('clients').replaceChildren(...s.clients.map(clientRow));
}

function roomRow(r) {
  const actions = el('td', { class: 'actions' });
  if (r.fixed) {
    actions.append(el('span', { class: 'dim' }, 'из конфига'));
  } else {
    actions.append(
      el('button', { class: 'btn btn-sm', onclick: () => act(() => api('PATCH', 'rooms/' + r.id, { disabled: !r.disabled })) },
        r.disabled ? 'ВКЛЮЧИТЬ' : 'ВЫКЛЮЧИТЬ'),
      el('button', { class: 'btn btn-sm', onclick: () => renameRoom(r) }, 'ИЗМЕНИТЬ'),
      el('button', { class: 'btn btn-sm btn-danger', onclick: () => {
        if (confirm('Удалить комнату ' + r.name + '?')) act(() => api('DELETE', 'rooms/' + r.id));
      } }, 'УДАЛИТЬ'));
  }
  const status = el('span', { class: 'state ' + r.state }, STATES[r.state] || r.state);
  if (r.error) status.title = r.error;
  return el('tr', { class: r.disabled ? 'off' : '' },
    el('td', {}, r.name),
    el('td', {}, r.provider + ' / ' + r.transport),
    el('td', { class: 'cut', title: r.room }, r.room),
    el('td', {}, status),
    actions);
}

function renameRoom(r) {
  const name = prompt('Название комнаты', r.name);
  if (name === null) return;
  const room = prompt('Ссылка на комнату', r.room);
  if (room === null) return;
  act(() => api('PATCH', 'rooms/' + r.id, { name, room }));
}

function clientRow(c) {
  return el('tr', { class: c.disabled ? 'off' : '' },
    el('td', {}, c.name),
    el('td', {}, bytes(c.down) + ' / ' + bytes(c.up)),
    el('td', {}, new Date(c.created).toLocaleDateString()),
    el('td', { class: 'cut', title: c.note || '' }, c.note || ''),
    el('td', { class: 'actions' },
      el('button', { class: 'btn btn-sm btn-primary', onclick: () => showLinks(c) }, 'ССЫЛКИ'),
      el('button', { class: 'btn btn-sm', onclick: () => act(() => api('PATCH', 'clients/' + c.id, { disabled: !c.disabled })) },
        c.disabled ? 'ВКЛЮЧИТЬ' : 'ОТКЛЮЧИТЬ'),
      el('button', { class: 'btn btn-sm', onclick: () => {
        if (confirm('Выдать новую ссылку подписки? Старая перестанет работать.')) {
          act(() => api('PATCH', 'clients/' + c.id, { rotate_sub: true })).then((ok) => ok && showLinks(c));
        }
      } }, 'НОВАЯ ПОДПИСКА'),
      el('button', { class: 'btn btn-sm btn-danger', onclick: () => {
        if (confirm('Удалить клиента ' + c.name + '? Его ключ перестанет работать во всех комнатах.')) {
          if (shownClient === c.id) hideLinks();
          act(() => api('DELETE', 'clients/' + c.id));
        }
      } }, 'УДАЛИТЬ')));
}

function copyButton(input) {
  return el('button', { class: 'btn copy', onclick: () => copy(input) }, 'КОПИРОВАТЬ');
}

async function copy(input) {
  input.select();
  try { await navigator.clipboard.writeText(input.value); } catch (_) { document.execCommand('copy'); }
}

async function showLinks(c) {
  try {
    const data = await api('GET', 'clients/' + c.id + '/links');
    shownClient = c.id;
    $('links-name').textContent = '· ' + c.name;
    $('links-sub').value = data.sub;
    $('links-list').replaceChildren(...data.links.map((l) => {
      const input = el('input', { class: 'field wide mono', readonly: '' });
      input.value = l.link;
      return el('div', { class: 'link-row' },
        el('div', { class: 'label' }, l.room.toUpperCase() + (l.disabled ? ' (выключена)' : '')),
        el('div', { class: 'row' }, input, copyButton(input)));
    }));
    $('links').classList.remove('hidden');
    $('links').scrollIntoView({ behavior: 'smooth' });
  } catch (err) { fail(err); }
}

function hideLinks() {
  shownClient = null;
  $('links').classList.add('hidden');
  $('links-sub').value = '';
  $('links-list').replaceChildren();
}

function updateCreate() {
  const creates = state && state.creators && state.creators.includes($('room-provider').value);
  $('room-create').classList.toggle('hidden', !creates);
}

function roomBody(create) {
  return {
    name: $('room-name').value, provider: $('room-provider').value, transport: $('room-transport').value,
    room: $('room-url').value, create,
  };
}

function resetRoomForm() { $('room-name').value = ''; $('room-url').value = ''; }

document.addEventListener('DOMContentLoaded', () => {
  $('login-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    $('login-error').textContent = '';
    try {
      await api('POST', 'login', { password: $('login-pass').value });
      $('login-pass').value = '';
      render(await api('GET', 'state'));
    } catch (err) { $('login-error').textContent = err.message; }
  });
  $('logout').addEventListener('click', async () => {
    hideLinks();
    await api('POST', 'logout').catch(() => {});
    showLogin();
  });
  $('room-form').addEventListener('submit', (e) => {
    e.preventDefault();
    act(() => api('POST', 'rooms', roomBody(false))).then((ok) => ok && resetRoomForm());
  });
  $('room-create').addEventListener('click', () => {
    if (!$('room-name').reportValidity()) return;
    act(() => api('POST', 'rooms', roomBody(true))).then((ok) => ok && resetRoomForm());
  });
  $('room-provider').addEventListener('change', updateCreate);
  $('client-form').addEventListener('submit', (e) => {
    e.preventDefault();
    act(() => api('POST', 'clients', { name: $('client-name').value, note: $('client-note').value }))
      .then((ok) => { if (ok) { $('client-name').value = ''; $('client-note').value = ''; } });
  });
  for (const b of document.querySelectorAll('.copy[data-for]')) {
    b.addEventListener('click', () => copy($(b.dataset.for)));
  }
  $('links-close').addEventListener('click', hideLinks);
  api('GET', 'state').then(render).catch(() => {});
  setInterval(() => { if (state && !document.hidden) refreshView().catch(fail); }, 5000);
});
