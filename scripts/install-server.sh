#!/bin/bash
#
# olcrtc: установка и обновление сервера с веб-панелью.
#
# ai-generated: the whole file (panel installer, after Mirage's).
#
# Нужны только этот скрипт и бинарник olcrtc рядом с ним: распакуйте архив
# из scripts/build-server.sh на сервере и запускайте оттуда от root.
#
#   ./install-server.sh                         # установка: панель на 127.0.0.1:8884
#   ./install-server.sh --room https://vk.ru/call/join/...   # и комната в конфиге
#   ./install-server.sh --panel-public --cert c.pem --key k.pem
#   ./install-server.sh --update                # бэкап, новый бинарь, проверка, откат
#   ./install-server.sh --uninstall             # снять службу, данные оставить
#
# Скрипт идемпотентен: повторный запуск обновляет бинарь и юнит, но не
# трогает конфиг, комнаты и клиентов панели. Туннелю olcrtc входящие порты
# не нужны: он сам ходит к сервису звонков. Порт открывается только для
# публичной панели.

set -Eeuo pipefail

PROVIDER="vkcalls"
TRANSPORT="vp8channel"
ROOM=""
FPS="60"
DNS="1.1.1.1:53"
PANEL_PORT="8884"
PANEL_PATH=""
PANEL_PASS=""
PANEL_PUBLIC=0
PANEL_ENABLED=1
CERT=""
KEY=""
PUBLIC_URL=""
MODE="install"

NAME="olcrtc"
BIN="/usr/local/bin/olcrtc"
CONF_DIR="/etc/olcrtc"
CONF="$CONF_DIR/server.yaml"
UNIT="/etc/systemd/system/$NAME.service"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

say()  { printf '  %s\n' "$*"; }
step() { printf '\n\033[1m> %s\033[0m\n' "$*"; }
warn() { printf '  \033[33m! %s\033[0m\n' "$*"; }
die()  { printf '\n\033[31mx %s\033[0m\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Использование: ./install-server.sh [опции]

  --provider <name>      провайдер по умолчанию (vkcalls)
  --transport <name>     транспорт по умолчанию (vp8channel)
  --room <link>          комната в конфиге; без неё комнаты добавляются в панели
  --fps <n>              кадров в секунду vp8channel (60)
  --dns <host:port>      DNS сервера (1.1.1.1:53)
  --panel-port <n>       порт панели (8884)
  --panel-path <str>     секретный путь панели (по умолчанию случайный)
  --panel-password <pw>  пароль панели (по умолчанию случайный)
  --panel-public         слушать 0.0.0.0, нужны --cert и --key
  --cert <file>          TLS-сертификат публичной панели (копируется в /etc/olcrtc)
  --key <file>           TLS-ключ публичной панели (копируется в /etc/olcrtc)
  --public-url <url>     адрес для ссылок подписки, например https://vpn.example.com:8884
  --no-panel             без панели: комната и ключ только из конфига
  --update               обновить бинарь, проверить, при сбое откатить
  --uninstall            снять службу (данные остаются)
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --provider)       PROVIDER="${2:?}"; shift 2 ;;
        --transport)      TRANSPORT="${2:?}"; shift 2 ;;
        --room)           ROOM="${2:?}"; shift 2 ;;
        --fps)            FPS="${2:?}"; shift 2 ;;
        --dns)            DNS="${2:?}"; shift 2 ;;
        --panel-port)     PANEL_PORT="${2:?}"; shift 2 ;;
        --panel-path)     PANEL_PATH="${2:?}"; shift 2 ;;
        --panel-password) PANEL_PASS="${2:?}"; shift 2 ;;
        --panel-public)   PANEL_PUBLIC=1; shift ;;
        --cert)           CERT="${2:?}"; shift 2 ;;
        --key)            KEY="${2:?}"; shift 2 ;;
        --public-url)     PUBLIC_URL="${2:?}"; shift 2 ;;
        --no-panel)       PANEL_ENABLED=0; shift ;;
        --update)         MODE="update"; shift ;;
        --uninstall)      MODE="uninstall"; shift ;;
        -h|--help)        usage; exit 0 ;;
        *) die "неизвестный аргумент: $1 (--help для справки)" ;;
    esac
done

[ "$(id -u)" -eq 0 ] || die "нужны права root: sudo ./install-server.sh"
command -v systemctl >/dev/null 2>&1 || die "скрипт рассчитан на systemd"

# ── Помощники ────────────────────────────────────────────────────────────

# random_hex n - n случайных байт в hex.
random_hex() { head -c "$1" /dev/urandom | od -An -tx1 | tr -d ' \n'; }

# conf_value key - значение ключа panel из конфига (без кавычек).
conf_value() {
    sed -n "s/^[[:space:]]*$1:[[:space:]]*\"\{0,1\}\([^\"]*\)\"\{0,1\}[[:space:]]*$/\1/p" "$CONF" | head -1
}

open_port() {
    local port="$1"
    if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -qi "status: active"; then
        ufw allow "$port/tcp" >/dev/null
    elif command -v firewall-cmd >/dev/null 2>&1 && firewall-cmd --state >/dev/null 2>&1; then
        firewall-cmd --permanent --add-port="$port/tcp" >/dev/null && firewall-cmd --reload >/dev/null
    elif command -v iptables >/dev/null 2>&1; then
        iptables -C INPUT -p tcp --dport "$port" -j ACCEPT 2>/dev/null ||
            iptables -I INPUT -p tcp --dport "$port" -j ACCEPT
    fi
}

write_unit() {
    cat > "$UNIT" <<EOF
[Unit]
Description=olcrtc tunnel server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$BIN $CONF
Restart=always
RestartSec=3
User=$NAME
Group=$NAME
UMask=0077
WorkingDirectory=$CONF_DIR
LimitNOFILE=65535

NoNewPrivileges=true
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
AmbientCapabilities=CAP_NET_BIND_SERVICE
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectKernelLogs=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictRealtime=true
RestrictNamespaces=true
LockPersonality=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK
ReadWritePaths=$CONF_DIR

[Install]
WantedBy=multi-user.target
EOF
}

write_config() {
    local listen="127.0.0.1:$PANEL_PORT"
    [ "$PANEL_PUBLIC" -eq 0 ] || listen="0.0.0.0:$PANEL_PORT"
    {
        printf 'mode: srv\nauth:\n  provider: %s\n' "$PROVIDER"
        [ -z "$ROOM" ] || printf 'room:\n  id: "%s"\n' "$ROOM"
        printf 'net:\n  transport: %s\n  dns: "%s"\n' "$TRANSPORT" "$DNS"
        printf 'vp8:\n  fps: %s\n  batch_size: 64\n' "$FPS"
        if [ "$PANEL_ENABLED" -eq 1 ]; then
            printf 'panel:\n  listen: "%s"\n  path: "%s"\n  password: "%s"\n  store: panel.json\n' \
                "$listen" "$PANEL_PATH" "$PANEL_PASS"
            [ -z "$CERT" ] || printf '  cert: panel-cert.pem\n  key: panel-key.pem\n'
            [ -z "$PUBLIC_URL" ] || printf '  public_url: "%s"\n' "$PUBLIC_URL"
        else
            printf 'crypto:\n  key: "%s"\n' "$(random_hex 32)"
        fi
    } > "$CONF"
}

# healthy - служба 10 с жива без перезапусков, и панель, если она есть,
# отвечает по своему пути.
healthy() {
    local before now
    before=$(systemctl show -p NRestarts --value "$NAME")
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        systemctl is-active --quiet "$NAME" || { warn "$NAME остановился"; return 1; }
        now=$(systemctl show -p NRestarts --value "$NAME")
        [ "$now" = "$before" ] || { warn "$NAME перезапускался ($before -> $now)"; return 1; }
    done
    local path port scheme="http"
    path=$(conf_value path)
    port=$(conf_value listen | sed 's/.*://')
    [ -n "$path" ] || return 0
    [ -z "$(conf_value cert)" ] || scheme="https"
    command -v curl >/dev/null 2>&1 || return 0
    curl -fsSk -o /dev/null --max-time 5 "$scheme://127.0.0.1:$port/$path/" ||
        { warn "панель не отвечает на $port"; return 1; }
}

summary() {
    local path port listen
    path=$(conf_value path)
    [ -n "$path" ] || { say "панель выключена; конфиг: $CONF"; return; }
    listen=$(conf_value listen)
    port=${listen##*:}
    step "Панель"
    if [ "${listen%:*}" = "127.0.0.1" ]; then
        say "панель слушает только loopback; откройте SSH-туннель со своего компьютера:"
        say "  ssh -L $port:127.0.0.1:$port root@<адрес сервера>"
        say "и зайдите на http://127.0.0.1:$port/$path/"
    else
        say "https://<адрес сервера>:$port/$path/"
    fi
    say "пароль: в $CONF (panel.password)"
    [ -z "${NEW_PASS:-}" ] || say "пароль: $NEW_PASS"
}

# ── Удаление ─────────────────────────────────────────────────────────────

if [ "$MODE" = "uninstall" ]; then
    step "Удаление службы"
    systemctl disable --now "$NAME" 2>/dev/null || true
    rm -f "$UNIT"
    systemctl daemon-reload
    say "данные оставлены в $CONF_DIR; удалить полностью: rm -rf $CONF_DIR $BIN"
    exit 0
fi

[ -f "$SRC_DIR/olcrtc" ] || die "рядом со скриптом нет файла olcrtc (соберите: scripts/build-server.sh)"

# ── Обновление ───────────────────────────────────────────────────────────

if [ "$MODE" = "update" ]; then
    [ -f "$CONF" ] || die "сервер не установлен: нет $CONF"
    step "Резервная копия"
    BACKUP="$(mktemp -d /var/backups/olcrtc-update.XXXXXX)"
    chmod 700 "$BACKUP"
    tar -C / -czf "$BACKUP/pre-update.tgz" "${BIN#/}" "${CONF_DIR#/}" "${UNIT#/}"
    say "сохранено: $BACKUP/pre-update.tgz"
    rollback() {
        trap - ERR
        warn "обновление не прошло; возвращаю бинарь, конфиг и юнит из $BACKUP"
        systemctl stop "$NAME" || true
        tar -C / -xzf "$BACKUP/pre-update.tgz"
        systemctl daemon-reload
        systemctl restart "$NAME" || true
        exit 1
    }
    trap rollback ERR
    step "Обновление"
    systemctl stop "$NAME"
    install -m 755 "$SRC_DIR/olcrtc" "$BIN"
    write_unit
    systemctl daemon-reload
    systemctl start "$NAME"
    healthy || rollback
    trap - ERR
    say "обновлено: $(sha256sum "$BIN" | cut -c1-16)"
    ls -1dt /var/backups/olcrtc-update.* 2>/dev/null | tail -n +4 | xargs -r rm -rf --
    exit 0
fi

# ── Установка ────────────────────────────────────────────────────────────

step "Пользователь и файлы"
id -u "$NAME" >/dev/null 2>&1 ||
    useradd --system --no-create-home --home-dir "$CONF_DIR" --shell /usr/sbin/nologin "$NAME"
install -d -m 700 -o "$NAME" -g "$NAME" "$CONF_DIR"
install -m 755 "$SRC_DIR/olcrtc" "$BIN"
say "$BIN"

if [ -f "$CONF" ]; then
    say "конфиг уже есть, не трогаю: $CONF"
else
    if [ "$PANEL_PUBLIC" -eq 1 ] && { [ -z "$CERT" ] || [ -z "$KEY" ]; }; then
        die "публичной панели нужны --cert и --key"
    fi
    [ -n "$PANEL_PATH" ] || PANEL_PATH="$(random_hex 12)"
    if [ -z "$PANEL_PASS" ]; then
        PANEL_PASS="$(random_hex 12)"
        NEW_PASS="$PANEL_PASS"
    fi
    [[ "$PANEL_PATH" =~ ^[A-Za-z0-9_-]{8,}$ ]] || die "--panel-path: от 8 букв, цифр, - или _"
    [ "${#PANEL_PASS}" -ge 12 ] || die "--panel-password: от 12 символов"
    [[ "$PANEL_PASS$ROOM$PUBLIC_URL" != *'"'* ]] || die "кавычки в значениях не поддерживаются"
    ( umask 077; write_config )
    say "конфиг: $CONF"
fi
# Служба работает от $NAME и не прочтёт ключ, доступный только root: панель
# получает копии. После продления сертификата запустите скрипт с --cert и
# --key снова.
if [ -n "$CERT" ]; then
    { [ -r "$CERT" ] && [ -r "$KEY" ]; } || die "нет файла $CERT или $KEY"
    install -m 600 "$CERT" "$CONF_DIR/panel-cert.pem"
    install -m 600 "$KEY" "$CONF_DIR/panel-key.pem"
    say "сертификат панели скопирован в $CONF_DIR"
fi
chown -R "$NAME:$NAME" "$CONF_DIR"
chmod 600 "$CONF"

step "Служба"
write_unit
systemctl daemon-reload
systemctl enable "$NAME" >/dev/null 2>&1
systemctl restart "$NAME"
healthy || die "служба не поднялась: journalctl -u $NAME -n 50"
say "$NAME работает"

if [ "$PANEL_PUBLIC" -eq 1 ]; then
    open_port "$PANEL_PORT"
    say "порт $PANEL_PORT открыт"
fi

summary
