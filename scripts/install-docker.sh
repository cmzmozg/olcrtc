#!/usr/bin/env bash
# // ai-generated: interactive Docker installer, certificate discovery and renewal.
set -Eeuo pipefail
umask 077

DIR=/opt/olcrtc-docker
IMAGE=ghcr.io/cmzmozg/olcrtc:latest
CERTBOT_IMAGE=certbot/certbot:v5.5.0
ACTION=menu
YES=0
PULL=1
HOST=
PUBLIC_IP=
CERT=
KEY=
PORT=8884
WEBROOT=
EMAIL=
MANAGED=0
SYSTEMD_DIR=${OLCRTC_SYSTEMD_DIR:-/etc/systemd/system}

# // ai-generated: fail reports an installer error.
fail() { printf 'Ошибка: %s\n' "$*" >&2; exit 1; }

# // ai-generated: ask reads from the terminal, including a curl pipe.
ask() {
    local answer
    if (( YES )); then printf '%s' "$2"; return; fi
    read -r -p "$1 [$2]: " answer </dev/tty
    printf '%s' "${answer:-$2}"
}

# // ai-generated: usage documents installer options.
usage() {
    cat <<'EOF'
Установка: sudo bash install-docker.sh
  --action install|update|status|logs|stop|uninstall|renew
  --dir /opt/olcrtc-docker    каталог установки и данных
  --image IMAGE              образ сервера
  --host HOST                домен или IPv4 панели (обычно определяется сам)
  --ip IPV4                  публичный IPv4 VPS (обычно определяется сам)
  --port PORT                HTTPS-порт панели (8884)
  --cert FILE --key FILE     существующий сертификат и ключ
  --webroot DIR              webroot для ACME, если порт 80 занят
  --email EMAIL              адрес аккаунта Let's Encrypt
  --yes                     принять значения по умолчанию и условия Let's Encrypt
  --skip-pull                использовать уже загруженный образ
EOF
}

# // ai-generated: parse accepts explicit settings without shell evaluation.
parse() {
    while (( $# )); do
        case "$1" in
            --action) ACTION=${2:?}; shift 2 ;;
            --dir) DIR=${2:?}; shift 2 ;;
            --image) IMAGE=${2:?}; shift 2 ;;
            --host) HOST=${2:?}; shift 2 ;;
            --ip) PUBLIC_IP=${2:?}; shift 2 ;;
            --port) PORT=${2:?}; shift 2 ;;
            --cert) CERT=${2:?}; shift 2 ;;
            --key) KEY=${2:?}; shift 2 ;;
            --webroot) WEBROOT=${2:?}; shift 2 ;;
            --email) EMAIL=${2:?}; shift 2 ;;
            --yes) YES=1; shift ;;
            --skip-pull) PULL=0; shift ;;
            -h|--help) usage; exit 0 ;;
            *) fail "неизвестный аргумент: $1" ;;
        esac
    done
    [[ $DIR =~ ^/[a-zA-Z0-9/_-]+$ && $DIR != / ]] || fail 'недопустимый каталог установки'
    [[ $PORT =~ ^[0-9]{1,5}$ ]] || fail 'недопустимый порт'
    (( 10#$PORT > 0 && 10#$PORT < 65536 )) || fail 'недопустимый порт'
    PORT=$((10#$PORT))
    [[ $IMAGE =~ ^[a-zA-Z0-9][a-zA-Z0-9._/@:-]+$ ]] || fail 'недопустимое имя образа'
}

# // ai-generated: compose scopes every Docker operation to this installation.
compose() { docker compose --project-directory "$DIR" -f "$DIR/compose.yaml" "$@"; }

# // ai-generated: require_tools checks host tools and optionally installs Docker.
require_tools() {
    [[ $(uname -s) == Linux && $EUID == 0 ]] || fail 'нужен Linux и запуск от root'
    local tool
    for tool in curl openssl getent find systemctl ss; do
        command -v "$tool" >/dev/null || fail "установите $tool"
    done
    systemctl show-environment >/dev/null || fail 'нужен работающий systemd для продления TLS'
    if ! command -v docker >/dev/null; then
        [[ $(ask 'Установить Docker с get.docker.com? (y/n)' y) == y ]] || exit 0
        local setup
        setup=$(mktemp)
        curl -fsSL https://get.docker.com -o "$setup"
        sh "$setup"
        rm -f "$setup"
    fi
    docker info >/dev/null || fail 'Docker daemon недоступен'
    docker compose version >/dev/null || fail 'установите Docker Compose plugin'
}

# // ai-generated: ipv4 validates an address before it is used in DNS or ACME.
ipv4() {
    local a b c d
    [[ $1 =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || return 1
    IFS=. read -r a b c d <<<"$1"
    (( 10#$a < 256 && 10#$b < 256 && 10#$c < 256 && 10#$d < 256 ))
}

# // ai-generated: resolves_here verifies DNS A records against the public IPv4.
resolves_here() {
    if ipv4 "$1"; then [[ $1 == "$PUBLIC_IP" ]]; return; fi
    getent ahostsv4 "$1" | awk '{print $1}' | sort -u | grep -Fxq "$PUBLIC_IP"
}

# // ai-generated: certificate_host selects a certificate SAN that resolves here.
certificate_host() {
    local candidate
    while read -r candidate; do
        [[ -n $candidate && $candidate != \*.* ]] || continue
        if resolves_here "$candidate"; then printf '%s' "$candidate"; return 0; fi
    done < <(openssl x509 -in "$1" -noout -ext subjectAltName 2>/dev/null |
        tr ',' '\n' | sed -nE 's/^[[:space:]]*(DNS:|IP Address:)([^[:space:]]+).*$/\2/p')
    return 1
}

# // ai-generated: discover_certificate finds a usable existing VPS certificate.
discover_certificate() {
    local file key candidate base
    while IFS= read -r file; do
        openssl x509 -in "$file" -checkend 3600 -noout >/dev/null 2>&1 || continue
        candidate=$HOST
        if [[ -z $candidate ]]; then candidate=$(certificate_host "$file") || continue; fi
        base=$(dirname "$file")
        for key in "$base/privkey.pem" "$base/key.pem" "$base/private.key" "${file%.*}.key"; do
            [[ -f $key && $key != "$file" ]] || continue
            if certificate_matches "$file" "$key" "$candidate"; then
                CERT=$file KEY=$key HOST=$candidate
                return 0
            fi
        done
    done < <(find /etc /opt /var/lib/caddy /root/.acme.sh \
        -maxdepth 7 \( -name '*.pem' -o -name '*.crt' -o -name '*.cer' \) \
        -print 2>/dev/null | sort)
    return 1
}

# // ai-generated: certificate_matches checks validity, hostname and private key.
certificate_matches() {
    local cert=$1 key=$2 host=$3 cert_pub key_pub
    openssl x509 -in "$cert" -checkend 3600 -noout >/dev/null 2>&1 || return 1
    if ipv4 "$host"; then
        openssl x509 -in "$cert" -checkip "$host" -noout 2>/dev/null | grep -Fq 'does match certificate' || return 1
    else
        openssl x509 -in "$cert" -checkhost "$host" -noout 2>/dev/null | grep -Fq 'does match certificate' || return 1
    fi
    cert_pub=$(openssl x509 -in "$cert" -pubkey -noout) || return 1
    key_pub=$(openssl pkey -in "$key" -passin pass: -pubout 2>/dev/null) || return 1
    [[ $cert_pub == "$key_pub" ]]
}

# // ai-generated: detect_host determines the public address and reuses VPS TLS.
detect_host() {
    if [[ -z $PUBLIC_IP ]]; then
        PUBLIC_IP=$(curl -4fsS --connect-timeout 5 --max-time 10 https://api.ipify.org ||
            curl -4fsS --connect-timeout 5 --max-time 10 https://ipv4.icanhazip.com) || fail 'не удалось определить IP: задайте --ip'
    fi
    ipv4 "$PUBLIC_IP" || fail 'некорректный публичный IPv4'
    printf 'Публичный IP: %s\n' "$PUBLIC_IP"
    if [[ -n $CERT || -n $KEY ]]; then
        [[ -f $CERT && -f $KEY ]] || fail 'нужны существующие --cert и --key'
        if [[ -z $HOST ]]; then HOST=$(certificate_host "$CERT") || fail 'имя сертификата не резолвится на VPS'; fi
    elif discover_certificate; then
        printf 'Найден сертификат: %s (%s)\n' "$CERT" "$HOST"
    fi
    HOST=$(ask 'Домен или IPv4 панели' "${HOST:-$PUBLIC_IP}")
    [[ $HOST =~ ^[a-zA-Z0-9][a-zA-Z0-9.-]*$ && ${#HOST} -le 253 ]] || fail 'недопустимое имя хоста'
    resolves_here "$HOST" || fail "$HOST не резолвится на $PUBLIC_IP"
    if [[ -n $CERT ]]; then
        certificate_matches "$CERT" "$KEY" "$HOST" || fail 'сертификат или ключ не подходит выбранному адресу'
    fi
}

# // ai-generated: certbot runs a current ACME client without changing host certbot.
certbot() {
    local mounts=()
    if [[ -n $WEBROOT ]]; then mounts=(-v "$WEBROOT:$WEBROOT"); fi
    docker run --rm --network host -v /etc/letsencrypt:/etc/letsencrypt \
        -v /var/lib/letsencrypt:/var/lib/letsencrypt "${mounts[@]}" "$CERTBOT_IMAGE" "$@"
}

# // ai-generated: issue_certificate obtains domain or short-lived IP TLS.
issue_certificate() {
    local identifier=(-d "$HOST") challenge=(--standalone) account=(--register-unsafely-without-email)
    if ipv4 "$HOST"; then identifier=(--ip-address "$HOST" --preferred-profile shortlived); fi
    if [[ -z $WEBROOT ]] && ss -Hltn 'sport = :80' | grep -q .; then
        WEBROOT=$(ask 'Порт 80 занят. Укажите webroot для /.well-known/acme-challenge' '')
        [[ -n $WEBROOT ]] || fail 'для выпуска нужен свободный порт 80 или --webroot'
    fi
    if [[ -n $WEBROOT ]]; then
        [[ $WEBROOT =~ ^/[a-zA-Z0-9/_.-]+$ && -d $WEBROOT ]] || fail 'недопустимый webroot'
        challenge=(--webroot --webroot-path "$WEBROOT")
    fi
    EMAIL=$(ask "Email аккаунта Let's Encrypt (необязательно)" "$EMAIL")
    if [[ -n $EMAIL ]]; then account=(--email "$EMAIL"); fi
    printf 'Выпуск Let\x27s Encrypt для %s. Входящий TCP/80 должен быть доступен из интернета.\n' "$HOST"
    [[ $(ask 'Принять условия https://letsencrypt.org/repository/ и выпустить сертификат? (y/n)' y) == y ]] || exit 0
    open_port 80
    certbot certonly --non-interactive --agree-tos --cert-name "olcrtc-docker-$HOST" \
        "${account[@]}" "${challenge[@]}" "${identifier[@]}"
    CERT="/etc/letsencrypt/live/olcrtc-docker-$HOST/fullchain.pem"
    KEY="/etc/letsencrypt/live/olcrtc-docker-$HOST/privkey.pem"
    MANAGED=1
}

# // ai-generated: open_port permits only the required port in an active host firewall.
open_port() {
    if command -v ufw >/dev/null && ufw status | grep -q 'Status: active'; then
        ufw allow "$1/tcp"
    elif command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
        firewall-cmd --permanent --add-port="$1/tcp"
        firewall-cmd --add-port="$1/tcp"
    fi
}

# // ai-generated: sync_tls copies only validated changed certificates atomically.
sync_tls() {
    certificate_matches "$CERT" "$KEY" "$HOST" || fail 'сертификат истёк, не соответствует хосту или ключу'
    if cmp -s "$CERT" "$DIR/data/panel.crt" && cmp -s "$KEY" "$DIR/data/panel.key"; then return; fi
    install -m 600 -o 10001 -g 10001 "$CERT" "$DIR/data/panel.crt.new"
    install -m 600 -o 10001 -g 10001 "$KEY" "$DIR/data/panel.key.new"
    mv -f "$DIR/data/panel.crt.new" "$DIR/data/panel.crt"
    mv -f "$DIR/data/panel.key.new" "$DIR/data/panel.key"
    if [[ -f $DIR/compose.yaml ]]; then compose restart server; fi
}

# // ai-generated: write_compose persists the selected image and runtime settings.
write_compose() {
    cat >"$DIR/compose.yaml" <<EOF
# // ai-generated: installed server runtime configuration.
name: olcrtc-docker
services:
  server:
    image: $IMAGE
    network_mode: host
    restart: unless-stopped
    user: "10001:10001"
    read_only: true
    cap_drop: [ALL]
    security_opt: [no-new-privileges:true]
    tmpfs: ["/tmp:rw,noexec,nosuid,size=16m"]
    volumes:
      - ./data:/etc/olcrtc
    stop_grace_period: 15s
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
EOF
}

# // ai-generated: write_config creates the public panel with random credentials.
write_config() {
    local path password provider transport
    path=$(openssl rand -hex 16)
    password=$(openssl rand -hex 24)
    provider=$(ask 'Провайдер новых комнат: vkcalls, jitsi, telemost, wbstream, salutejazz' vkcalls)
    case "$provider" in
        vkcalls|telemost|wbstream) transport=vp8channel ;;
        jitsi|salutejazz) transport=datachannel ;;
        *) fail 'неизвестный провайдер' ;;
    esac
    cat >"$DIR/data/server.yaml" <<EOF
# // ai-generated: HTTPS panel bootstrap; rooms and client keys live in panel.json.
mode: srv
auth:
  provider: $provider
net:
  transport: $transport
  dns: "1.1.1.1:53"
vp8:
  fps: 60
  batch_size: 64
  olt_disabled: true
panel:
  listen: "0.0.0.0:$PORT"
  path: "$path"
  password: "$password"
  store: panel.json
  cert: /etc/olcrtc/panel.crt
  key: /etc/olcrtc/panel.key
  public_url: "https://$HOST:$PORT"
EOF
    printf 'Панель: https://%s:%s/%s/\nПароль: %s\n' "$HOST" "$PORT" "$path" "$password" >"$DIR/access.txt"
    chown 10001:10001 "$DIR/data/server.yaml"
}

# // ai-generated: save_settings records TLS sources without changing their owners.
save_settings() {
    local name
    : >"$DIR/settings.env"
    for name in HOST CERT KEY WEBROOT MANAGED CERTBOT_IMAGE; do
        printf '%s=%q\n' "$name" "${!name}" >>"$DIR/settings.env"
    done
    [[ -f ${BASH_SOURCE[0]} ]] || fail 'сначала скачайте скрипт в файл'
    if [[ $(realpath "${BASH_SOURCE[0]}") != "$DIR/install-docker.sh" ]]; then
        install -m 700 "${BASH_SOURCE[0]}" "$DIR/install-docker.sh"
    fi
}

# // ai-generated: install_timer renews managed TLS and follows existing certificate renewals.
install_timer() {
    cat >"$SYSTEMD_DIR/olcrtc-docker-tls.service" <<EOF
# // ai-generated: synchronize the panel TLS certificate.
[Unit]
Description=olcrtc panel TLS renewal
After=docker.service network-online.target
[Service]
Type=oneshot
ExecStart=/bin/bash $DIR/install-docker.sh --dir $DIR --action renew --yes
EOF
    cat >"$SYSTEMD_DIR/olcrtc-docker-tls.timer" <<'EOF'
# // ai-generated: poll TLS sources and renew short-lived IP certificates.
[Unit]
Description=olcrtc panel TLS renewal timer
[Timer]
OnBootSec=5m
OnUnitActiveSec=1h
RandomizedDelaySec=5m
Persistent=true
[Install]
WantedBy=timers.target
EOF
    systemctl daemon-reload
    systemctl enable --now olcrtc-docker-tls.timer
}

# // ai-generated: wait_panel verifies HTTPS readiness without exposing credentials.
wait_panel() {
    local i
    for (( i=0; i<30; i++ )); do
        if curl -kfsS --max-time 2 "https://127.0.0.1:$PORT/$(sed -n 's/^  path: "\(.*\)"/\1/p' "$DIR/data/server.yaml")/" >/dev/null 2>&1; then return; fi
        sleep 1
    done
    compose logs --tail 30
    fail 'панель не отвечает по HTTPS'
}

# // ai-generated: install_server bootstraps persistent state and starts the image.
install_server() {
    [[ ! -e $DIR/compose.yaml && ! -e $DIR/data/server.yaml && ! -e $DIR/settings.env ]] || fail 'установка уже существует: выберите update'
    [[ -z $(docker ps -aq --filter "label=com.docker.compose.project=${COMPOSE_PROJECT_NAME:-olcrtc-docker}") ]] || fail 'контейнер другой установки уже существует'
    ss -Hltn "sport = :$PORT" | grep -q . && fail "порт $PORT уже занят"
    detect_host
    [[ -n $CERT ]] || issue_certificate
    install -d -m 700 -o root -g root "$DIR"
    install -d -m 700 -o 10001 -g 10001 "$DIR/data"
    sync_tls
    write_config
    write_compose
    save_settings
    if (( PULL )); then compose pull; fi
    compose up -d
    wait_panel
    install_timer
    open_port "$PORT"
    cat "$DIR/access.txt"
    printf 'Данные: %s/data\nВнешний firewall провайдера должен пропускать TCP/%s.\n' "$DIR" "$PORT"
    printf 'В панели добавьте комнату и клиента, затем скопируйте ссылку или подписку.\n'
}

# // ai-generated: update_server pulls first and restores the old image on failure.
update_server() {
    [[ -f $DIR/compose.yaml ]] || fail 'установка не найдена'
    local old
    old=$(docker inspect --format '{{.Image}}' "$(compose ps -aq server)")
    PORT=$(sed -n 's/^  listen: "0.0.0.0:\([0-9]*\)"/\1/p' "$DIR/data/server.yaml")
    cp -p "$DIR/compose.yaml" "$DIR/compose.yaml.previous"
    write_compose
    if (( PULL )) && ! compose pull; then
        mv -f "$DIR/compose.yaml.previous" "$DIR/compose.yaml"
        fail 'не удалось загрузить образ; работающий контейнер сохранён'
    fi
    if compose up -d && (wait_panel); then
        printf 'Обновление завершено. Конфиг, комнаты и клиенты сохранены.\n'
        return
    fi
    mv -f "$DIR/compose.yaml.previous" "$DIR/compose.yaml"
    IMAGE=$old
    write_compose
    compose up -d
    fail 'обновление не прошло проверку; восстановлен предыдущий образ'
}

# // ai-generated: main dispatches the interactive installation menu.
main() {
    parse "$@"
    require_tools
    if [[ $ACTION == menu ]]; then
        printf '1) Установить\n2) Обновить\n3) Статус и ссылка\n4) Логи\n5) Остановить\n6) Удалить контейнер (данные остаются)\n0) Выход\n'
        case "$(ask 'Действие' 1)" in
            1) ACTION=install ;; 2) ACTION=update ;; 3) ACTION=status ;;
            4) ACTION=logs ;; 5) ACTION=stop ;; 6) ACTION=uninstall ;; 0) exit 0 ;;
            *) fail 'неизвестное действие' ;;
        esac
    fi
    case "$ACTION" in
        install) install_server ;;
        update) update_server ;;
        status) compose ps; cat "$DIR/access.txt" ;;
        logs) compose logs --tail 100 -f ;;
        stop) compose stop ;;
        uninstall)
            if (( ! YES )); then
                [[ $(ask 'Удалить контейнер, сохранив данные? (y/n)' n) == y ]] || exit 0
            fi
            compose down
            systemctl disable --now olcrtc-docker-tls.timer
            printf 'Данные сохранены в %s\n' "$DIR"
            ;;
        renew)
            [[ -f $DIR/settings.env ]] || fail 'настройки TLS не найдены'
            # shellcheck source=/dev/null
            source "$DIR/settings.env"
            if (( MANAGED )); then certbot renew --quiet --cert-name "olcrtc-docker-$HOST"; fi
            sync_tls
            ;;
        *) fail 'неизвестное действие' ;;
    esac
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
