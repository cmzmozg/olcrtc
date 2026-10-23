#!/bin/bash
#
# olcrtc: проверка install-server.sh в контейнере с systemd.
#
# ai-generated: the whole file (panel installer).
#
# Собирает архив (build-server.sh), ставит его в чистый Ubuntu с systemd и
# проходит установку, повторный запуск, вход в панель, обновление, откат
# сломанного обновления и удаление. Нужен docker; хост не меняется.
#
#   scripts/test-install-server.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="olcrtc-install-test"
NAME="olcrtc-install-test-$$"
WORK="$(mktemp -d)"
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true; rm -rf "$WORK"' EXIT

ok()   { printf '  ok   %s\n' "$*"; }
fail() { printf '  FAIL %s\n' "$*" >&2; exit 1; }
inside() { docker exec "$NAME" bash -c "$*"; }

docker build -q -t "$IMAGE" - >/dev/null <<'EOF'
FROM ubuntu:26.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    systemd systemd-sysv curl ca-certificates && rm -rf /var/lib/apt/lists/*
CMD ["/sbin/init"]
EOF

OUT="$WORK" ARCHES=amd64 "$ROOT/scripts/build-server.sh" >/dev/null
ARCHIVE="$(ls "$WORK"/olcrtc-server-*-linux-amd64.tar.gz)"

docker run -d --name "$NAME" --privileged --cgroupns=host \
    -v /sys/fs/cgroup:/sys/fs/cgroup:rw --tmpfs /run --tmpfs /run/lock "$IMAGE" >/dev/null
for _ in $(seq 30); do
    inside 'systemctl is-system-running 2>/dev/null | grep -qE "running|degraded"' && break
    sleep 1
done
docker cp "$ARCHIVE" "$NAME:/root/olcrtc.tar.gz"
inside 'cd /root && tar xzf olcrtc.tar.gz && cd olcrtc-server && sha256sum -c --quiet SHA256SUMS' && ok "архив и контрольные суммы"

out="$(inside 'cd /root/olcrtc-server && ./install-server.sh')" || fail "установка: $out"
inside 'systemctl is-active --quiet olcrtc' && ok "служба работает"
[ "$(inside 'stat -c %a:%U /etc/olcrtc/server.yaml')" = "600:olcrtc" ] && ok "конфиг 600, владелец olcrtc"
path="$(inside "sed -n 's/^  path: \"\\(.*\\)\"/\\1/p' /etc/olcrtc/server.yaml")"
pass="$(printf '%s\n' "$out" | sed -n 's/^  пароль: \([0-9a-f]\{24\}\)$/\1/p')"
[ -n "$path" ] && [ -n "$pass" ] || fail "нет пути или пароля в выводе"
ok "установщик выдал путь и пароль"

[ "$(inside 'curl -s -o /dev/null -w %{http_code} http://127.0.0.1:8884/')" = 404 ] && ok "корень отвечает 404"
[ "$(inside "curl -s -o /dev/null -w %{http_code} http://127.0.0.1:8884/$path/")" = 200 ] && ok "панель отвечает по пути"
login="curl -s -o /dev/null -w %{http_code} -c /tmp/jar -H 'X-Olcrtc-Panel: 1' -H 'Content-Type: application/json'"
[ "$(inside "$login -d '{\"password\":\"$pass\"}' http://127.0.0.1:8884/$path/api/login")" = 200 ] && ok "вход по выданному паролю"
inside "curl -s -b /tmp/jar -H 'X-Olcrtc-Panel: 1' -d '{\"name\":\"phone\"}' http://127.0.0.1:8884/$path/api/clients" | grep -q '"phone"' &&
    ok "клиент добавлен"

before="$(inside 'sha256sum /etc/olcrtc/server.yaml /etc/olcrtc/panel.json')"
inside 'cd /root/olcrtc-server && ./install-server.sh' >/dev/null || fail "повторная установка"
[ "$(inside 'sha256sum /etc/olcrtc/server.yaml /etc/olcrtc/panel.json')" = "$before" ] && ok "повтор не трогает конфиг и клиентов"

inside 'cd /root/olcrtc-server && ./install-server.sh --update' >/dev/null || fail "обновление"
inside 'systemctl is-active --quiet olcrtc' && ok "обновление прошло"

good="$(inside 'sha256sum /usr/local/bin/olcrtc')"
inside 'mkdir -p /root/broken && cp /root/olcrtc-server/install-server.sh /root/broken/ &&
    printf "#!/bin/sh\nexit 1\n" > /root/broken/olcrtc && chmod +x /root/broken/olcrtc'
if inside 'cd /root/broken && ./install-server.sh --update' >/dev/null 2>&1; then
    fail "сломанное обновление прошло"
fi
[ "$(inside 'sha256sum /usr/local/bin/olcrtc')" = "$good" ] && inside 'systemctl is-active --quiet olcrtc' &&
    ok "сломанное обновление откатилось"

inside 'cd /root/olcrtc-server && ./install-server.sh --uninstall' >/dev/null
inside '! systemctl is-active --quiet olcrtc && [ ! -e /etc/systemd/system/olcrtc.service ] && [ -f /etc/olcrtc/panel.json ]' &&
    ok "удаление снимает службу и оставляет данные"
echo "install-server.sh: все проверки прошли"
