#!/usr/bin/env bash
# // ai-generated: container integration checks with a local TLS fixture, no live CA.
# shellcheck disable=SC2031 # Sourced installer defaults are intentionally isolated in a subshell.
set -Eeuo pipefail
[[ $EUID == 0 ]] || { echo 'Run as root' >&2; exit 1; }
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
IMAGE=${OLCRTC_TEST_IMAGE:-olcrtc:docker-test}
STAGE=$(mktemp -d /tmp/olcrtc-docker-test-XXXXXXXX)
INSTALL="$STAGE/install"
PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
project=${STAGE##*/}
export COMPOSE_PROJECT_NAME=${project,,}
BAD_IMAGE=olcrtc:bad-${project,,}

# // ai-generated: cleanup removes only the test project and its temporary files.
cleanup() {
    docker compose --project-directory "$INSTALL" -f "$INSTALL/compose.yaml" down >/dev/null 2>&1 || true
    docker image rm "$BAD_IMAGE" >/dev/null 2>&1 || true
    rm -rf -- "$STAGE"
}
trap cleanup EXIT
mkdir "$STAGE/bin" "$STAGE/systemd"
export OLCRTC_SYSTEMD_DIR="$STAGE/systemd"
printf '#!/bin/sh\nexit 0\n' >"$STAGE/bin/systemctl"
printf '#!/bin/sh\nexit 1\n' >"$STAGE/bin/ufw"
chmod +x "$STAGE/bin/"*
export PATH="$STAGE/bin:$PATH"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=localhost \
    -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
    -keyout "$STAGE/key.pem" -out "$STAGE/cert.pem" >/dev/null 2>&1

# // ai-generated: cover automatic SAN discovery and ACME flags without issuing a certificate.
(
    # shellcheck source=install-docker.sh
    source "$ROOT/scripts/install-docker.sh"
    PUBLIC_IP=127.0.0.1
    HOST=localhost
    # // ai-generated: find limits discovery to the test certificate pair.
    find() { printf '%s\n' "$STAGE/cert.pem"; }
    discover_certificate
    [[ $CERT == "$STAGE/cert.pem" && $KEY == "$STAGE/key.pem" ]]
    if certificate_matches "$CERT" "$KEY" wrong.invalid; then exit 1; fi
    # // ai-generated: open_port avoids host firewall mutations in ACME argument tests.
    open_port() { :; }
    # // ai-generated: ss models a free HTTP-01 port for the ACME test.
    ss() { :; }
    # // ai-generated: certbot captures requests instead of contacting a certificate authority.
    certbot() { printf '%s\n' "$@" >"$STAGE/acme.args"; }
    HOST=203.0.113.10 YES=1
    issue_certificate >/dev/null
    grep -Fxq -- --ip-address "$STAGE/acme.args"
    grep -Fxq shortlived "$STAGE/acme.args"
    [[ $MANAGED == 1 ]]
    HOST=example.invalid
    issue_certificate >/dev/null
    grep -Fxq -- -d "$STAGE/acme.args"
    if grep -Fxq -- --ip-address "$STAGE/acme.args"; then exit 1; fi
)

bash "$ROOT/scripts/install-docker.sh" --yes --action install --skip-pull --image "$IMAGE" \
    --dir "$INSTALL" --host 127.0.0.1 --ip 127.0.0.1 --port "$PORT" \
    --cert "$STAGE/cert.pem" --key "$STAGE/key.pem" >"$STAGE/install.log" 2>&1 || { cat "$STAGE/install.log"; exit 1; }

python3 - "$INSTALL" "$PORT" <<'PY'
# // ai-generated: verify TLS, the secret path, login and client persistence through the real API.
import http.cookiejar
import json
from pathlib import Path
import re
import ssl
import sys
import urllib.error
import urllib.request

root = Path(sys.argv[1])
config = (root / "data/server.yaml").read_text()
path = re.search(r'  path: "(.*)"', config)[1]
password = re.search(r'  password: "(.*)"', config)[1]
base = f"https://127.0.0.1:{sys.argv[2]}"
context = ssl.create_default_context(cafile=str(root / "data/panel.crt"))
jar = http.cookiejar.CookieJar()
client = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar), urllib.request.HTTPSHandler(context=context))


# // ai-generated: request asserts status while preserving the session cookie.
def request(endpoint, method="GET", data=None, status=200):
    body = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request(base + endpoint, body, {"Content-Type": "application/json", "X-Olcrtc-Panel": "1"}, method=method)
    try:
        response = client.open(req, timeout=5)
    except urllib.error.HTTPError as error:
        response = error
    assert response.code == status, (endpoint, response.code, status)
    return response.read()


request("/", status=404)
request("/api/state", status=404)
request(f"/{path}/api/state", status=401)
request(f"/{path}/api/metrics", status=401)
request(f"/{path}/api/audit", status=401)
assert b"app.js" in request(f"/{path}/")
assert b"renderMetrics" in request(f"/{path}/monitor.js")
request(f"/{path}/api/login", "POST", {"password": password})
assert all(cookie.secure for cookie in jar)
request(f"/{path}/api/clients", "POST", {"name": "docker-test"})
state = json.loads(request(f"/{path}/api/state"))
assert state["clients"][0]["name"] == "docker-test"
store = root / "data/panel.json"
assert store.stat().st_mode & 0o777 == 0o600
assert store.stat().st_uid == 10001
assert len(json.loads(store.read_text())["clients"][0]["key"]) == 64
# // ai-generated: monitor counters and journal survive the real container installation.
metrics = json.loads(request(f"/{path}/api/metrics"))
assert metrics["clients"] == 1 and metrics["clients_enabled"] == 1
assert metrics["host"]["mem_total"] > 0 and metrics["host"]["disk_total"] > 0
journal = json.loads(request(f"/{path}/api/audit"))["entries"]
assert [entry["action"] for entry in journal] == ["client-create", "login"]
assert journal[0]["target"] == state["clients"][0]["id"]
audit = root / "data/panel.json.audit.json"
assert audit.stat().st_mode & 0o777 == 0o600 and audit.stat().st_uid == 10001
assert password not in audit.read_text()
assert json.loads(store.read_text())["clients"][0]["key"] not in audit.read_text()
PY

config_hash=$(sha256sum "$INSTALL/data/server.yaml")
store_hash=$(sha256sum "$INSTALL/data/panel.json")
audit_hash=$(sha256sum "$INSTALL/data/panel.json.audit.json")
bash "$ROOT/scripts/install-docker.sh" --yes --action update --skip-pull --image "$IMAGE" --dir "$INSTALL" >"$STAGE/update.log" 2>&1
[[ $(sha256sum "$INSTALL/data/server.yaml") == "$config_hash" ]]
[[ $(sha256sum "$INSTALL/data/panel.json") == "$store_hash" ]]
[[ $(sha256sum "$INSTALL/data/panel.json.audit.json") == "$audit_hash" ]]

# // ai-generated: exercise failed update rollback with an image that cannot serve HTTPS.
docker image inspect "$IMAGE" --format '{{.Config.User}}' | grep -Fx 10001:10001
docker build -t "$BAD_IMAGE" - >"$STAGE/bad-image.log" 2>&1 <<EOF
FROM $IMAGE
ENTRYPOINT ["/bin/false"]
EOF
if bash "$ROOT/scripts/install-docker.sh" --yes --action update --skip-pull --image "$BAD_IMAGE" --dir "$INSTALL" >"$STAGE/rollback.log" 2>&1; then
    echo 'Failed image update was accepted' >&2
    exit 1
fi
[[ $(sha256sum "$INSTALL/data/panel.json") == "$store_hash" ]]

# // ai-generated: replace the source TLS pair, run renewal sync and verify the live certificate.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=localhost \
    -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
    -keyout "$STAGE/key.pem" -out "$STAGE/cert.pem" >/dev/null 2>&1
bash "$ROOT/scripts/install-docker.sh" --yes --action renew --dir "$INSTALL" >"$STAGE/renew.log" 2>&1
cmp "$STAGE/cert.pem" "$INSTALL/data/panel.crt"
cmp "$STAGE/key.pem" "$INSTALL/data/panel.key"
for _ in {1..20}; do
    if curl --cacert "$STAGE/cert.pem" -s "https://127.0.0.1:$PORT/" -o /dev/null; then break; fi
    sleep 1
done
curl --cacert "$STAGE/cert.pem" -s "https://127.0.0.1:$PORT/" -o /dev/null
# // ai-generated: certificate sync must respect an explicitly stopped server.
bash "$ROOT/scripts/install-docker.sh" --yes --action stop --dir "$INSTALL" >"$STAGE/stop.log" 2>&1
openssl x509 -in "$STAGE/cert.pem" -signkey "$STAGE/key.pem" -set_serial 123 -days 2 -out "$STAGE/rotated.pem"
mv "$STAGE/rotated.pem" "$STAGE/cert.pem"
bash "$ROOT/scripts/install-docker.sh" --yes --action renew --dir "$INSTALL" >"$STAGE/stopped-renew.log" 2>&1
[[ -z $(docker compose --project-directory "$INSTALL" -f "$INSTALL/compose.yaml" ps -q --status running server) ]]
bash "$ROOT/scripts/install-docker.sh" --yes --action uninstall --dir "$INSTALL" >"$STAGE/uninstall.log" 2>&1
[[ $(sha256sum "$INSTALL/data/panel.json") == "$store_hash" ]]
echo 'Docker HTTPS, persistent clients, update rollback and TLS sync: OK'
