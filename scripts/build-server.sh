#!/bin/bash
#
# olcrtc: сборка сервера для Linux и архив для установки.
#
# ai-generated: the whole file (panel installer).
#
# Собирает olcrtc (чистый Go, без CGO) и кладёт его в архив вместе с
# install-server.sh, ревизией и контрольными суммами:
#
#   scripts/build-server.sh                # amd64 и arm64
#   ARCHES=amd64 scripts/build-server.sh   # только amd64
#
# На сервере:
#
#   tar xzf olcrtc-server-<rev>-linux-amd64.tar.gz && cd olcrtc-server
#   sha256sum -c SHA256SUMS && sudo ./install-server.sh

set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${OUT:-$ROOT/build}"
ARCHES="${ARCHES:-amd64 arm64}"

REV="$(git -C "$ROOT" -c safe.directory='*' rev-parse --short=8 HEAD 2>/dev/null || echo dev)"
if [ -n "$(git -C "$ROOT" -c safe.directory='*' status --porcelain -uno 2>/dev/null)" ]; then
    REV="$REV-dirty"
fi

mkdir -p "$OUT"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

for arch in $ARCHES; do
    dir="$STAGE/$arch/olcrtc-server"
    mkdir -p "$dir"
    echo "building olcrtc $REV linux/$arch"
    (cd "$ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH="$arch" GOFLAGS=-buildvcs=false \
        go build -trimpath -ldflags "-s -w" -o "$dir/olcrtc" ./cmd/olcrtc)
    install -m 755 "$ROOT/scripts/install-server.sh" "$dir/install-server.sh"
    echo "$REV" > "$dir/REVISION"
    (cd "$dir" && sha256sum olcrtc install-server.sh REVISION > SHA256SUMS)
    archive="$OUT/olcrtc-server-$REV-linux-$arch.tar.gz"
    tar -C "$STAGE/$arch" -czf "$archive" olcrtc-server
    echo "  $archive"
    echo "  sha256 $(sha256sum "$archive" | cut -d' ' -f1)"
done
