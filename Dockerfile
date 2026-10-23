# syntax=docker/dockerfile:1
# // ai-generated: multi-stage, pure Go server image with the embedded panel.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
RUN --mount=type=cache,target=/go/pkg/mod go install github.com/magefile/mage@v1.17.2
COPY magefile.go ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY pkg/ ./pkg/
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH GOFLAGS=-buildvcs=false mage build && \
    cp build/olcrtc-$TARGETOS-$TARGETARCH /olcrtc

FROM --platform=$BUILDPLATFORM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 olcrtc && adduser -S -D -H -u 10001 -G olcrtc olcrtc && \
    mkdir /etc/olcrtc && chown olcrtc:olcrtc /etc/olcrtc

FROM alpine:3.23
COPY --from=runtime /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=runtime /etc/passwd /etc/group /etc/
COPY --from=runtime --chown=10001:10001 /etc/olcrtc /etc/olcrtc
COPY --from=build /olcrtc /usr/local/bin/olcrtc
COPY LICENSE NOTICE /usr/share/licenses/olcrtc/
COPY docs/upstream-license-wtfpl.txt /usr/share/licenses/olcrtc/
LABEL org.opencontainers.image.title="olcrtc" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/cmzmozg/olcrtc"
USER 10001:10001
WORKDIR /etc/olcrtc
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/olcrtc"]
CMD ["/etc/olcrtc/server.yaml"]
