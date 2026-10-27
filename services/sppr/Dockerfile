# syntax=docker/dockerfile:1

FROM golang:1.24.1-alpine3.21 AS builder
ENV GOWORK=off
WORKDIR /workspace/services/sppr
COPY shared/go.mod shared/go.sum /workspace/shared/
COPY services/sppr/go.mod services/sppr/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY shared /workspace/shared
COPY services/sppr ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -o /out/sppr ./cmd/server

FROM alpine:3.21
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 sppr
USER sppr
WORKDIR /app
COPY --from=builder /out/sppr /app/sppr
EXPOSE 8086
ENTRYPOINT ["/app/sppr"]
