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

FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libgomp1 wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home sppr \
    && mkdir -p /models/registry && chown sppr:sppr /models/registry
COPY services/sppr/ml/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY services/sppr/ml/sppr_ml /opt/sppr-ml/sppr_ml
# Outside /models: Compose mounts that read-only, so a volume cannot nest in it.
RUN mkdir -p /var/lib/sppr/models && chown sppr:sppr /var/lib/sppr/models
ENV PYTHONPATH=/opt/sppr-ml PYTHONDONTWRITEBYTECODE=1 OMP_NUM_THREADS=2
USER sppr
WORKDIR /app
COPY --from=builder /out/sppr /app/sppr
EXPOSE 8086
ENTRYPOINT ["/app/sppr"]
