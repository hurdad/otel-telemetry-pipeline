#!/usr/bin/env bash
set -euo pipefail

: "${NATS_URL:=nats://localhost:4222}"

nats --server "$NATS_URL" stream add OTEL_TELEMETRY \
  --defaults \
  --subjects "otel.traces,otel.metrics,otel.logs" \
  --retention limits \
  --max-age 24h \
  --storage file \
  --ack
