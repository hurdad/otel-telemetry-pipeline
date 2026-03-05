# otel-ingest-pipeline

## Architecture

```text
OpenTelemetry Collector
        ↓ OTLP gRPC
otel-otlp-gateway
        ↓
NATS JetStream
        ↓
jetstream-clickhouse-loader
        ↓
ClickHouse
```

The repository contains two C++20 services:
- `otel-otlp-gateway`: OTLP gRPC ingest service implementing traces/metrics/logs collector APIs.
- `jetstream-clickhouse-loader`: JetStream consumer that decodes OTLP protobuf payloads and batches inserts into ClickHouse.

## Quick start (Docker Compose)

If you want to run the full pipeline locally with minimal setup, use Docker Compose:

```bash
docker compose up --build
```

Then send OTLP data to the collector:
- gRPC: `localhost:4317`
- HTTP: `localhost:4318`

The compose stack starts Collector → Gateway → NATS JetStream → Loader → ClickHouse, and initializes both the JetStream stream and ClickHouse schema automatically.

## Repository layout

- `services/otlp-gateway`: OTLP ingest gateway implementation.
- `services/jetstream-clickhouse-loader`: JetStream-to-ClickHouse loader implementation.
- `libs/`: shared first-party libraries (serialization, telemetry, common utilities).
- `configs/`: example runtime configuration for gateway, loader, and collector.
- `scripts/`: JetStream stream bootstrap and ClickHouse schema SQL.
- `charts/otel-telemetry-pipeline`: Helm chart for Kubernetes deployment.

## Build instructions

```bash
git submodule update --init --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Binaries:
- `build/services/otlp-gateway/otel-otlp-gateway`
- `build/services/jetstream-clickhouse-loader/jetstream-clickhouse-loader`

## Testing

Configure with tests enabled, build, and run with CTest:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DOTEL_PIPELINE_BUILD_TESTS=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

This wires unit-test executables for first-party libraries under `libs/` (and optional `telemetry`) and excludes third-party test suites.

## Running locally

1. Start NATS with JetStream and ClickHouse.
2. Apply stream and table schema scripts.
3. Run gateway and loader binaries.
4. Configure OpenTelemetry Collector to export OTLP to `otel-otlp-gateway:4317`.

Configuration examples are in `configs/gateway.yaml` and `configs/loader.yaml`.

### Telemetry env vars

- `OTEL_EXPORTER_OTLP_ENDPOINT`
- `OTEL_SERVICE_NAME`
- `OTEL_RESOURCE_ATTRIBUTES`

Both gateway and loader use `libs/telemetry` self-instrumentation built on `opentelemetry-cpp` SDK:
- traces: internal operation spans (gRPC handling, JetStream operations, ClickHouse writes)
- metrics: `self_telemetry.spans_total` counter and `self_telemetry.span_duration_ms` histogram
- logs: structured SDK logs emitted by the telemetry runtime

When `OTEL_EXPORTER_OTLP_ENDPOINT` is empty, self-telemetry exporters are disabled.

## JetStream setup

```bash
./scripts/create_jetstream_stream.sh
```

Creates stream `OTEL_TELEMETRY` with subjects:
- `otel.traces`
- `otel.metrics`
- `otel.logs`

Retention is configured with max age 24h.

## ClickHouse schema

```bash
clickhouse-client --multiquery < scripts/clickhouse_schema.sql
```

Tables:
- `otel_traces`
- `otel_metrics_gauge`
- `otel_metrics_sum`
- `otel_metrics_histogram`
- `otel_metrics_exponentialhistogram`
- `otel_metrics_summary`
- `otel_logs`

## Example pipeline

1. OpenTelemetry Collector receives app telemetry.
2. Collector exports OTLP gRPC to `otel-otlp-gateway`.
3. Gateway serializes `Export*ServiceRequest` protobufs and publishes payloads to JetStream subjects.
4. Loader consumes JetStream records, decodes protobuf payloads, batches rows (50k or 2s), and writes using ClickHouse native protocol.

## Single Docker image build (two-stage)

The repository root `Dockerfile` performs a multi-stage build that:
- builds both services (with static first-party libraries),
- runs the CTest suite,
- installs both binaries,
- copies only the two installed binaries into the final image and installs only runtime packages (no build/dev packages and no manual shared-library collection step).

Build it with:

```bash
docker build -t otel-ingest-pipeline .
```

Included binaries in the final image:
- `/usr/local/bin/otel-otlp-gateway`
- `/usr/local/bin/jetstream-clickhouse-loader`

By default, the container starts `otel-otlp-gateway`.


## Jemalloc support and tuning

The build enables jemalloc by default on Linux via `-DOTEL_PIPELINE_USE_JEMALLOC=ON` and links it into both service binaries (`otel-otlp-gateway`, `jetstream-clickhouse-loader`).

In container images, jemalloc is also configured as a runtime fallback with:

```bash
LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
```

You can tune allocator behavior using `MALLOC_CONF`. Example values:

```bash
# Lower fragmentation in long-running workloads
export MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000

# More aggressive page purging under memory pressure
export MALLOC_CONF=background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:250
```

Validation checklist:

```bash
# Verify jemalloc is linked
ldd /usr/local/bin/otel-otlp-gateway | grep jemalloc
ldd /usr/local/bin/jetstream-clickhouse-loader | grep jemalloc

# Verify preload fallback is present
echo "$LD_PRELOAD"

# Optional allocator stats at process exit
MALLOC_CONF=stats_print:true /usr/local/bin/otel-otlp-gateway --config /etc/otel/gateway.yaml
```

For before/after memory-pressure comparisons, run the same ingest load profile twice (jemalloc off vs on), then compare RSS/heap metrics and allocator stats output over identical windows.

## Docker Compose full pipeline

A ready-to-run compose stack is provided in `docker-compose.yml` with:
- OpenTelemetry Collector (`otel-collector`)
- OTLP Gateway (`otel-otlp-gateway`)
- NATS JetStream (`nats`)
- JetStream stream bootstrap job (`jetstream-init`)
- ClickHouse (`clickhouse`)
- JetStream loader (`jetstream-clickhouse-loader`)

Start everything:

```bash
docker compose up --build
```

Send OTLP data to the collector at:
- gRPC: `localhost:4317`
- HTTP: `localhost:4318`

The collector forwards telemetry to the gateway, which publishes to JetStream subjects (`otel.traces`, `otel.metrics`, `otel.logs`).

ClickHouse tables are auto-created from `scripts/clickhouse_schema.sql` at container startup.

## Helm chart

A Helm chart is available at `charts/otel-telemetry-pipeline` for deploying:
- `otel-otlp-gateway`
- `jetstream-clickhouse-loader`

The chart expects reachable NATS and ClickHouse endpoints (defaults match the compose service hostnames).

Install example:

```bash
helm upgrade --install otel-pipeline ./charts/otel-telemetry-pipeline \
  --namespace observability \
  --create-namespace \
  --set gateway.image.repository=<your-registry>/otel-otlp-gateway \
  --set gateway.image.tag=<tag> \
  --set loader.image.repository=<your-registry>/jetstream-clickhouse-loader \
  --set loader.image.tag=<tag>
```

Override connectivity if needed:

```bash
helm upgrade --install otel-pipeline ./charts/otel-telemetry-pipeline \
  --set nats.url=nats://nats.my-namespace.svc.cluster.local:4222 \
  --set loader.clickhouse.host=clickhouse.my-namespace.svc.cluster.local
```
