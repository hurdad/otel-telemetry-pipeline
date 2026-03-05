# otel-telemetry-pipeline

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

## Build instructions

```bash
git submodule update --init --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Binaries:
- `build/services/otlp_gateway/otel-otlp-gateway`
- `build/services/clickhouse_loader/jetstream-clickhouse-loader`

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
- `otel_metrics`
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
docker build -t otel-telemetry-pipeline .
```

Included binaries in the final image:
- `/usr/local/bin/otel-otlp-gateway`
- `/usr/local/bin/jetstream-clickhouse-loader`

By default, the container starts `otel-otlp-gateway`.

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
