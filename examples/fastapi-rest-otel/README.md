# FastAPI sample app (OTEL -> Collector)

This sample REST app is instrumented with OpenTelemetry and exports traces over OTLP gRPC.

## Endpoints

- `GET /health`
- `GET /items`
- `POST /items?name=<name>`
- `GET /items/{item_id}`
- `DELETE /items/{item_id}`

## Run locally

```bash
cd examples/fastapi-rest-otel
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

By default the app exports traces to `http://localhost:4317`.

Set these env vars as needed:

- `OTEL_EXPORTER_OTLP_ENDPOINT` (default: `http://localhost:4317`)
- `OTEL_SERVICE_NAME` (default: `fastapi-rest-sample`)
- `DEPLOYMENT_ENV` (default: `local`)

## Run with Docker

```bash
docker build -t fastapi-rest-otel-sample .
docker run --rm -p 8000:8000 \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4317 \
  fastapi-rest-otel-sample
```

## Generate traffic manually

```bash
curl http://localhost:8000/health
curl -X POST 'http://localhost:8000/items?name=demo'
curl http://localhost:8000/items
```

## Generate traffic with Locust

A load profile is provided in `locustfile.py`.

```bash
cd examples/fastapi-rest-otel
locust -f locustfile.py --host http://localhost:8000
```

Or run headless:

```bash
locust -f locustfile.py --host http://localhost:8000 --headless --users 15 --spawn-rate 3
```

## Compose integration

This repository's root `docker-compose.yml` includes:

- `fastapi-rest-sample` (app container)
- `fastapi-rest-sample-loadgen` (Locust headless load generator)

Telemetry flow:

```text
FastAPI app -> OpenTelemetry Collector -> otlp-gateway -> NATS JetStream -> ClickHouse
```
