from __future__ import annotations

import os
import random
from typing import Dict

from fastapi import FastAPI, HTTPException
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.metrics import CallbackOptions, Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
OTEL_SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "fastapi-rest-sample")

resource = Resource.create(
    {
        "service.name": OTEL_SERVICE_NAME,
        "service.version": "1.0.0",
        "deployment.environment": os.getenv("DEPLOYMENT_ENV", "local"),
    }
)

provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(
            endpoint=OTEL_ENDPOINT,
            insecure=True,
        )
    )
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(
        endpoint=OTEL_ENDPOINT,
        insecure=True,
    )
)
metrics.set_meter_provider(
    MeterProvider(resource=resource, metric_readers=[metric_reader])
)
meter = metrics.get_meter(__name__)

app = FastAPI(title="FastAPI OTEL Sample")
FastAPIInstrumentor.instrument_app(app)

items: Dict[int, dict] = {}


def observe_current_items(_: CallbackOptions) -> list[Observation]:
    return [
        Observation(
            len(items),
            {
                "deployment.environment": os.getenv("DEPLOYMENT_ENV", "local"),
                "service.name": OTEL_SERVICE_NAME,
            },
        )
    ]


meter.create_observable_gauge(
    name="app.items.current",
    callbacks=[observe_current_items],
    unit="1",
    description="Current number of items stored in in-memory repository.",
)


@app.get("/health")
def health() -> dict:
    with tracer.start_as_current_span("healthcheck") as span:
        span.set_attribute("app.health", True)
        return {"status": "ok"}


@app.get("/items")
def list_items() -> dict:
    with tracer.start_as_current_span("list_items") as span:
        span.set_attribute("items.count", len(items))
        return {"items": list(items.values())}


@app.post("/items")
def create_item(name: str) -> dict:
    with tracer.start_as_current_span("create_item") as span:
        item_id = random.randint(1000, 9999)
        item = {"id": item_id, "name": name}
        items[item_id] = item

        span.set_attribute("item.id", item_id)
        span.set_attribute("item.name", name)
        span.add_event("item.created")

        return item


@app.get("/items/{item_id}")
def get_item(item_id: int) -> dict:
    with tracer.start_as_current_span("get_item") as span:
        span.set_attribute("item.id", item_id)
        if item_id not in items:
            span.set_attribute("item.found", False)
            raise HTTPException(status_code=404, detail="item not found")

        span.set_attribute("item.found", True)
        return items[item_id]


@app.delete("/items/{item_id}")
def delete_item(item_id: int) -> dict:
    with tracer.start_as_current_span("delete_item") as span:
        span.set_attribute("item.id", item_id)
        if item_id not in items:
            span.set_attribute("item.found", False)
            raise HTTPException(status_code=404, detail="item not found")

        deleted = items.pop(item_id)
        span.set_attribute("item.found", True)
        span.add_event("item.deleted")
        return deleted
