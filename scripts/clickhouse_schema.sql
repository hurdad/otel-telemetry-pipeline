CREATE TABLE IF NOT EXISTS otel_traces
(
    timestamp DateTime64(9),
    trace_id String,
    span_id String,
    parent_span_id String,
    service_name LowCardinality(String),
    operation_name String,
    duration UInt64
)
ENGINE = MergeTree
ORDER BY (service_name, timestamp);

CREATE TABLE IF NOT EXISTS otel_metrics
(
    timestamp DateTime64(9),
    service_name LowCardinality(String),
    metric_name String,
    value Float64
)
ENGINE = MergeTree
ORDER BY (service_name, timestamp);

CREATE TABLE IF NOT EXISTS otel_logs
(
    timestamp DateTime64(9),
    service_name LowCardinality(String),
    severity_text LowCardinality(String),
    body String
)
ENGINE = MergeTree
ORDER BY (service_name, timestamp);
