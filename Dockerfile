FROM ubuntu:24.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    protobuf-compiler \
    libprotobuf-dev \
    protobuf-compiler-grpc \
    libgrpc++-dev \
    zlib1g-dev \
    libgtest-dev \
    libjemalloc-dev \
    libyaml-cpp-dev \
    libspdlog-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DOTEL_PIPELINE_BUILD_TESTS=ON \
    -DCMAKE_INSTALL_PREFIX=/opt/otel \
    -DBUILD_SHARED_LIBS=OFF \
    && cmake --build build -j 4 \
    && ctest --test-dir build --output-on-failure \
    && cmake --install build

# ---------------------------------------------------------------------------
# Gateway runtime
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS gateway

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51 \
    libprotobuf32 \
    zlib1g \
    libstdc++6 \
    libgcc-s1 \
    libjemalloc2 \
    libyaml-cpp0.8 \
    libspdlog1.12 \
    netcat-openbsd \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home --shell /bin/false otel

COPY --from=builder /opt/otel/bin/otlp-gateway /usr/local/bin/otlp-gateway

USER otel

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD nc -z localhost 4317 || exit 1

ENTRYPOINT ["/usr/local/bin/otlp-gateway"]

# ---------------------------------------------------------------------------
# Loader runtime
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS loader

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libstdc++6 \
    libgcc-s1 \
    libjemalloc2 \
    libgrpc++1.51 \
    libprotobuf32 \
    libyaml-cpp0.8 \
    libspdlog1.12 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --no-create-home --shell /bin/false otel

COPY --from=builder /opt/otel/bin/jetstream-clickhouse-loader /usr/local/bin/jetstream-clickhouse-loader

USER otel

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD grep -q "jetstream" /proc/1/status || exit 1

ENTRYPOINT ["/usr/local/bin/jetstream-clickhouse-loader"]
