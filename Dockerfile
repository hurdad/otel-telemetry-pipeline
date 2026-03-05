FROM ubuntu:24.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    pkg-config \
    protobuf-compiler \
    libprotobuf-dev \
    grpc-proto \
    libgrpc++-dev \
    zlib1g-dev \
    libgtest-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

RUN git submodule update --init --recursive

RUN cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DOTEL_PIPELINE_BUILD_TESTS=ON \
    -DCMAKE_INSTALL_PREFIX=/opt/otel \
    -DBUILD_SHARED_LIBS=OFF \
    && cmake --build build -j \
    && ctest --test-dir build --output-on-failure \
    && cmake --install build

FROM ubuntu:24.04 AS runtime

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgrpc++1.51 \
    libprotobuf32 \
    zlib1g \
    libstdc++6 \
    libgcc-s1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/otel/bin/otel-otlp-gateway /usr/local/bin/otel-otlp-gateway
COPY --from=builder /opt/otel/bin/jetstream-clickhouse-loader /usr/local/bin/jetstream-clickhouse-loader

ENTRYPOINT ["/usr/local/bin/otel-otlp-gateway"]
