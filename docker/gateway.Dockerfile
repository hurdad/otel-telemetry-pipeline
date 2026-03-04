FROM ubuntu:24.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git pkg-config protobuf-compiler libprotobuf-dev \
    grpc-proto libgrpc++-dev zlib1g-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN git submodule update --init --recursive && \
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -j --target otel-otlp-gateway

EXPOSE 4317
CMD ["/src/build/services/otlp_gateway/otel-otlp-gateway"]
