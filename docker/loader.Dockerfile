FROM ubuntu:24.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential cmake git pkg-config protobuf-compiler libprotobuf-dev \
    grpc-proto libgrpc++-dev zlib1g-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN git submodule update --init --recursive && \
    cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build -j --target jetstream-clickhouse-loader

CMD ["/src/build/services/clickhouse_loader/jetstream-clickhouse-loader"]
