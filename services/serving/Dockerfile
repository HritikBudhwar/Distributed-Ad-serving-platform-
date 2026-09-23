FROM rust:bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake g++ protobuf-compiler pkg-config libssl-dev libsasl2-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY proto /proto
COPY services/serving /src
ENV PROTO_FILE=/proto/adpulse.proto
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/adpulse-serving /usr/local/bin/adpulse-serving
ENV HTTP_PORT=8000
EXPOSE 8000
CMD ["adpulse-serving"]
