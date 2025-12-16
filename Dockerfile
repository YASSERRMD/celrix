FROM rust:latest AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/celrix-server /usr/local/bin/
COPY --from=builder /app/target/release/celrix-cli /usr/local/bin/
EXPOSE 6380 9090
HEALTHCHECK --interval=30s --timeout=5s CMD curl -f http://localhost:9090/health || exit 1
ENTRYPOINT ["celrix-server"]
