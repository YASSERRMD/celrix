# CELRIX Multi-Architecture Dockerfile
# Supports amd64 and arm64

# Build stage
FROM --platform=$BUILDPLATFORM rust:1.75-bookworm AS builder

ARG TARGETPLATFORM
ARG BUILDPLATFORM

WORKDIR /app

# Install cross-compilation tools
RUN apt-get update && apt-get install -y \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/

# Build for release
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r celrix && useradd -r -g celrix celrix

# Create data directory
RUN mkdir -p /data && chown celrix:celrix /data

# Copy binary
COPY --from=builder /app/target/release/celrix-server /usr/local/bin/
COPY --from=builder /app/target/release/celrix-cli /usr/local/bin/

# Set permissions
RUN chmod +x /usr/local/bin/celrix-server /usr/local/bin/celrix-cli

USER celrix

WORKDIR /data

# Expose ports
EXPOSE 6380 16380 9090

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

# Entry point
ENTRYPOINT ["celrix-server"]
CMD ["--bind", "0.0.0.0:6380", "--admin-port", "9090"]
