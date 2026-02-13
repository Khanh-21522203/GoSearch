# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /build

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files first for caching
COPY go.mod go.sum* ./
RUN go mod download

# Copy source
COPY . .

# Build binary
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.Version=${VERSION}" \
    -o gotextsearch \
    ./cmd/server

# Runtime stage
FROM alpine:3.19

# Security: run as non-root
RUN addgroup -g 1000 gotextsearch && \
    adduser -u 1000 -G gotextsearch -D gotextsearch

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/gotextsearch /app/gotextsearch

# Create data directory
RUN mkdir -p /data && chown gotextsearch:gotextsearch /data

USER gotextsearch

# Default configuration
ENV GOTEXTSEARCH_DATA_DIR=/data
ENV GOTEXTSEARCH_PORT=8080
ENV GOTEXTSEARCH_LOG_LEVEL=info

EXPOSE 8080

VOLUME ["/data"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/gotextsearch"]
CMD ["--config=/app/config.yaml"]
