# Build Stage
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY ../../../ .
RUN go build -o servicebot ./cmd/servicebot

# Runtime Stage
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/servicebot .
CMD ["./servicebot"]
