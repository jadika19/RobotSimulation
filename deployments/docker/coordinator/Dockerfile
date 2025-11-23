# Build Stage
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY ../../../ .       
RUN go build -o coordinator ./cmd/coordinator

# Runtime Stage
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/coordinator .
COPY --from=builder /app/internal/coordinator/live_map.html ./internal/coordinator/live_map.html
EXPOSE 8080 9001
CMD ["./coordinator"]
