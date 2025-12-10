# Build Stage
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY ../../../ .
RUN go build -o detector ./cmd/detector

# Runtime Stage
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/detector .
CMD ["./detector"]
