FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY ../../../ .
RUN go build -o world ./cmd/world

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/world .
COPY --from=builder /app/internal/world/live_map.html ./internal/world/live_map.html
CMD ["./world"]
