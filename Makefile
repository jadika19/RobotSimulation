TIDY=go mod tidy

.PHONY: all build test coordinator detector

all: build
build:
	$(TIDY)
	go build ./...

test:
	go test ./...

coordinator:
	$(TIDY)
	go run ./cmd/coordinator

detector:
	$(TIDY)
	go run ./cmd/detector
