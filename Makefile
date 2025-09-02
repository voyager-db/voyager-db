BINS=voyagerd voyagerctl


.PHONY: all build test lint docker compose-up compose-down


all: build


build:
	go build ./cmd/voyagerd
	go build ./cmd/voyagerctl


test:
	go test -race ./...


lint:
	golangci-lint run


docker:
	docker build -t ghcr.io/voyager-db/voyagerd:latest .


compose-up:
	cd deploy && docker compose up -d --build


compose-down:
cd deploy && docker compose down -v