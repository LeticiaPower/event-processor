
build:
	go build -o bin/producer ./producer/cmd
	go build -o bin/consumer ./consumer/cmd

install:
	go mod download
	go mod tidy

up:
	docker compose up -d

down:
	docker compose down

run-producer:
	go run ./producer/cmd/main.go

run-consumer:
	go run ./consumer/cmd/main.go

test:
	go test ./consumer/service