.PHONY: up down logs clean build test verify
.DEFAULT_GOAL := help

help:
	@echo "Kafka Laboratory Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make up          - Start the Kafka environment via Docker Compose"
	@echo "  make down        - Stop the Kafka environment"
	@echo "  make logs        - Tail logs of the Kafka environment"
	@echo "  make clean       - Maven clean"
	@echo "  make build       - Maven compile"
	@echo "  make test        - Run tests"
	@echo "  make verify      - Run all verification (clean, test, verify)"

up:
	@echo "Progressive Environment: Go to the specific lab directory (e.g. labs/lab-001) and run docker-compose up -d"

down:
	@echo "Progressive Environment: Go to the specific lab directory (e.g. labs/lab-001) and run docker-compose down -v"

logs:
	@echo "Progressive Environment: Go to the specific lab directory (e.g. labs/lab-001) and run docker-compose logs -f"

clean:
	@./mvnw clean || mvn clean

build:
	@./mvnw compile || mvn compile

test:
	@./mvnw test || mvn test

verify:
	@./mvnw clean verify || mvn clean verify
