-include .env
export

.PHONY: help build up down logs restart clean health test backend shell-api shell-db

help:
	@echo "Usage:"
	@echo ""
	@echo "  Docker:"
	@echo "    make up              Start services"
	@echo "    make build           Rebuild Docker images"
	@echo "    make down            Stop services"
	@echo "    make logs            Tail logs"
	@echo "    make restart         Restart services"
	@echo "    make clean           Stop + delete volumes (destroys data)"
	@echo "    make shell-api       Shell into API container"
	@echo "    make shell-db        PostgreSQL shell"
	@echo ""
	@echo "  Development:"
	@echo "    make backend         Run API locally (hot-reload)"
	@echo "    make test            Run test suite"
	@echo "    make health          Check service health"

build:
	docker compose build

up:
	docker compose up -d
	@echo "API:  http://localhost:$(API_PORT)"
	@echo "Docs: http://localhost:$(API_PORT)/docs"

down:
	docker compose down

logs:
	docker compose logs -f

restart:
	docker compose restart

clean:
	docker compose down -v
	@echo "All containers and volumes removed"

health:
	@curl -sf http://localhost:$(API_PORT)/healthcheck && echo ""
	@docker exec datalake-postgres pg_isready -U $(POSTGRES_USER)

shell-api:
	docker exec -it ohlc-datalake-api /bin/bash

shell-db:
	docker exec -it datalake-postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

backend:
	uvicorn src.api:app --reload --port $(API_PORT)

test:
	pytest tests/ -v
