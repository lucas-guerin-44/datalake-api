-include .env
export

.PHONY: help build up down logs restart clean health test backend shell-api shell-db deploy deploy-check

# VPS deploy target — set VPS=user@host on the command line or in .env
VPS ?=
REMOTE_PATH ?= /opt/datalake-api

# On Windows, MSYS ships its own ssh that reads keys from /home/<user>/.ssh
# (not %USERPROFILE%\.ssh). Force Windows's native OpenSSH — same binary your
# interactive PowerShell session uses, so it picks up the existing known_hosts.
ifeq ($(OS),Windows_NT)
SSH ?= /c/Windows/System32/OpenSSH/ssh.exe
else
SSH ?= ssh
endif

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
	@echo ""
	@echo "  Deploy:"
	@echo "    make deploy VPS=user@host       Build locally, ship image over SSH, compose up"
	@echo "    make deploy-check VPS=user@host Dry-run summary (no changes made)"

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

# --- Remote deploy: build locally, ship image over SSH, reuse deploy.sh for compose up ---
# Requires the VPS to already have the repo cloned at $(REMOTE_PATH) with a valid .env.
# Compose/Caddy changes are picked up by `git pull` on the remote, so commit+push first.
deploy:
	@[ -n "$(VPS)" ] || { echo "Error: set VPS=user@host (e.g. make deploy VPS=datalake@datalake.lucasguerin.fr)" >&2; exit 2; }
	@# --ignore-cr-at-eol tolerates core.autocrlf phantom diffs on Windows;
	@# untracked files are ignored here — the real intent is "no unstaged content diffs vs HEAD".
	@if ! git diff --quiet --ignore-cr-at-eol HEAD -- 2>/dev/null; then \
	     echo "Error: uncommitted content changes vs HEAD. Commit or stash first." >&2; \
	     git diff --name-only --ignore-cr-at-eol HEAD -- >&2; \
	     exit 2; \
	 fi
	@SHA=$$(git rev-parse --short HEAD); \
	 IMAGE=datalake-api:$$SHA; \
	 echo ">> [local] building $$IMAGE"; \
	 docker build -t "$$IMAGE" -t datalake-api:latest . && \
	 echo ">> [ship] saving + transferring image to $(VPS)" && \
	 docker save "$$IMAGE" | gzip | $(SSH) $(VPS) "gunzip | docker load" && \
	 echo ">> [remote] git pull (compose/Caddy/migrations)" && \
	 $(SSH) $(VPS) "cd $(REMOTE_PATH) && git pull --ff-only" && \
	 echo ">> [remote] API_IMAGE=$$IMAGE ./deploy/deploy.sh --no-pull --skip-build" && \
	 $(SSH) $(VPS) "cd $(REMOTE_PATH) && API_IMAGE=$$IMAGE ./deploy/deploy.sh --no-pull --skip-build" && \
	 echo ">> deployed $$IMAGE to $(VPS)"

deploy-check:
	@[ -n "$(VPS)" ] || { echo "Error: set VPS=user@host" >&2; exit 2; }
	@SHA=$$(git rev-parse --short HEAD); \
	 echo "Would build:   datalake-api:$$SHA"; \
	 echo "Would ship to: $(VPS):$(REMOTE_PATH)"; \
	 if git diff --quiet --ignore-cr-at-eol HEAD -- 2>/dev/null; then \
	     echo "Working tree: clean"; \
	 else \
	     echo "Working tree: DIRTY (deploy will refuse):"; \
	     git diff --name-only --ignore-cr-at-eol HEAD --; \
	 fi; \
	 UNPUSHED=$$(git log @{u}..HEAD --oneline 2>/dev/null | wc -l); \
	 if [ "$$UNPUSHED" -gt 0 ]; then echo "WARNING: $$UNPUSHED commit(s) not pushed — remote git pull won't see them"; fi
