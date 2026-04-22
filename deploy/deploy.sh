#!/usr/bin/env bash
#
# Build the API image tagged by git SHA, update a 'latest' pointer,
# then bring up the prod stack. Run this on the VPS from the repo root.
#
# Usage:
#   ./deploy/deploy.sh                     # build current HEAD, deploy
#   ./deploy/deploy.sh --no-pull           # skip git pull (already updated)
#   ./deploy/deploy.sh --skip-build        # image already loaded (e.g. via `make deploy`)
#                                          #   — requires API_IMAGE env var set

set -euo pipefail

SKIP_PULL=0
SKIP_BUILD=0
for arg in "$@"; do
    case "$arg" in
        --no-pull) SKIP_PULL=1 ;;
        --skip-build) SKIP_BUILD=1 ;;
        *) echo "Unknown arg: $arg" >&2; exit 2 ;;
    esac
done

if [ "$SKIP_PULL" -eq 0 ]; then
    echo ">> git pull"
    git pull --ff-only
fi

if [ "$SKIP_BUILD" -eq 1 ]; then
    : "${API_IMAGE:?API_IMAGE env var must be set when using --skip-build}"
    IMAGE="${API_IMAGE}"
else
    SHA="$(git rev-parse --short HEAD)"
    IMAGE="datalake-api:${SHA}"
fi

# The container runs as uid 1000 (non-root). Bind-mounted dirs on the host
# need matching ownership, otherwise duckdb/postgres init fails with EACCES.
mkdir -p datalake staging backups
sudo chown -R 1000:1000 datalake staging backups

if [ "$SKIP_BUILD" -eq 0 ]; then
    echo ">> building ${IMAGE}"
    docker build -t "${IMAGE}" -t "datalake-api:latest" .
else
    echo ">> skipping build, using preloaded image ${IMAGE}"
fi

echo ">> bringing up prod stack (API_IMAGE=${IMAGE})"
API_IMAGE="${IMAGE}" docker compose -f docker-compose.prod.yml up -d

echo ">> healthcheck"
sleep 5
if ! curl -fsS http://127.0.0.1/healthcheck > /dev/null; then
    echo "WARNING: healthcheck failed. Check 'docker compose logs api'."
    exit 1
fi

echo ">> deployed ${IMAGE}"
