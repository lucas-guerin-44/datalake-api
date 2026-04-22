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

echo ">> building web image (static landing)"
# The API image is built by this host-loaded tag, but the web service is
# defined with `build:` in docker-compose.prod.yml — rebuild it each deploy so
# landing-page changes picked up in `git pull` ship.
docker compose -f docker-compose.prod.yml build web

echo ">> bringing up prod stack (API_IMAGE=${IMAGE})"
API_IMAGE="${IMAGE}" docker compose -f docker-compose.prod.yml up -d

echo ">> healthcheck"
sleep 5
# Caddy now serves the landing page at / and the API under /api/*. The API
# liveness endpoint is /api/healthcheck through the edge; /healthcheck on :80
# would hit the static site and return HTML.
if ! curl -fsS http://127.0.0.1/api/healthcheck > /dev/null; then
    echo "WARNING: API healthcheck failed. Check 'docker compose logs api'."
    exit 1
fi
if ! curl -fsS -o /dev/null http://127.0.0.1/; then
    echo "WARNING: landing page unreachable. Check 'docker compose logs web caddy'."
    exit 1
fi

# Keep the current image + the N most-recent previous tags, delete older ones.
# Also prune dangling layers (rebuilds leave a lot of <none>-tagged intermediates).
KEEP_IMAGES="${KEEP_IMAGES:-3}"
echo ">> cleanup: keeping most-recent ${KEEP_IMAGES} datalake-api:<sha> tags"

# Sort tagged datalake-api:* images by CreatedAt desc, skip the 'latest' pointer
# and the currently-running tag, then remove the tail.
docker images --format '{{.Repository}}:{{.Tag}} {{.CreatedAt}}' datalake-api \
    | grep -v ':latest ' \
    | grep -v "^${IMAGE} " \
    | sort -k2 -r \
    | awk -v keep="${KEEP_IMAGES}" 'NR > keep {print $1}' \
    | xargs -r docker rmi 2>/dev/null || true

# Dangling (<none>) layers from rebuilds. Safe: they have no tag and no
# container refers to them.
docker image prune -f >/dev/null

echo ">> deployed ${IMAGE}"
