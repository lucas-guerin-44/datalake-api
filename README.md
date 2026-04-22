# Datalake API

A containerized REST API for storing and querying financial market data — OHLC bars and tick-level data — with **automatic multi-timeframe derivation on ingest**, atomic writes, Parquet backup/restore, and WebSocket streaming for live-feed simulation. Built with FastAPI, DuckDB, and PostgreSQL.

Designed for personal use by algo traders who want a central store for historical price data imported from CSV exports (MetaTrader 5 and Dukascopy formats auto-detected).

## Architecture

```
  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
  │  CSV / Excel │   │   REST API   │   │  WebSocket   │
  │   uploads    │   │   clients    │   │  consumers   │
  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
         │                  │                   │
         ▼                  ▼                   ▼
       ┌─────────────────────────────────────────┐
       │          FastAPI app (src/api.py)       │
       └───────────────────┬─────────────────────┘
                           │
                     ┌─────┴─────┐
                     ▼           ▼
               ┌──────────┐  ┌────────────┐
               │  DuckDB  │  │ PostgreSQL │
               │ OHLC +   │  │ users &    │
               │ tick data│  │ API keys   │
               └──────────┘  └────────────┘
```

| Component | Purpose |
|-----------|---------|
| **DuckDB** | OHLC bars and tick data in a single embedded file (`datalake/ohlc.duckdb`). Fast columnar scans. |
| **PostgreSQL** | User accounts and API keys. |
| **FastAPI** | REST + WebSocket API for ingestion, querying, streaming, and auth. |
| **Docker Compose** | Orchestrates PostgreSQL + API. DuckDB is bind-mounted. |

### Key behaviors

- **Auto-derivation on ingest.** Upload M1 bars (or ticks) and you automatically get M5, M15, M30, H1, H4, D1 as derived rows — no extra uploads needed. Each bar is marked `source='raw'` or `source='derived'`; derivation never clobbers raw rows.
- **Atomic ingest + derive.** Every ingest is wrapped in a single write transaction. A crash mid-derive rolls back the raw insert too, so the datalake can't end up half-written.
- **UTC timestamps.** All stored timestamps are naive UTC wall-clock. If you ingested data on an older build (pre-UTC-fix), run `POST /catalog/migrate-timezone` once to correct it (see below).
- **Single-writer serialization.** A process-level lock makes DuckDB's single-writer nature explicit — multiple concurrent ingests queue cleanly instead of stepping on each other.
- **Backup/restore to Parquet.** `POST /catalog/export` dumps the entire datalake to partitioned Parquet + a manifest; `POST /catalog/restore` merges it back. Survives full DB loss as long as you have the backup directory.
- **Background jobs.** Long ingests/exports can run out-of-band via `background=true`; poll `/jobs/{id}` for status.

## Data Ingestion

### OHLC bars

Export OHLC data from your trading platform as CSV with columns: `timestamp`, `open`, `high`, `low`, `close`. MetaTrader's `<DATE>`, `<TIME>`, `<OPEN>`, `<HIGH>`, `<LOW>`, `<CLOSE>` export format is auto-detected. Excel files (`.xlsx`, `.xls`) also supported.

```bash
# Single file — ingests + auto-derives higher timeframes (M5, M15, M30, H1, H4, D1)
curl -X POST http://localhost:8000/ingest \
  -H "X-API-Key: $API_KEY" \
  -F "file=@XAUUSD_M1_20240101_20241231.csv" \
  -F "instrument=XAUUSD" \
  -F "timeframe=M1"

# Disable derivation if you only want the raw timeframe
curl -X POST http://localhost:8000/ingest \
  -F "file=@..." -F "instrument=XAUUSD" -F "timeframe=M1" -F "derive=false" ...

# Run derivation as a background job (returns a derive_job_id to poll via /jobs/{id})
curl -X POST http://localhost:8000/ingest \
  -F "file=@..." -F "instrument=XAUUSD" -F "timeframe=M1" -F "background=true" ...

# Batch: place files in staging/ as {INSTRUMENT}_{TIMEFRAME}_*.csv
curl -X POST http://localhost:8000/ingest-batch \
  -H "X-API-Key: $API_KEY"
```

**Pro tip:** ingest the finest timeframe you have (M1, or ticks) and let derivation populate everything else. One upload → all timeframes.

### Tick data

Tick CSV files with columns: `timestamp`, `price`, `volume` (optional: `bid`, `ask`). If only `bid`/`ask` are provided, `price` is computed as the mid. Auto-detects MetaTrader tick exports (`<DATE>`, `<BID>`, `<ASK>`, `<LAST>`, `<VOLUME>`) and Dukascopy format (`Gmt time`, `Bid`, `Ask`, `Volume`).

Tick ingest also auto-derives the full OHLC ladder (M1 through D1).

```bash
# Single file
curl -X POST http://localhost:8000/ingest/ticks \
  -H "X-API-Key: $API_KEY" \
  -F "file=@XAUUSD_ticks_202401.csv" \
  -F "instrument=XAUUSD"

# Batch: place files in staging/ as {INSTRUMENT}_TICK_*.csv
curl -X POST http://localhost:8000/ingest-batch/ticks \
  -H "X-API-Key: $API_KEY"
```

### Ingest observability

Every ingest emits a structured log line with timing per phase and row-count deltas:

```json
{
  "message": "File ingestion completed",
  "file": "XAUUSD_M1_2024.csv",
  "instrument": "XAUUSD",
  "timeframe": "M1",
  "rows_in_file": 2600000,
  "rows_new": 2600000,
  "rows_matched": 0,
  "derived_targets": {"M5": 520000, "M15": 173333, "M30": 86666, "H1": 43333, "H4": 10833, "D1": 1806},
  "timing_ms": {"read": 8432, "upsert": 2104, "derive": 4891, "total": 15471}
}
```

## WebSocket Streaming

Live-feed simulation by replaying historical data over WebSocket. Useful for backtesting dashboards.

```bash
# Stream ticks at real-time speed
wscat -c "ws://localhost:8000/ws/ticks?instrument=XAUUSD&speed=1"

# Stream M5 bars at 60x speed (one bar per second)
wscat -c "ws://localhost:8000/ws/bars?instrument=XAUUSD&timeframe=M5&speed=60"

# Fast delivery for dashboard replay (consumer controls pacing)
wscat -c "ws://localhost:8000/ws/ticks?instrument=XAUUSD&speed=1000&max_delay=0.1"

# Burst mode — no pacing, all rows as fast as possible
wscat -c "ws://localhost:8000/ws/ticks?instrument=XAUUSD&max_delay=0"
```

| Param | Default | Description |
|-------|---------|-------------|
| `instrument` | required | Symbol to stream |
| `timeframe` | required (bars only) | Bar timeframe (M5, H1, etc.) |
| `start` / `end` | — | ISO-8601 time range filter |
| `speed` | `1.0` | Playback multiplier (1 = real-time, 10 = 10x) |
| `max_delay` | `10.0` | Max seconds between messages (0 = burst) |

Each message is a JSON object. Stream ends with `{"done": true}`.

## Backup & Restore

### Export

```bash
curl -X POST http://localhost:8000/catalog/export \
  -H "X-API-Key: dk_..." \
  -F "background=true"
# → {"status": "ok", "job_id": "..."}

# Poll the job
curl http://localhost:8000/jobs/<job_id>
```

Output lives in `backups/<UTC-timestamp>/`:
```
backups/2026-04-21T22-00-00Z/
├── manifest.json
├── ohlc/instrument=XAUUSD/timeframe=M1/data_0.parquet
├── ohlc/instrument=XAUUSD/timeframe=M5/data_0.parquet
└── ticks/instrument=XAUUSD/data_0.parquet
```

`manifest.json` records row counts and schema version. Rsync the directory offsite for disaster recovery.

### Restore

```bash
curl -X POST http://localhost:8000/catalog/restore \
  -H "X-API-Key: dk_..." \
  -F "manifest_path=/app/backups/2026-04-21T22-00-00Z/manifest.json"
```

Restore is **idempotent** — merges rows via `ON CONFLICT`. Running it twice doesn't double-up anything. Safe to run against a live datalake.

### Coverage gaps

```bash
curl "http://localhost:8000/catalog/gaps?instrument=XAUUSD&timeframe=M1"
```

Returns unusually-large gaps (> 2× the bar size by default) so you can spot missing data ranges. FX weekend closures are flagged with `is_weekend: true` so you can filter them out.

## Quick Start

### 1. Configure

```bash
cp .env.example .env
# Optional: set POSTGRES_PASSWORD to something non-default for real deployments.
```

### 2. Start

```bash
docker compose up -d
# or: make up
```

### 3. Create a user and mint an API key

There is no HTTP registration or login flow. Users and keys are provisioned through the container:

```bash
# Seed a user (password is random + discarded — API keys are the only credential)
docker compose exec -T api python - <<'EOF'
import secrets
from src.core.database import get_db_context, create_user, get_user_by_username
from src.auth.auth import get_password_hash
with get_db_context() as db:
    if not get_user_by_username(db, "admin"):
        create_user(db, "admin", "admin@example.com", get_password_hash(secrets.token_urlsafe(32)))
EOF

# Mint a long-lived admin key — printed once, copy it into a password manager
docker compose exec api python -m scripts.mint_api_key --username admin --name "local-dev" --scopes admin
export API_KEY="dk_..."
```

**Using `/docs`?** Paste the key into Swagger's "Authorize" → `X-API-Key`. Re-paste it after page reloads.

### 4. Query

```bash
# OHLC query (public if ALLOW_PUBLIC_READS=true)
curl "http://localhost:8000/query?instrument=XAUUSD&timeframe=M5&start=2024-01-01&end=2024-12-31&limit=100"

# Tick query
curl "http://localhost:8000/ticks?instrument=XAUUSD&start=2024-01-01&limit=10000"

# Download as CSV
curl "http://localhost:8000/download?instrument=XAUUSD&timeframe=M5" -o ohlc.csv
curl "http://localhost:8000/ticks/download?instrument=XAUUSD" -o ticks.csv
```

## API Reference

Interactive docs at `http://localhost:8000/docs`.

| Endpoint | Auth | Description |
|----------|------|-------------|
| `POST /auth/api-keys` | Admin | Create API key for the calling admin's user |
| `GET /auth/api-keys` | Admin | List API keys for the calling admin's user |
| `PATCH /auth/api-keys/{id}` | Admin | Update API key |
| `DELETE /auth/api-keys/{id}` | Admin | Revoke API key |
| `GET /query` | Public* | Query OHLC data (cursor-paginated) |
| `GET /download` | Public* | Download OHLC as CSV |
| `GET /ticks` | Public* | Query tick data (cursor-paginated) |
| `GET /ticks/download` | Public* | Download ticks as CSV |
| `GET /catalog` | Public* | Database stats and coverage (OHLC + ticks, with `sources: [raw, derived]`) |
| `GET /catalog/stats` | Public* | Quick row-count stats |
| `GET /catalog/gaps` | Public* | Find oversized gaps in an OHLC series |
| `POST /catalog/export` | Write | Dump the datalake to partitioned Parquet + manifest |
| `POST /catalog/restore` | Admin | Merge a prior export back into the live datalake |
| `POST /catalog/migrate-timezone` | Admin | One-shot UTC data fix (see below) |
| `GET /instruments` | Public* | List instruments (OHLC + ticks) |
| `GET /instruments/{symbol}` | Public* | Coverage per timeframe, including `sources` |
| `GET /timeframes` | Public* | List timeframes (incl. TICK) |
| `POST /ingest` | Write | Upload OHLC CSV/Excel file (auto-derives by default) |
| `POST /ingest-batch` | Write | Batch import OHLC from folder |
| `POST /ingest/ticks` | Write | Upload tick CSV file (auto-derives OHLC M1..D1) |
| `POST /ingest-batch/ticks` | Write | Batch import ticks from folder |
| `GET /jobs` | Public* | List recent background jobs |
| `GET /jobs/{id}` | Public* | Get status/result of a specific job |
| `WS /ws/ticks` | Public* | Stream tick data (live-feed replay) |
| `WS /ws/bars` | Public* | Stream OHLC bars (live-feed replay) |
| `GET /healthcheck` | No | Health check |
| `GET /metrics` | Admin | Prometheus metrics (request counts, latency histograms) |
| `GET /public/stats` | No (always) | Aggregate-only landing-page stats, cached 60s, rate-limited 30/min |

*Public when `ALLOW_PUBLIC_READS=true`, requires auth when `false` (default).

**Ingest form params:**
- `derive` (bool, default `true`) — auto-materialize higher timeframes
- `background` (bool, default `false`) — run derivation in a background job; response returns `derive_job_id`

**Auth:** API key only — send `X-API-Key: dk_...` on every call. Mint keys via `scripts/mint_api_key.py` or the `POST /auth/api-keys` admin endpoint. WebSocket endpoints read the same header; query-string credentials are rejected to keep them out of proxy logs.

**API key scopes:** `read` (query/download), `write` (read + ingest + export), `admin` (all, incl. restore + migrate-timezone + key management)

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_PASSWORD` | `datalake` | PostgreSQL password |
| `API_PORT` | `8000` | API port |
| `ALLOW_PUBLIC_READS` | `false` | Allow unauthenticated reads/streams |
| `RATE_LIMIT_ENABLED` | `true` | Enable slowapi rate limits |
| `RATE_LIMIT_DEFAULT` | `120/minute` | Default per-IP limit |
| `MAX_WS_PER_CLIENT` | `5` | Max concurrent WebSocket connections per client IP |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `DUCKDB_PATH` | `datalake/ohlc.duckdb` | Path to the DuckDB file |
| `DUCKDB_MEMORY_LIMIT` | `2GB` | Memory cap for DuckDB query execution |
| `MAX_UPLOAD_SIZE_MB` | `500` | Reject uploads larger than this |

## Schema migrations

DuckDB schema changes live as numbered Python modules in `src/core/migrations_sql/`, each exposing `up(con)`. A `_schema_migrations` table records applied versions so each runs exactly once per DB file. Add a new migration by dropping a new `NNN_description.py` alongside the existing ones — it'll run on next startup.

## One-shot: fix pre-UTC data

If your datalake was populated on an older build, its timestamps are stored in the host's local timezone (not UTC). Run this **once** to shift them:

```bash
curl -X POST http://localhost:8000/catalog/migrate-timezone \
  -H "X-API-Key: dk_..." \
  -F "source_timezone=Europe/Berlin"
```

Substitute the IANA zone name that matches the machine that originally ran the ingest (`Europe/Berlin`, `America/New_York`, etc.). Rewrites every row in `ohlc_data` and `tick_data` to UTC wall-clock. Admin scope required. **Take a backup first** via `POST /catalog/export`.

## Local Development

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt -r requirements-test.txt

# Start PostgreSQL
docker run -d --name datalake-postgres \
  -e POSTGRES_DB=datalake -e POSTGRES_USER=datalake -e POSTGRES_PASSWORD=datalake \
  -p 6543:5432 postgres:16-alpine

# Run the API
uvicorn src.api:app --reload

# Run tests
pytest tests/ -v
```

## Project Structure

```
src/
├── api.py                      # FastAPI entry point
├── config.py                   # Environment loading
├── schemas.py                  # Pydantic models
├── auth/
│   └── auth.py                 # API key auth
├── core/
│   ├── database.py             # SQLAlchemy models (User, APIKey)
│   ├── datalake.py             # DuckDB operations (OHLC + tick + derivation + gaps)
│   ├── migrations.py           # Lightweight DuckDB migration runner
│   ├── migrations_sql/         # Numbered migration modules (NNN_*.py)
│   └── pagination.py           # Cursor-based pagination
├── middleware/
│   ├── logging_config.py       # Structured JSON logging
│   ├── middleware.py           # Request logging (redacts token / api_key query params)
│   └── ratelimit.py            # slowapi limiter (per-IP)
├── services/
│   ├── backup.py               # Parquet export/restore
│   ├── jobs.py                 # In-memory background job registry
│   ├── pipeline.py             # CSV/Excel ingestion (OHLC + tick) + derivation trigger
│   └── validators.py           # Input validation + filename sanitization
└── routes/
    ├── auth_routes.py          # /auth/*
    ├── catalog.py              # /catalog, /catalog/gaps, /catalog/export, /catalog/restore, /catalog/migrate-timezone
    ├── health.py               # /healthcheck
    ├── ingest.py               # /ingest, /ingest/ticks, /ingest-batch*
    ├── instruments.py          # /instruments, /timeframes
    ├── jobs.py                 # /jobs, /jobs/{id}
    ├── public.py               # /public/stats (aggregate-only, no auth, cached)
    ├── query.py                # /query, /download, /ticks, /ticks/download
    └── stream.py               # /ws/ticks, /ws/bars

web/                            # Astro + Tailwind landing page (served at https://datalake.lucasguerin.fr/)
├── src/
│   ├── layouts/Base.astro
│   ├── pages/index.astro
│   ├── components/             # Hero, StatsStrip (live via /api/public/stats), Architecture, Stack, SampleResponse
│   └── styles/global.css
├── astro.config.mjs
└── package.json
```

## Landing page

`web/` is a static Astro site served at the apex `https://datalake.lucasguerin.fr/`. It's built inside `web.Dockerfile` (Node builder → Caddy runtime) and wired into `docker-compose.prod.yml` as the `web` service. The edge Caddy proxies `/api/*` to the FastAPI container (prefix-stripped) and everything else to `web:80`.

Local dev (no Caddy split, API still on `localhost:8000`):

```bash
cd web
npm install
npm run dev           # http://localhost:4321
```

Point the landing at a running API during dev by editing `StatsStrip.astro`'s fetch URL, or run the whole prod stack via `docker compose -f docker-compose.prod.yml up --build`.

## Make Commands

```bash
make up                             # Start services
make down                            # Stop
make logs                            # Tail logs
make clean                           # Stop + delete volumes
make test                            # Run tests
make health                          # Health check
make backend                         # Run locally (hot-reload)
make shell-api                       # Shell into API container
make shell-db                        # PostgreSQL shell
make deploy VPS=user@host            # Build locally, ship image over SSH, redeploy prod stack
make deploy-check VPS=user@host      # Dry-run summary
```

## Known Limitations

- **Single DuckDB file** — fine for millions of rows, may need sharding at billions. Single-writer is enforced at the Python layer via a process-level lock.
- **Background jobs are in-memory** — the `_JOBS` registry doesn't survive API restarts. Long-running jobs lose their status if the container is restarted mid-flight (the write still completes atomically, you just won't be able to poll the outcome).
- **No point-in-time correctness** — the datalake stores the latest known value per `(instrument, timeframe, timestamp)`. Restatements overwrite. Fine for personal backtests; wrong for regulated contexts.

## Disclaimer

Built in an afternoon out of personal need — I wanted a single place to dump MT5 CSV exports and query them without spinning up a full data warehouse. It has grown a bit since: auto-derivation, backups, transactional ingest, WebSocket auth gating, graceful shutdown, metrics. It works, it's tested (230+ tests), but it's still personal software. If you use it, expect to tweak things to fit your setup.

## License

MIT
