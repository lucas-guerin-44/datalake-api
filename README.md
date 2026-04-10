# Datalake API

A containerized REST API for storing and querying financial market data — both OHLC bars and tick-level data. Built with FastAPI, DuckDB for analytical queries, and PostgreSQL for authentication. Includes WebSocket streaming for live-feed simulation.

Designed for personal use by algo traders who need a central store for historical price data imported from CSV exports (MetaTrader 5 and Dukascopy formats auto-detected).

## Architecture

```
  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
  │  CSV / Excel │   │   REST API   │   │  WebSocket   │
  │   uploads    │   │   clients    │   │  consumers   │
  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
         │                  │                   │
         ▼                  ▼                   ▼
       ┌─────────────────────────────────────────┐
       │          FastAPI app (src/api.py)        │
       └───────────────────┬─────────────────────┘
                           │
                     ┌─────┴─────┐
                     ▼           ▼
               ┌──────────┐  ┌────────────┐
               │  DuckDB  │  │ PostgreSQL │
               │ OHLC +   │  │ users &    │
               │ tick data │  │ API keys   │
               └──────────┘  └────────────┘
```

| Component | Purpose |
|-----------|---------|
| **DuckDB** | OHLC bars and tick data in a single embedded file (`datalake/ohlc.duckdb`). Fast columnar scans. |
| **PostgreSQL** | User accounts and API keys. |
| **FastAPI** | REST + WebSocket API for ingestion, querying, streaming, and auth. |
| **Docker Compose** | Orchestrates PostgreSQL + API. DuckDB is bind-mounted. |

## Data Ingestion

### OHLC bars

Export OHLC data from your trading platform as CSV with columns: `timestamp`, `open`, `high`, `low`, `close`. MetaTrader's `<DATE>`, `<TIME>`, `<OPEN>`, `<HIGH>`, `<LOW>`, `<CLOSE>` export format is auto-detected. Excel files (`.xlsx`, `.xls`) also supported.

```bash
# Single file
curl -X POST http://localhost:8000/ingest \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@XAUUSD_M5_20240101_20241231.csv" \
  -F "instrument=XAUUSD" \
  -F "timeframe=M5"

# Batch: place files in staging/ as {INSTRUMENT}_{TIMEFRAME}_*.csv
curl -X POST http://localhost:8000/ingest-batch \
  -H "Authorization: Bearer $TOKEN"
```

### Tick data

Tick CSV files with columns: `timestamp`, `price`, `volume` (optional: `bid`, `ask`). If only `bid`/`ask` are provided, `price` is computed as the mid. Auto-detects MetaTrader tick exports (`<DATE>`, `<BID>`, `<ASK>`, `<LAST>`, `<VOLUME>`) and Dukascopy format (`Gmt time`, `Bid`, `Ask`, `Volume`).

```bash
# Single file
curl -X POST http://localhost:8000/ingest/ticks \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@XAUUSD_ticks_202401.csv" \
  -F "instrument=XAUUSD"

# Batch: place files in staging/ as {INSTRUMENT}_TICK_*.csv
curl -X POST http://localhost:8000/ingest-batch/ticks \
  -H "Authorization: Bearer $TOKEN"
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

## Quick Start

### 1. Configure

```bash
cp .env.example .env
# Change SECRET_KEY:
# python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 2. Start

```bash
docker compose up -d
# or: make up
```

### 3. Register and get a token

```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "email": "admin@example.com", "password": "your-password"}'

curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "your-password"}'

export TOKEN="eyJ..."
```

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
| `POST /auth/register` | No | Create user account |
| `POST /auth/login` | No | Get JWT token |
| `GET /auth/me` | JWT | Current user info |
| `POST /auth/api-keys` | JWT | Create API key |
| `GET /auth/api-keys` | JWT | List API keys |
| `PATCH /auth/api-keys/{id}` | JWT | Update API key |
| `DELETE /auth/api-keys/{id}` | JWT | Revoke API key |
| `GET /query` | Public* | Query OHLC data (cursor-paginated) |
| `GET /download` | Public* | Download OHLC as CSV |
| `GET /ticks` | Public* | Query tick data (cursor-paginated) |
| `GET /ticks/download` | Public* | Download ticks as CSV |
| `GET /catalog` | Public* | Database stats and coverage (OHLC + ticks) |
| `GET /instruments` | Public* | List instruments (OHLC + ticks) |
| `GET /instruments/{symbol}` | Public* | Coverage per timeframe (incl. TICK) |
| `GET /timeframes` | Public* | List timeframes (incl. TICK) |
| `POST /ingest` | Write | Upload OHLC CSV/Excel file |
| `POST /ingest-batch` | Write | Batch import OHLC from folder |
| `POST /ingest/ticks` | Write | Upload tick CSV file |
| `POST /ingest-batch/ticks` | Write | Batch import ticks from folder |
| `WS /ws/ticks` | No | Stream tick data (live-feed replay) |
| `WS /ws/bars` | No | Stream OHLC bars (live-feed replay) |
| `GET /healthcheck` | No | Health check |

*Public when `ALLOW_PUBLIC_READS=true` (default), requires auth when `false`.

**Auth methods:** JWT Bearer (`Authorization: Bearer <token>`) or API key header (`X-API-Key: dk_...`)

**API key scopes:** `read` (query/download), `write` (read + ingest), `admin` (all)

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SECRET_KEY` | — | **Required.** JWT signing key |
| `POSTGRES_PASSWORD` | `datalake` | PostgreSQL password |
| `API_PORT` | `8000` | API port |
| `ALLOW_PUBLIC_READS` | `true` | Public read access |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

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
├── api.py                  # FastAPI entry point
├── config.py               # Environment loading
├── schemas.py              # Pydantic models
├── auth/
│   └── auth.py             # JWT + API key auth
├── core/
│   ├── database.py         # SQLAlchemy models (User, APIKey)
│   ├── datalake.py         # DuckDB operations (OHLC + tick tables)
│   └── pagination.py       # Cursor-based pagination
├── middleware/
│   ├── logging_config.py   # Structured JSON logging
│   └── middleware.py        # Request logging
├── services/
│   ├── pipeline.py         # CSV/Excel ingestion (OHLC + tick)
│   └── validators.py       # Input validation
└── routes/
    ├── auth_routes.py      # /auth/*
    ├── catalog.py          # /catalog
    ├── health.py           # /healthcheck
    ├── ingest.py           # /ingest, /ingest/ticks
    ├── instruments.py      # /instruments, /timeframes
    ├── query.py            # /query, /download, /ticks, /ticks/download
    └── stream.py           # /ws/ticks, /ws/bars (WebSocket streaming)
```

## Make Commands

```bash
make up           # Start services
make down         # Stop
make logs         # Tail logs
make clean        # Stop + delete volumes
make test         # Run tests
make health       # Health check
make backend      # Run locally (hot-reload)
make shell-api    # Shell into API container
make shell-db     # PostgreSQL shell
```

## Known Limitations

- **Single DuckDB file** — fine for millions of rows, may need sharding at billions
- **WebSocket streaming has no auth** — suitable for local/trusted networks, not public-facing without a proxy

## Disclaimer

Built in an afternoon out of personal need — I wanted a single place to dump MT5 CSV exports and query them without spinning up a full data warehouse. It works, it's tested, but it's not battle-hardened. If you use it, expect to tweak things to fit your setup.

## License

MIT
