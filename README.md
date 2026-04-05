# OHLC Datalake API

A containerized REST API for storing and querying OHLC (Open, High, Low, Close) financial market data. Built with FastAPI, DuckDB for analytical queries, and PostgreSQL for authentication.

Designed for personal use by algo traders who need a central store for historical price data imported from CSV exports (MetaTrader 5 format auto-detected).

## Architecture

```
  ┌──────────────┐              ┌──────────────┐
  │  CSV / Excel │              │   REST API   │
  │   uploads    │              │   clients    │
  └──────┬───────┘              └──────┬───────┘
         │                             │
         ▼                             │
┌────────────────┐                     │
│  FastAPI app   │◄────────────────────┘
│  (src/api.py)  │
└───────┬────────┘
        │
  ┌─────┴─────┐
  ▼           ▼
┌────────┐  ┌────────────┐
│ DuckDB │  │ PostgreSQL │
│  OHLC  │  │ users &    │
│  data  │  │ API keys   │
└────────┘  └────────────┘
```

| Component | Purpose |
|-----------|---------|
| **DuckDB** | All OHLC data in a single embedded file (`datalake/ohlc.duckdb`). Fast columnar scans. |
| **PostgreSQL** | User accounts and API keys. |
| **FastAPI** | REST API for ingestion, querying, and auth. |
| **Docker Compose** | Orchestrates PostgreSQL + API. DuckDB is bind-mounted. |

## Data Ingestion

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
# Public if ALLOW_PUBLIC_READS=true (default)
curl "http://localhost:8000/query?instrument=XAUUSD&timeframe=M5&start=2024-01-01&end=2024-12-31&limit=100"

# Download as CSV
curl "http://localhost:8000/download?instrument=XAUUSD&timeframe=M5" -o output.csv
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
| `GET /download` | Public* | Download as CSV |
| `GET /catalog` | Public* | Database stats and coverage |
| `GET /instruments` | Public* | List instruments |
| `GET /instruments/{symbol}` | Public* | Coverage per timeframe |
| `GET /timeframes` | Public* | List timeframes |
| `POST /ingest` | Write | Upload CSV/Excel file |
| `POST /ingest-batch` | Write | Batch import from folder |
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
│   ├── datalake.py         # DuckDB operations
│   └── pagination.py       # Cursor-based pagination
├── middleware/
│   ├── logging_config.py   # Structured JSON logging
│   └── middleware.py        # Request logging
├── services/
│   ├── pipeline.py         # CSV/Excel ingestion
│   └── validators.py       # Input validation
└── routes/
    ├── auth_routes.py      # /auth/*
    ├── catalog.py          # /catalog
    ├── health.py           # /healthcheck
    ├── ingest.py           # /ingest
    ├── instruments.py      # /instruments, /timeframes
    └── query.py            # /query, /download
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
- **No volume data** — OHLC only, no tick or volume columns

## Disclaimer

Built in an afternoon out of personal need — I wanted a single place to dump MT5 CSV exports and query them without spinning up a full data warehouse. It works, it's tested, but it's not battle-hardened. If you use it, expect to tweak things to fit your setup.

## License

MIT
