"""
MT5 bridge server - runs INSIDE Wine alongside MetaTrader5.

Pairs with src/services/mt5_client.py on the Linux side.

Setup on the VPS:
    1. Install MT5 under Wine (standard headless Wine install).
    2. Install a Wine-Python (see https://github.com/lucas-eb/wine-python or similar).
    3. Inside Wine: pip install MetaTrader5
    4. Run this script inside Wine (systemd unit recommended):
         wine python scripts/mt5_bridge.py --host 127.0.0.1 --port 18812

The FastAPI container reaches this via host.docker.internal:18812
(configure MT5_BRIDGE_URL in .env).

Stdlib-only on purpose - no extra deps beyond MetaTrader5 itself.
"""
import argparse
import json
import logging
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    import MetaTrader5 as mt5
except ImportError:
    print("FATAL: MetaTrader5 package not installed (this script must run inside Wine-Python)")
    raise

TIMEFRAME_MAP = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1,
    "W1": mt5.TIMEFRAME_W1,
}

log = logging.getLogger("mt5_bridge")


def _ensure_mt5():
    if not mt5.initialize():
        raise RuntimeError(f"mt5.initialize failed: {mt5.last_error()}")


def _parse_iso(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fetch_bars(symbol: str, timeframe: str, start: datetime, end: datetime) -> list:
    tf = TIMEFRAME_MAP.get(timeframe)
    if tf is None:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    if not mt5.symbol_select(symbol, True):
        raise RuntimeError(f"symbol_select({symbol}) failed: {mt5.last_error()}")

    rates = mt5.copy_rates_range(symbol, tf, start, end)
    if rates is None:
        raise RuntimeError(f"copy_rates_range returned None for {symbol}: {mt5.last_error()}")

    out = []
    for r in rates:
        ts = datetime.fromtimestamp(int(r["time"]), tz=timezone.utc)
        out.append({
            "timestamp": ts.isoformat(),
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
        })
    return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

    def _send_json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/ping":
            self._send_json(200, {"status": "ok", "mt5_initialized": bool(mt5.terminal_info())})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/bars":
            self._send_json(404, {"error": "not found"})
            return

        try:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            req = json.loads(body)
            symbol = req["symbol"]
            timeframe = req.get("timeframe", "M1")
            start = _parse_iso(req["start"])
            end = _parse_iso(req["end"])
        except (KeyError, ValueError, json.JSONDecodeError) as e:
            self._send_json(400, {"error": f"bad request: {e}"})
            return

        try:
            _ensure_mt5()
            bars = fetch_bars(symbol, timeframe, start, end)
            self._send_json(200, {"symbol": symbol, "timeframe": timeframe, "bars": bars})
        except Exception as e:
            log.exception("fetch_bars failed")
            self._send_json(500, {"error": str(e)})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18812)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    _ensure_mt5()
    log.info("MT5 initialized. Listening on %s:%d", args.host, args.port)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        mt5.shutdown()


if __name__ == "__main__":
    main()
