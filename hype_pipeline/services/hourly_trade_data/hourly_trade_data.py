
#!/usr/bin/env python3
"""
Hourly streamer for hype_pipeline

- One-time backfill of 1h OHLCV from /info (candleSnapshot) using COIN (e.g., @107 for HYPE/USDC)
- Live aggregation of trades into 1h buckets via WebSocket (appends to same CSV)
- Writes to ../data/hourly.csv by default (relative to this script), or use DATA_DIR env

Env (read from environment; you can export them via .env + `set -a; source .env` in shell):
  DATA_DIR=../data                          # where hourly.csv lives
  COIN=@107                                 # HYPE/USDC spot pair index (e.g., @107)
  START_ISO=2018-01-01T00:00:00Z            # earliest backfill time
  INFO_URL=https://api.hyperliquid.xyz/info
  WS_URL=wss://api.hyperliquid.xyz/ws
  VERBOSE=1

Run:
  python3 hourly_trade_data.py            # backfills then streams
  BACKFILL_ONLY=1 python3 hourly_trade_data.py
  STREAM_ONLY=1 python3 hourly_trade_data.py
"""
import os, csv, json, time, signal, pathlib
from datetime import datetime, timezone, timedelta
import requests
from websocket import create_connection, WebSocketConnectionClosedException
import contextlib, fcntl

# ---------- Config & Paths ----------
HERE = pathlib.Path(__file__).resolve()
DEFAULT_DATA_DIR = HERE.parent.parent.parent / "data"   # .../hype_pipeline/data
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", str(DEFAULT_DATA_DIR))).resolve()
OUT_CSV = DATA_DIR / "hourly.csv"

COIN = os.getenv("COIN", "@107")  # HYPE spot pair index on mainnet
START_ISO = os.getenv("START_ISO", "2018-01-01T00:00:00Z")
INFO_URL = os.getenv("INFO_URL", "https://api.hyperliquid.xyz/info")
WS_URL = os.getenv("WS_URL", "wss://api.hyperliquid.xyz/ws")
VERBOSE = os.getenv("VERBOSE", "1") == "1"
BACKFILL_ONLY = os.getenv("BACKFILL_ONLY", "0") == "1"
STREAM_ONLY = os.getenv("STREAM_ONLY", "0") == "1"

INTERVAL_MS = 3600000  # 1h
RANGE_STEP_MS = 499 * INTERVAL_MS  # chunk size to respect 500-candle cap

# ---------- Utils ----------
def parse_iso(s):
    return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(timezone.utc)

def ts_ms(dt): return int(dt.timestamp()*1000)
def iso_from_ms(ms): return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()

def ensure_header():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("x", newline="") as f:  # "x" = fail if exists
            csv.writer(f).writerow([
                "hour_start_iso","hour_start_ms","open","high","low","close","volume","trades_count","vwap"
            ])

def last_hour_ms():
    if not OUT_CSV.exists():
        return None
    try:
        with OUT_CSV.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            back = min(size, 65536)  # scan last 64KB for ending lines
            f.seek(size - back)
            tail = f.read().decode("utf-8", errors="ignore").strip().splitlines()
            for line in reversed(tail):
                parts = line.strip().split(",")
                if parts and parts[0] != "hour_start_iso":
                    return int(parts[1])
    except Exception:
        return None
    return None


# ---------- Backfill via /info ----------
def info_candle_snapshot(coin: str, interval: str, start_ms: int, end_ms: int):
    # IMPORTANT: Hyperliquid expects "req" (not "request") for this endpoint.
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,               # e.g., "@107"
            "interval": interval,       # "1h"
            "startTime": int(start_ms),
            "endTime": int(end_ms),
        }
    }
    r = requests.post(INFO_URL, json=payload, timeout=30)
    r.raise_for_status()
    out = r.json()
    if not isinstance(out, list):
        raise RuntimeError(f"Unexpected /info response: {out}")
    return out
    
@contextlib.contextmanager
def file_lock(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    # a+ so the file exists for locking even if missing
    with path.open("a+") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            yield fh
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

def backfill_hourly():
    ensure_header()
    start_dt = parse_iso(START_ISO)
    now = datetime.now(timezone.utc)
    # resume from last line if present
    last_ms = last_hour_ms()
    if last_ms:
        start_dt = max(start_dt, datetime.fromtimestamp(last_ms/1000, tz=timezone.utc) + timedelta(hours=1))
        if VERBOSE: print(f"[BACKFILL] Resuming from {start_dt.isoformat()}")

    s = ts_ms(start_dt); e = ts_ms(now)
    wrote = 0
    while s < e:
        chunk_end = min(s + RANGE_STEP_MS, e)
        data = info_candle_snapshot(COIN, "1h", s, chunk_end)
        data.sort(key=lambda c: c["t"])
        if data:
            data.sort(key=lambda c: int(c["t"]))
            # lock around write; also re-check last existing hour inside the lock
            with file_lock(OUT_CSV):
                existing_last = last_hour_ms()
                with OUT_CSV.open("a", newline="") as f:
                    w = csv.writer(f)
                    for c in data:
                        t = int(c["t"])
                        if existing_last is not None and t <= existing_last:
                            continue  # skip duplicates/older rows
                        row = [iso_from_ms(t), t, c.get("o"), c.get("h"), c.get("l"),
                               c.get("c"), c.get("v"), "", ""]
                        w.writerow(row); wrote += 1
                        if VERBOSE and wrote % 200 == 0:
                            print(f"[BACKFILL] wrote {wrote} rows; last {row[0]}")
            s = int(data[-1]["t"]) + INTERVAL_MS

        else:
            s = chunk_end + 1
        time.sleep(0.15)
    print(f"[BACKFILL] complete -> {OUT_CSV}")

# ---------- Live stream & aggregate trades ----------
def ws_sub(ws, sub): ws.send(json.dumps({"method":"subscribe","subscription":sub}))

def stream_trades_and_aggregate():
    ensure_header()
    # pick initial bucket
    last_ms = last_hour_ms()
    if last_ms is not None:
        bucket_start = last_ms + INTERVAL_MS
    else:
        now = datetime.now(timezone.utc)
        bucket_start = ts_ms(now.replace(minute=0, second=0, microsecond=0))

    agg = None  # dict with open,high,low,close,volume,trades_count,sum_px_sz

    def flush():
        nonlocal agg, bucket_start
        if not agg: return
        vwap = (agg["sum_px_sz"] / agg["volume"]) if agg["volume"] > 0 else ""
        row = [iso_from_ms(bucket_start), bucket_start, agg["open"], agg["high"], agg["low"], agg["close"], agg["volume"], agg["trades_count"], vwap]
        with OUT_CSV.open("a", newline="") as f:
            csv.writer(f).writerow(row)
        if VERBOSE: print("[HOUR CLOSE]", row)

    last_ping = 0
    reconnect = 2
    while True:
        try:
            print(f"[WS] Connecting {WS_URL} ...")
            ws = create_connection(WS_URL, timeout=20)
            print(f"[WS] Subscribing trades for {COIN} ...")
            ws_sub(ws, {"type":"trades","coin":COIN})
            last_ping = time.time()
            reconnect = 2

            while True:
                if time.time() - last_ping > 30:
                    ws.send(json.dumps({"method":"ping"})); last_ping = time.time()
                raw = ws.recv()
                if not raw: continue
                msg = json.loads(raw)
                if msg.get("channel") != "trades": continue
                data = msg.get("data")
                if not isinstance(data, list): continue

                for t in data:
                    t_ms = int(t["time"])
                    px = float(t["px"]); sz = float(t["sz"])

                    while t_ms >= bucket_start + INTERVAL_MS:
                        if agg: flush()
                        bucket_start += INTERVAL_MS
                        agg = None

                    if agg is None:
                        agg = {"open": px, "high": px, "low": px, "close": px, "volume": 0.0, "trades_count": 0, "sum_px_sz": 0.0}

                    agg["high"] = max(agg["high"], px)
                    agg["low"]  = min(agg["low"], px)
                    agg["close"] = px
                    agg["volume"] += sz
                    agg["trades_count"] += 1
                    agg["sum_px_sz"] += px * sz

                    if VERBOSE: print("[TRADE]", iso_from_ms(t_ms), px, sz)

        except (WebSocketConnectionClosedException, ConnectionResetError, TimeoutError, OSError) as e:
            print(f"[WS] Disconnected: {e}. Reconnecting in {reconnect}s...")
            time.sleep(reconnect); reconnect = min(reconnect*2, 60); continue
        except KeyboardInterrupt:
            print("Interrupted by user."); break
        finally:
            try: ws.close()
            except Exception: pass

# ---------- Main ----------
def main():
    ensure_header()
    backfill_hourly()
#    if not STREAM_ONLY: backfill_hourly()
#    if not BACKFILL_ONLY: stream_trades_and_aggregate()

if __name__ == "__main__":
    main()
