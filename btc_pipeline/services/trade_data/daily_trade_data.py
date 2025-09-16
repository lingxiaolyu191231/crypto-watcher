#!/usr/bin/env python3
"""
Daily BTC/USDT (or any symbol) OHLCV backfill from Binance REST API.
- Writes CSV with daily candles that can be fed to your indicator/backtest code.
- Resumable: continues from the last day in the output file.
- Robust retries and chunked pagination (Binance cap: 1000 klines per request).

Env vars (with sensible defaults):
  DATA_DIR=./data
  OUT_CSV=${DATA_DIR}/daily.csv
  SYMBOL=BTCUSDT
  INTERVAL=1d
  START_ISO=2017-08-17T00:00:00Z
  VERBOSE=1

Output: ${DATA_DIR}/daily.csv with header:
  day_start_iso,day_start_ms,open,high,low,close,volume,trades_count,quote_volume,vwap
"""
import os, csv, time, pathlib
from datetime import datetime, timezone, timedelta
from typing import List
import requests

BINANCE_API = os.getenv("BINANCE_API", "https://api.binance.com")
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "./data")).resolve()
OUT_CSV = pathlib.Path(os.getenv("OUT_CSV", str(DATA_DIR / "daily.csv"))).resolve()
SYMBOL = os.getenv("SYMBOL", "BTCUSDT").strip().upper()
INTERVAL = os.getenv("INTERVAL", "1d").strip()
START_ISO = os.getenv("START_ISO", "2017-08-17T00:00:00Z").strip()
VERBOSE = os.getenv("VERBOSE", "1") == "1"
LIMIT = 1000

def parse_iso(s: str):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def ts_ms(dt):
    return int(dt.timestamp() * 1000)

def iso_from_ms(ms: int):
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()

def ensure_header():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUT_CSV.exists():
        with OUT_CSV.open("w", newline="") as f:
            csv.writer(f).writerow([
                "day_start_iso","day_start_ms","open","high","low","close",
                "volume","trades_count","quote_volume","vwap"
            ])

def last_day_ms():
    if not OUT_CSV.exists():
        return None
    try:
        with OUT_CSV.open("r") as f:
            last = None
            for line in f: last = line
            if last:
                parts = last.strip().split(",")
                if parts and parts[0] != "day_start_iso":
                    return int(parts[1])
    except Exception:
        return None
    return None

def get_klines(symbol: str, interval: str, start_ms: int, end_ms: int, *, max_retries: int = 6, backoff0: float = 0.6) -> List[list]:
    url = f"{BINANCE_API}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": LIMIT, "startTime": start_ms, "endTime": end_ms}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            if 500 <= r.status_code < 600:
                wait = backoff0 * (2 ** attempt)
                print(f"[BINANCE] {r.status_code}. retry {attempt+1}/{max_retries} in {wait:.1f}s...")
                time.sleep(wait); continue
            r.raise_for_status()
            out = r.json()
            if not isinstance(out, list):
                raise RuntimeError(f"Unexpected Binance response: {out}")
            return out
        except requests.RequestException as e:
            wait = backoff0 * (2 ** attempt)
            print(f"[BINANCE] error: {e}. retry {attempt+1}/{max_retries} in {wait:.1f}s...")
            time.sleep(wait)
    raise RuntimeError("Giving up after repeated Binance failures.")

def backfill_daily():
    ensure_header()
    start_dt = parse_iso(START_ISO)
    now = datetime.now(timezone.utc)
    last = last_day_ms()
    if last is not None:
        start_dt = max(start_dt, datetime.fromtimestamp(last/1000, tz=timezone.utc) + timedelta(days=1))
        if VERBOSE:
            print(f"[BACKFILL] Resuming from {start_dt.isoformat()}")
    s = ts_ms(start_dt); e = ts_ms(now); wrote = 0
    while s < e:
        data = get_klines(SYMBOL, INTERVAL, s, e)
        if not data:
            s += 24*3600*1000; continue
        rows = []
        for k in data:
            otime = int(k[0]); open_, high, low, close = k[1], k[2], k[3], k[4]
            vol = k[5]; close_time = int(k[6]); quote_vol = k[7]; trades = k[8]
            try:
                v = float(vol); qv = float(quote_vol); vwap = (qv / v) if v > 0 else ""
            except Exception: vwap = ""
            rows.append([iso_from_ms(otime), otime, open_, high, low, close, vol, trades, quote_vol, vwap])
        with OUT_CSV.open("a", newline="") as f:
            w = csv.writer(f)
            for row in rows:
                w.writerow(row); wrote += 1
                if VERBOSE and wrote % 200 == 0:
                    print(f"[BACKFILL] wrote {wrote} rows; last {row[0]}")
        s = int(data[-1][0]) + 1
    print(f"[BACKFILL] complete -> {OUT_CSV}")

def main():
    if not SYMBOL: raise SystemExit("SYMBOL must be set (e.g., BTCUSDT)")
    if INTERVAL != "1d": print(f"[WARN] INTERVAL={INTERVAL} (script optimized for 1d)")
    backfill_daily()
if __name__ == "__main__":
    main()

