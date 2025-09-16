# hourly_trade_data.py (hype_pipeline version)

- Writes to `../data/hourly.csv` by default (relative to this file).
- Accepts env vars: `DATA_DIR`, `COIN` (e.g., `@107`), `START_ISO`, `INFO_URL`, `WS_URL`, `VERBOSE`.
- Backfills 1h OHLCV via `/info` `candleSnapshot` (uses `"req"`).
- Streams trades and aggregates into 1h buckets going forward.

## Usage
```bash
# From repo root
export DATA_DIR=./data
export COIN='@107'
python3 services/hourly_trade_data/hourly_trade_data.py
```
