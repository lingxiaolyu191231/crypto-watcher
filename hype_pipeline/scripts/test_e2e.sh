#!/usr/bin/env bash
set -euo pipefail

# --- Discover repo root (this script lives in hype_pipeline/scripts) ---
BASE="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$BASE/hype_pipeline.log"

# --- CLI flags ---
RELAX=0
ONLY_LAST_HOURS="${ONLY_LAST_HOURS:-6}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --relax-thresholds)
      RELAX=1
      shift
      ;;
    --hours)
      ONLY_LAST_HOURS="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1"
      exit 2
      ;;
  esac
done

# --- Load env (export all) ---
if [[ -f "$BASE/.env" ]]; then
  set -a; source "$BASE/.env"; set +a
fi

# --- Optional relaxed thresholds to force visible alerts in tests ---
if [[ "$RELAX" == "1" ]]; then
  export BUY_THR="${BUY_THR:--2.0}"
  export SELL_THR="${SELL_THR:-0.5}"
  export ALERT_MIN_CONF="${ALERT_MIN_CONF:-0}"
  echo "[test] Using relaxed thresholds: BUY_THR=$BUY_THR SELL_THR=$SELL_THR ALERT_MIN_CONF=$ALERT_MIN_CONF"
fi

# --- Short backfill window for fast tests (macOS-friendly) ---
export START_ISO="$(date -u -v-"$ONLY_LAST_HOURS"H '+%Y-%m-%dT%H:00:00Z')"

# --- Run the full pipeline (hourly path) ---
echo "[test] Running pipeline.sh hourly (START_ISO=$START_ISO)"
bash "$BASE/scripts/pipeline.sh" hourly

# --- Choose a Python w/ pandas (email_alerts venv preferred) ---
PY="$BASE/apps/email_alerts/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY="$(command -v python3)"
fi

# Pass BASE to the Python snippet
export HYPE_BASE="$BASE"

# --- Verify latest hour alignment & alerts ---
"$PY" - <<'PY'
import os, sys, pathlib, pandas as pd

BASE = pathlib.Path(os.environ["HYPE_BASE"])
data = BASE / "data"
w_path = data / "watchlist.csv"
a_path = data / "hype_alerts.csv"

def die(msg, code=1):
    print(f"[FAIL] {msg}")
    sys.exit(code)

def ok(msg):
    print(f"[OK] {msg}")

# Files exist
if not w_path.exists(): die(f"Missing {w_path}")
if not a_path.exists(): die(f"Missing {a_path}")

w = pd.read_csv(w_path, parse_dates=["hour_start_iso"])
if w.empty: die("watchlist.csv is empty")
a = pd.read_csv(a_path, parse_dates=["ts"])

w_last = w["hour_start_iso"].dt.floor("h").max()
a_last = pd.to_datetime(a["ts"], utc=True, errors="coerce").dt.floor("h").max()

print(f"[info] Watchlist last: {w_last}")
print(f"[info] Alerts last   : {a_last}")

# Alignment check
if w_last != a_last:
    die(f"Latest hour mismatch: watchlist={w_last} alerts={a_last}", code=2)
ok("Latest hour aligned between watchlist and alerts")

# Did latest hour actually raise an alert (given current thresholds)?
a["ts_hour"] = pd.to_datetime(a["ts"], utc=True, errors="coerce").dt.floor("h")
hits = a[(a["ts_hour"]==a_last) & ((a.get("buy_alert",0)==1) | (a.get("sell_alert",0)==1))].copy()
min_conf = float(os.getenv("ALERT_MIN_CONF","0"))
if "alert_confidence" in a.columns:
    hits = hits[hits["alert_confidence"].fillna(0) >= min_conf]

print(f"[info] Alerts in latest hour: {len(hits)}")
if len(hits):
    cols = [c for c in ["ts","buy_alert","sell_alert","signal_score","alert_confidence","close","alert_reasons"] if c in hits.columns]
    print(hits[cols].to_string(index=False))
else:
    print("[info] No qualifying alerts this hour under current thresholds (subject will NOT include [HYPE Alerts]).")

ok("Smoke test finished")
PY

echo "[test] Done. See $LOG for pipeline details."

