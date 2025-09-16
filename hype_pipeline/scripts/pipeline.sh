#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "$0")/.." && pwd)"

# Load .env (export everything); strip BOM if present
ENV_PATH="$BASE/.env"
if [ -f "$ENV_PATH" ]; then
  set -a
  perl -pe 's/^\xEF\xBB\xBF//' -i "$ENV_PATH" 2>/dev/null || true
  source "$ENV_PATH"
  set +a
fi

# Safe defaults after .env is loaded
LOG_FILE="${LOG_FILE:-$BASE/hype_pipeline.log}"
DATA_DIR="${DATA_DIR:-$BASE/data}"

# Only email on failures by default; set STATUS_ALERTS_ON_SUCCESS=1 to also email on success
STATUS_ALERTS_ON_SUCCESS="${STATUS_ALERTS_ON_SUCCESS:-0}"

# Debug line AFTER LOG_FILE is set
ENV_MD5="$(command -v md5 >/dev/null 2>&1 && md5 -q "$ENV_PATH" || shasum "$ENV_PATH" 2>/dev/null | awk '{print $1}')"
echo "[pipeline] CWD=$PWD BASE=$BASE ENV=$ENV_PATH MD5=${ENV_MD5:-<n/a>} SUBJECT_PREFIX=${SUBJECT_PREFIX:-<unset>} TO=${TO:-<unset>} FROM=${FROM:-<unset>} ALERT_TO=${ALERT_TO:-<unset>} LOG_FILE=$LOG_FILE" >> "$LOG_FILE"

# App/service directories
IND_DIR="$BASE/apps/indicators_with_signals"
ALERTS_DIR="$BASE/apps/hype_alerts"
WL_DIR="$BASE/apps/watchlist"
MAIL_DIR="$BASE/apps/email_alerts"
SVC_DIR="$BASE/services/hourly_trade_data"

# Artifacts
IND_INPUT="$DATA_DIR/hourly.csv"
IND_OUTPUT="$DATA_DIR/hourly_with_indicators_signals.csv"
ALERTS_OUTPUT="$DATA_DIR/hype_alerts.csv"
WL_INPUT="$IND_OUTPUT"
WL_OUTPUT="$DATA_DIR/watchlist.csv"

LOG_FILE="${LOG_FILE:-$BASE/hype_pipeline.log}"

# ---------- helpers: status email ----------
send_status() {
  local status="$1"   # success|failure
  local stage="$2"    # e.g., pipeline|indicators|alerts...
  local started="$3"  # epoch seconds
  # run_status_alert.py uses SMTP_* / ALERT_* env; it is best-effort (won't fail the pipeline)
  if [[ -f "$MAIL_DIR/run_status_alert.py" ]]; then
    ( cd "$MAIL_DIR"
      if [[ -d .venv ]]; then source .venv/bin/activate; fi
      python3 run_status_alert.py \
        --status "$status" \
        --stage "$stage" \
        --log "$LOG_FILE" \
        --duration_sec "$(( $(date +%s) - started ))" || true
      if [[ -d .venv ]]; then deactivate || true; fi
    )
  fi
}

# ---------- steps ----------
run_backfill() {
  cd "$SVC_DIR"
  source .venv/bin/activate
  # Finalized hourly candles only (NO streaming)
  DATA_DIR="$BASE/data" \
  BACKFILL_ONLY=1 \
  STREAM_ONLY=0 \
  VERBOSE="${VERBOSE:-1}" \
  COIN="${COIN:-@107}" \
  INFO_URL="${INFO_URL:-https://api.hyperliquid.xyz/info}" \
  python3 hourly_trade_data.py
  deactivate || true
}

run_indicators() {
  cd "$IND_DIR"
  source .venv/bin/activate
  INPUT="$IND_INPUT" OUTPUT="$IND_OUTPUT" python3 indicators_with_signals.py
  deactivate || true
}

run_alerts() {
  cd "$ALERTS_DIR"
  # If you keep a venv here, use it; otherwise it will run with system/outer venv
  if [[ -d .venv ]]; then source .venv/bin/activate; fi
  python3 hype_alerts.py \
    --input  "$IND_OUTPUT" \
    --output "$ALERTS_OUTPUT" \
    --buy-thr "${BUY_THR:--2.75}" \
    --sell-thr "${SELL_THR:-0.75}" \
    --score-ema-alpha "${SCORE_EMA_ALPHA:-0.4}" \
    --cooldown-hours "${COOLDOWN_HOURS:-12}"
  if [[ -d .venv ]]; then deactivate || true; fi
}

run_watchlist() {
  cd "$WL_DIR"
  source .venv/bin/activate
  SCORE_MIN="${SCORE_MIN:-3}" \
  BEAR_OK="${BEAR_OK:-1}" \
  LIMIT="${LIMIT:-0}" \
  INCLUDE_RSI="${INCLUDE_RSI:-0}" \
  INCLUDE_TREND="${INCLUDE_TREND:-0}" \
  INPUT="$WL_INPUT" OUTPUT="$WL_OUTPUT" python3 indicators_to_watchlist.py
  deactivate || true
}

run_email() {
  cd "$MAIL_DIR"
  source .venv/bin/activate
  SMTP_HOST="${SMTP_HOST:-smtp.gmail.com}" \
  SMTP_PORT="${SMTP_PORT:-587}" \
  SMTP_USER="${SMTP_USER:?missing SMTP_USER}" \
  SMTP_PASS="${SMTP_PASS:?missing SMTP_PASS}" \
  FROM="${FROM:-$SMTP_USER}" \
  TO="${TO:?missing TO}" \
  SUBJECT_PREFIX="${SUBJECT_PREFIX:-[HYPE Watchlist]}" \
  INPUT="$WL_OUTPUT" \
  STATE="${STATE:-$BASE/.watchlist_state.json}" \
  INCLUDE_COLUMNS="${INCLUDE_COLUMNS:-}" \
  python3 email_watchlist_alert.py
  deactivate || true
}

# Optional quick validator (non-fatal)
run_validate() {
  # choose a pandas-enabled python
  PY_VALIDATOR="${PYTHON_VALIDATOR:-$BASE/apps/indicators_with_signals/.venv/bin/python}"
  if [[ ! -x "$PY_VALIDATOR" ]]; then
    PY_VALIDATOR="$BASE/apps/email_alerts/.venv/bin/python"
  fi

  # run the inline validator from the project root so relative paths resolve
  ( cd "$BASE" && "$PY_VALIDATOR" - <<'PY' || true
import pandas as pd, pathlib
root = pathlib.Path(".")
def ok(m): print("[validate]", m)

# hourly.csv hour aligned & unique
try:
  h = pd.read_csv("data/hourly.csv")
  ts = "hour_start_iso" if "hour_start_iso" in h.columns else ("ts" if "ts" in h.columns else None)
  if ts:
    h[ts] = pd.to_datetime(h[ts], utc=True, errors="coerce")
    if h[ts].notna().all() and ((h[ts].dt.minute==0)&(h[ts].dt.second==0)).all() and not h[ts].duplicated().any():
      ok("hourly.csv OK")
except Exception as e:
  print("[validate] hourly.csv check skipped:", e)

# indicators required columns
try:
  w = pd.read_csv("data/hourly_with_indicators_signals.csv")
  need = ["sma_200","adx_14","rsi_14","bb_low_20","bb_up_20","signal_score","close"]
  if all(c in w.columns for c in need): ok("indicators OK")
except Exception as e:
  print("[validate] indicators check skipped:", e)

# alerts required columns
try:
  a = pd.read_csv("data/hype_alerts.csv")
  need_a = ["buy_alert","sell_alert","alert_confidence","alert_reasons"]
  if all(c in a.columns for c in need_a): ok("alerts OK")
except Exception as e:
  print("[validate] alerts check skipped:", e)
PY
  )
}


# ---------- entrypoint with success/failure alerts ----------
CMD="${1:-hourly}"
case "$CMD" in
  backfill)
    ST=$(date +%s)
    trap 'send_status failure backfill $ST; exit 1' ERR
    run_backfill
    [[ "$STATUS_ALERTS_ON_SUCCESS" == "1" ]] && send_status success backfill $ST
    ;;
  indicators)
    ST=$(date +%s)
    trap 'send_status failure indicators $ST; exit 1' ERR
    run_indicators
    [[ "$STATUS_ALERTS_ON_SUCCESS" == "1" ]] && send_status success indicators $ST
    ;;
  alerts)
    ST=$(date +%s)
    trap 'send_status failure alerts $ST; exit 1' ERR
    run_alerts
    [[ "$STATUS_ALERTS_ON_SUCCESS" == "1" ]] && send_status success alerts $ST
    ;;
  watchlist)
    ST=$(date +%s)
    trap 'send_status failure watchlist $ST; exit 1' ERR
    run_watchlist
    [[ "$STATUS_ALERTS_ON_SUCCESS" == "1" ]] && send_status success watchlist $ST
    ;;
  email)
    ST=$(date +%s)
    trap 'send_status failure email $ST; exit 1' ERR
    run_email
    [[ "$STATUS_ALERTS_ON_SUCCESS" == "1" ]] && send_status success email $ST
    ;;
  hourly)
    ST=$(date +%s)
    trap 'echo "[pipeline] FAILED"; send_status failure pipeline $ST; exit 1' ERR
    run_backfill
    run_indicators
    run_alerts
    run_watchlist
    run_email
    run_validate
    echo "[pipeline] SUCCESS"
    [[ "$STATUS_ALERTS_ON_SUCCESS" == "1" ]] && send_status success pipeline $ST
    ;;
  *)
    echo "Usage: $0 {hourly|backfill|indicators|alerts|watchlist|email}"
    exit 1
    ;;
esac
