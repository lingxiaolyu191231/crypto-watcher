# =====================
# hype_pipeline Makefile
# =====================
# Repo root: ./projects/hype_pipeline

SHELL := /bin/bash
.ONESHELL:
PYTHON ?= python3

# --- Paths ---
DATA_DIR        := data
HOURLY          := $(DATA_DIR)/hourly.csv
INDICATORS_OUT  := $(DATA_DIR)/hourly_with_indicators_signals.csv
ALERTS_OUT      := $(DATA_DIR)/hype_alerts.csv
WATCHLIST       := $(DATA_DIR)/watchlist.csv

# --- Entry points ---
INGEST_PY       := services/hourly_trade_data/hourly_trade_data.py
INDICATORS_PY   := apps/indicators_with_signals/indicators_with_signals.py
ALERTS_PY       := apps/hype_alerts/hype_alerts.py
EMAIL_PY        := apps/email_alerts/email_watchlist_alert.py

# --- Logging / timestamp ---
LOG_FILE        := hype_pipeline.log
TIMESTAMP       = $$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# --- Email / alerts knobs (can be overridden via env) ---
BUY_THR   ?= -3
SELL_THR  ?= 3
MIN_CONF  ?= 0
LOOKBACKH ?= 24

.PHONY: bootstrap ingest indicators-step watchlist alerts check email \
        pipeline hourly validate help

# ----------------
# Bootstrap venvs
# ----------------
bootstrap:
	@echo "[hype] bootstrapping venvs…"
	@set -e; \
	for d in \
	  services/hourly_trade_data \
	  apps/indicators_with_signals \
	  apps/hype_alerts \
	  apps/watchlist \
	  apps/email_alerts \
	; do \
	  echo "==> $$d"; \
	  cd $$d; \
	  python3 -m venv .venv; \
	  . .venv/bin/activate; \
	  pip install --upgrade pip; \
	  if [ -f requirements.txt ]; then pip install -r requirements.txt; fi; \
	  deactivate; \
	  cd - >/dev/null; \
	done

# ---------------------------
# Explicit pipeline (hourly)
# ---------------------------

# Ingest ONLY closed 1h candles from Hyperliquid /info (idempotent append)
ingest:
	@echo "[$(TIMESTAMP)] [hype] Ingesting *closed* hourly candles via /info…" | tee -a $(LOG_FILE)
	BACKFILL_ONLY=1 STREAM_ONLY=0 VERBOSE=1 \
	COIN=@107 \
	INFO_URL=https://api.hyperliquid.xyz/info \
	$(PYTHON) $(INGEST_PY) --output $(HOURLY)

# Compute indicators + signal scores (also write watchlist via --watchlist)
indicators-step: ingest
	@echo "[$(TIMESTAMP)] [hype] Indicators → $(INDICATORS_OUT)" | tee -a $(LOG_FILE)
	$(PYTHON) $(INDICATORS_PY) \
		--input $(HOURLY) \
		--watchlist $(WATCHLIST) \
		--output $(INDICATORS_OUT)

# (Optional) Mirroring step if you ever want to ensure watchlist == indicators (atomic)
watchlist: indicators-step
	@echo "[$(TIMESTAMP)] [hype] Sync watchlist from indicators (safe)" | tee -a $(LOG_FILE)
	tmp="$(WATCHLIST).tmp.$$"; cp "$(INDICATORS_OUT)" "$$tmp" && mv "$$tmp" "$(WATCHLIST)"

# Build alerts from indicators output (NOT from watchlist)
alerts: indicators-step
	@echo "[$(TIMESTAMP)] [hype] Alerts → $(ALERTS_OUT)" | tee -a $(LOG_FILE)
	$(PYTHON) $(ALERTS_PY) \
		--input $(INDICATORS_OUT) \
		--output $(ALERTS_OUT) \
		--buy-thr -2.75 \
		--sell-thr 0.75 \
		--score-ema-alpha 0.4 \
		--cooldown-hours 12

# Freshness check: watchlist must be at least as fresh as alerts hour
check: $(WATCHLIST) $(ALERTS_OUT)
	@echo "[$(TIMESTAMP)] [hype] Freshness check" | tee -a $(LOG_FILE)
	$(PYTHON) -c "import pandas as pd,sys; w=pd.read_csv('data/watchlist.csv',parse_dates=['hour_start_iso'])['hour_start_iso'].dt.floor('h').max(); a=pd.read_csv('data/hype_alerts.csv',parse_dates=['ts']); a['ts']=pd.to_datetime(a['ts'],utc=True,errors='coerce'); a_hr=a['ts'].dt.floor('h').max(); print('watchlist latest:',w); print('alerts    latest:',a_hr); sys.exit(0 if (pd.notna(w) and pd.notna(a_hr) and w>=a_hr) else 2)"

# Send email: Watchlist (latest hour) + Alerts (last 24h)
email: $(WATCHLIST) $(ALERTS_OUT)
	@echo "[$(TIMESTAMP)] [hype] Email" | tee -a $(LOG_FILE)
	BUY_THR=$(BUY_THR) SELL_THR=$(SELL_THR) ALERT_MIN_CONF=$(MIN_CONF) \
	ALERT_ONLY_LATEST=0 ALERT_LOOKBACK_HOURS=$(LOOKBACKH) \
	INCLUDE_COLUMNS="hour_start_iso,close,signal_score,reasons" \
	INPUT="$(WATCHLIST)" \
	$(PYTHON) $(EMAIL_PY)

# End-to-end CSV pipeline (no email)
pipeline: alerts
	@echo "[$(TIMESTAMP)] [hype] Pipeline complete" | tee -a $(LOG_FILE)

# Full hourly run: ingest → indicators → alerts → check → email
hourly: ingest indicators-step watchlist alerts check email
	@echo "[$(TIMESTAMP)] [hype] Hourly done." | tee -a $(LOG_FILE)


# --------------------
# Sanity validations
# --------------------
validate:
	@echo "[$(TIMESTAMP)] [hype] Validating hourly.csv / indicators / alerts…" | tee -a $(LOG_FILE)
	$(PYTHON) -c "import sys,pandas as pd,pathlib; root=pathlib.Path('.'); h=root/'data/hourly.csv'; \
	               (h.exists() or sys.exit('data/hourly.csv not found')); \
	               df=pd.read_csv(h); col='hour_start_iso' if 'hour_start_iso' in df.columns else ('ts' if 'ts' in df.columns else None); \
	               (col or sys.exit(\"hourly.csv must contain 'hour_start_iso' or 'ts'\")); \
	               ts=pd.to_datetime(df[col], utc=True, errors='raise'); \
	               (ts.dt.minute.eq(0)&ts.dt.second.eq(0)).all() or sys.exit('Non hour-aligned timestamps in hourly.csv'); \
	               ts.duplicated().any() and sys.exit('Duplicate hour rows in hourly.csv'); print('✓ hourly.csv OK')"
	$(PYTHON) -c "import sys,pandas as pd,pathlib; p=pathlib.Path('data/hourly_with_indicators_signals.csv'); \
	               (p.exists() or sys.exit('data/hourly_with_indicators_signals.csv not found')); \
	               w=pd.read_csv(p); need=['sma_200','adx_14','rsi_14','bb_low_20','bb_up_20','signal_score','close']; \
	               miss=[c for c in need if c not in w.columns]; \
	               (not miss or sys.exit(f'Missing indicators: {miss}')); print('✓ indicators file OK')"
	$(PYTHON) -c "import sys,pandas as pd,pathlib; p=pathlib.Path('data/hype_alerts.csv'); \
	               (p.exists() or sys.exit('data/hype_alerts.csv not found')); \
	               a=pd.read_csv(p); need=['buy_alert','sell_alert','alert_confidence','alert_reasons']; \
	               miss=[c for c in need if c not in a.columns]; \
	               (not miss or sys.exit(f'Missing alerts columns: {miss}')); print('✓ alerts file OK'); print('✔ validate: OK')"


# -----------
# CLI helper
# -----------
help:
	@echo "Usage:"
	@echo "  make ingest            # fetch finalized hourly candles → data/hourly.csv"
	@echo "  make indicators-step   # compute indicators/signals (+ write watchlist) → data/hourly_with_indicators_signals.csv"
	@echo "  make alerts            # generate hype alerts → data/hype_alerts.csv"
	@echo "  make hourly            # ingest → indicators → alerts → check → email"
	@echo "  make pipeline          # ingest → indicators → alerts (no email)"
	@echo "  make validate          # sanity-check CSVs"

