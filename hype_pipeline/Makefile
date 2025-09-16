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
	
SHELL := /bin/bash
.ONESHELL:

# =====================
# hype_pipeline Makefile
# =====================
# Repo root: ./projects/hype_pipeline
# This file preserves your existing script targets (bootstrap/hourly/backfill)
# and adds explicit pipeline steps (ingest -> indicators -> alerts) plus validate.

PYTHON ?= python3

# --- Paths ---
DATA_DIR        := data
HOURLY          := $(DATA_DIR)/hourly.csv
INDICATORS_OUT  := $(DATA_DIR)/hourly_with_indicators_signals.csv
ALERTS_OUT      := $(DATA_DIR)/hype_alerts.csv
WATCHLIST       := $(DATA_DIR)/watchlist.csv

INGEST_PY       := services/hourly_trade_data/hourly_trade_data.py
INDICATORS_PY   := apps/indicators_with_signals/indicators_with_signals.py
ALERTS_PY       := apps/hype_alerts/hype_alerts.py

LOG_FILE        := hype_pipeline.log
TIMESTAMP       = $$(date -u +"%Y-%m-%dT%H:%M:%SZ")

.PHONY: bootstrap hourly backfill indicators watchlist email \
        ingest alerts pipeline validate help

# --- Your existing script wrappers ---
bootstrap:
	@bash scripts/bootstrap.sh

# Keep calling your pipeline wrapper if you prefer
hourly:
	@bash scripts/pipeline.sh hourly

backfill:
	@bash scripts/pipeline.sh backfill

# --- Explicit pipeline steps (no streaming; finalized-only candles) ---
# Ingest ONLY closed 1h candles from Hyperliquid /info (idempotent append)
ingest:
	@echo "[$(TIMESTAMP)] [hype] Ingesting *closed* hourly candles via /info…" | tee -a $(LOG_FILE)
	BACKFILL_ONLY=1 STREAM_ONLY=0 VERBOSE=1 \
	COIN=@107 \
	INFO_URL=https://api.hyperliquid.xyz/info \
	$(PYTHON) $(INGEST_PY) --output $(HOURLY)

# Compute indicators + signal scores
indicators-step: ingest
	@echo "[$(TIMESTAMP)] [hype] Indicators → $(INDICATORS_OUT)" | tee -a $(LOG_FILE)
	$(PYTHON) $(INDICATORS_PY) \
		--input $(HOURLY) \
		--watchlist $(WATCHLIST) \
		--output $(INDICATORS_OUT)

# Generate hype alerts
alerts: indicators-step
	@echo "[$(TIMESTAMP)] [hype] Alerts → $(ALERTS_OUT)" | tee -a $(LOG_FILE)
	$(PYTHON) $(ALERTS_PY) \
		--input $(INDICATORS_OUT) \
		--output $(ALERTS_OUT) \
		--buy-thr -2.75 \
		--sell-thr 0.75 \
		--score-ema-alpha 0.4 \
		--cooldown-hours 12

# End-to-end local CSV pipeline
pipeline: alerts
	@echo "[$(TIMESTAMP)] [hype] Pipeline complete" | tee -a $(LOG_FILE)

# --- Data quality checks ---
validate:
	@echo "[$(TIMESTAMP)] [hype] Validating hourly.csv / indicators / alerts…" | tee -a $(LOG_FILE)
	@$(PYTHON) - <<'PY'
import sys, pandas as pd, pathlib
root = pathlib.Path(".")
def err(m): print("ERROR:", m); sys.exit(1)

# hourly.csv
hpath = root/"data/hourly.csv"
if not hpath.exists(): err("data/hourly.csv not found")
h = pd.read_csv(hpath)
tsc = "hour_start_iso" if "hour_start_iso" in h.columns else ("ts" if "ts" in h.columns else None)
if tsc is None: err("hourly.csv must contain 'hour_start_iso' or 'ts'")
h[tsc] = pd.to_datetime(h[tsc], utc=True, errors="raise")
if not ((h[tsc].dt.minute==0) & (h[tsc].dt.second==0)).all(): err("Non hour-aligned timestamps in hourly.csv")
if h[tsc].duplicated().any(): err("Duplicate hour rows in hourly.csv")
print("✓ hourly.csv OK")

# indicators
wpath = root/"data/hourly_with_indicators_signals.csv"
if not wpath.exists(): err("data/hourly_with_indicators_signals.csv not found")
w = pd.read_csv(wpath)
need = ["sma_200","adx_14","rsi_14","bb_low_20","bb_up_20","signal_score","close"]
miss = [c for c in need if c not in w.columns]
if miss: err(f"Missing indicators: {miss}")
print("✓ indicators file OK")

# alerts
apath = root/"data/hype_alerts.csv"
if not apath.exists(): err("data/hype_alerts.csv not found")
a = pd.read_csv(apath)
need_a = ["buy_alert","sell_alert","alert_confidence","alert_reasons"]
miss_a = [c for c in need_a if c not in a.columns]
if miss_a: err(f"Missing alerts columns: {miss_a}")
print("✓ alerts file OK")
print("✔ validate: OK")
PY

help:
	@echo "Usage:"
	@echo "  make ingest            # fetch finalized hourly candles → data/hourly.csv"
	@echo "  make indicators-step   # compute indicators/signals → data/hourly_with_indicators_signals.csv"
	@echo "  make alerts            # generate hype alerts → data/hype_alerts.csv"
	@echo "  make pipeline          # ingest → indicators-step → alerts"
	@echo "  make validate          # sanity-check CSVs"
	@echo "  make hourly            # (your existing wrapper: scripts/pipeline.sh hourly)"
	@echo "  make backfill          # (your existing wrapper: scripts/pipeline.sh backfill)"
