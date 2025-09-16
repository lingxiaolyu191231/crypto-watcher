# ===== BTC Pipeline (Daily, Binance.US) =====
# Use project venv Python explicitly
PYTHON := /Users/lingxiaolyu/projects/btc_pipeline/.venv/bin/python3
# Bootstrap virtualenv and install requirements
.PHONY: venv
venv:
	/opt/homebrew/bin/python3 -m venv .venv
	. .venv/bin/activate; \
		pip install --upgrade pip; \
		pip install requests pandas numpy matplotlib pyyaml
		
export PYTHONPATH := /Users/lingxiaolyu/projects/btc_pipeline

# Default DATA_DIR if not provided by the shell-sourced .env
DATA_DIR ?= /Users/lingxiaolyu/projects/btc_pipeline/data

# Helper: ensure logs dir exists (used by launchd or manual runs)
.PHONY: ensure_dirs
ensure_dirs:
	mkdir -p $(DATA_DIR)
	mkdir -p logs

# 1) Fetch daily OHLCV (resumable) from Binance.US
.PHONY: daily
daily: ensure_dirs
	BINANCE_API=https://api.binance.us \
	SYMBOL=$(SYMBOL) INTERVAL=1d START_ISO=$(START_ISO) DATA_DIR=$(DATA_DIR) \
	$(PYTHON) services/trade_data/daily_trade_data.py

# 2) Compute indicators + composite score
.PHONY: indicators
indicators: daily
	DATA_DIR=$(DATA_DIR) $(PYTHON) services/indicators_daily.py

# 3) Build the latest-day watchlist / alert CSV (filters enforced)
.PHONY: watchlist
watchlist: indicators
	DATA_DIR=$(DATA_DIR) $(PYTHON) services/watchlist_daily.py

# 4) Send email (only if there's a signal and EMAIL_TO present)
.PHONY: email
email: watchlist
	EMAIL_TO=$(EMAIL_TO) DATA_DIR=$(DATA_DIR) $(PYTHON) services/email_daily.py

# 5) Backtest all strategy configs and write artifacts
.PHONY: backtest
backtest: indicators
	DATA_DIR=$(DATA_DIR) $(PYTHON) services/backtest_daily.py

# 6) Auto-select best strategy on lookback window (updates configs.yaml)
.PHONY: auto_select
auto_select: backtest
	DATA_DIR=$(DATA_DIR) $(PYTHON) services/auto_select_strategy.py

# Convenience: run full pipeline (daily → indicators → watchlist → email → backtest → auto_select)
.PHONY: run
run: daily indicators watchlist email backtest auto_select

# Clean artifacts
.PHONY: clean
clean:
	rm -f $(DATA_DIR)/daily_with_indicators.csv \
	      $(DATA_DIR)/signal_watchlist.csv \
	      $(DATA_DIR)/latest_signal.csv
	rm -rf $(DATA_DIR)/backtests

