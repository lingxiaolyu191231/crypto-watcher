# CryptoWatcher

CryptoWatcher is a collection of automated crypto data pipelines for monitoring trading signals, computing indicators, and sending alerts.

Currently, the repo includes two pipelines:

- **hype_pipeline** — Ingests hourly candles for Hyperliquid/HYPE, computes indicators, generates buy/sell alerts, and emails watchlist reports.
- **btc_pipeline** — Ingests hourly Bitcoin price data, computes indicators, and can be extended for alerting and strategy backtests.

---

## 🔧 Features

- **Data ingestion**  
  Fetch hourly candles from exchange APIs (Hyperliquid for HYPE, [planned] Coinbase/other APIs for BTC).

- **Indicators & signals**  
  SMA, RSI, Bollinger Bands, ADX, custom scoring logic.

- **Alerts**  
  Threshold-based buy/sell signals. Optional email notifications.

- **Validation**  
  Ensures no duplicate timestamps; data is append-only.

- **Scheduling**  
  Run pipelines hourly via `launchd` (macOS) or cron.

---

## 📂 Repo Structure

.
├── hype_pipeline/ # full HYPE pipeline
│ ├── apps/ # email_alerts, indicators, watchlist
│ ├── data/ # local CSVs (ignored by git)
│ ├── scripts/ # bootstrap + pipeline.sh
│ └── services/ # ingestion code
├── btc_pipeline/ # BTC pipeline (hourly ingestion + indicators)
│ └── services/ # ingestion code
└── README.md # this file


---

## 🚀 Quickstart

Clone and install dependencies:
```bash
git clone https://github.com/<your-username>/cryptowatcher.git
cd cryptowatcher/hype_pipeline
python3 -m venv .venv && source .venv/bin/activate
pip install -r apps/email_alerts/requirements.txt

## 📬 Alerts

Watchlist reports are emailed hourly (latest data only).
HYPE Alerts (buy/sell) are highlighted in subject lines.
Status emails are sent only on failure (success emails disabled by default).

## ⚠️ Notes

Secrets: .env file holds API keys & SMTP credentials. It is git-ignored — create your own based on .env.example.
Append-only: ingestion is safe against duplicates and resumes from the last timestamp.
