# HYPE Pipeline (Hourly data → Indicators → Watchlist → Email)

One repo to run everything end-to-end. Works locally or with cron.

## Quick start
- Put your `hourly_trade_data.py` into `services/hourly_trade_data/` (or symlink).
- `make bootstrap` to create venvs & install deps.
- Copy `.env.example` to `.env` and fill SMTP + options.
- `make run-2h` runs indicators → watchlist → email.
