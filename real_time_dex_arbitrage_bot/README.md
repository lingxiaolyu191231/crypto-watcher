# Real-time DEX Arbitrage Detector (MVP)


An async Python service that scans for 2-leg arbitrage between DEX aggregators (0x ↔ 1inch) on Base and **emails** alerts when **net profit** (after MEV buffer & rough gas) exceeds thresholds.


## Quickstart
1. Clone repo, create virtualenv: `make bootstrap`
2. Copy `.env.example` to `.env` and fill API keys (0x, 1inch) and **SMTP** credentials.
3. Verify token addresses in `configs/tokens.base.yml`.
4. Run observer: `make observe`


## Configuration
- Tokens & pairs in `configs/*.yml`
- Thresholds via env: `MIN_PROFIT_USD`, `MIN_ROI_BPS`, `MEV_BUFFER_BPS`


## Alerts (Email)
Set these in `.env`:
```
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_username # for Gmail, use an App Password
SMTP_PASS=your_app_password
EMAIL_FROM=you@example.com
EMAIL_TO=you@example.com
SMTP_USE_TLS=true
```
- For **Gmail**: enable 2FA, create an **App Password**, use it as `SMTP_PASS`.
- For **AWS SES**: set host to your SES endpoint, user/pass are your SMTP creds.


## Notes
- MVP uses aggregator quotes; add direct pool math (Uniswap v3/Curve) later.
- Gas in USD is set to `0` by default; wire an ETH/USD oracle or RPC to improve accuracy.
- Use per-route empirical gas (e.g., ~250–450k) and a latency guard if you add execution.


## Roadmap
- Direct pool reads for Curve/Uni v3
- Triangular routes
- Paper executor with private relay simulation


---
- MVP uses aggregator quotes; add direct pool math (Uniswap v3/Curve) later.
- Gas in USD is set to `0` by default; wire an ETH/USD oracle or RPC to improve accuracy.
- Use per-route empirical gas (e.g., ~250–450k) and a latency guard if you add execution.


## Roadmap
- Direct pool reads for Curve/Uni v3
- Triangular routes
- Paper executor with private relay simulation
