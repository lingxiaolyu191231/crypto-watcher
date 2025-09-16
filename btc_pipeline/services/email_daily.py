#!/usr/bin/env python3
"""
Email the daily signal with descriptive reason in subject and body.
Env:
  DATA_DIR=./data
  EMAIL_TO=...
  SMTP_* vars...
  SUBJECT_PREFIX=[BTC Daily]
"""
import os, pathlib, smtplib, ssl
from email.message import EmailMessage
import pandas as pd

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "./data")).resolve()
WATCH = DATA_DIR / "signal_watchlist.csv"
INDIC = DATA_DIR / "daily_with_indicators.csv"

EMAIL_TO   = os.getenv("EMAIL_TO", "").strip()
EMAIL_FROM = os.getenv("EMAIL_FROM", os.getenv("SMTP_USER","").strip())
SMTP_HOST  = os.getenv("SMTP_HOST", "smtp.gmail.com").strip()
SMTP_PORT  = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER  = os.getenv("SMTP_USER", "").strip()
SMTP_PASS  = os.getenv("SMTP_PASS", "").strip()
PREFIX     = os.getenv("SUBJECT_PREFIX", "[BTC Daily]").strip()

if not WATCH.exists() or not EMAIL_TO:
    raise SystemExit("No watchlist or EMAIL_TO missing; skipping email.")

row = pd.read_csv(WATCH).iloc[0]
asof, symbol, close = row["asof"], row["symbol"], row["close"]
score, action, strat = row.get("score"), row.get("action"), row.get("strategy")

# Grab indicator context
reason_lines, tags = [], []
if INDIC.exists():
    df = pd.read_csv(INDIC)
    last = df.iloc[-1]
    # EMA trend
    if "ema50" in last and "ema200" in last:
        if last["ema50"] > last["ema200"]:
            reason_lines.append("EMA50 above EMA200 (bullish)")
            tags.append("EMA+")
        else:
            reason_lines.append("EMA50 below EMA200 (bearish)")
            tags.append("EMA-")
    # MACD
    if "macd_line" in last and "macd_signal" in last:
        if last["macd_line"] > last["macd_signal"]:
            reason_lines.append("MACD bullish crossover")
            tags.append("MACD+")
        else:
            reason_lines.append("MACD bearish crossover")
            tags.append("MACD-")
    # RSI
    if "rsi14" in last:
        rsi = last["rsi14"]
        if rsi < 30:
            reason_lines.append(f"RSI oversold ({rsi:.1f})")
            tags.append("RSI<30")
        elif rsi > 70:
            reason_lines.append(f"RSI overbought ({rsi:.1f})")
            tags.append("RSI>70")
    # ATR
    if "atr_rel14" in last:
        reason_lines.append(f"ATR%={last['atr_rel14']*100:.2f}")
    # Bollinger
    if "bb_mid20" in last and "bb_up20" in last:
        if last["close"] > last["bb_up20"]:
            reason_lines.append("Price above Bollinger upper band")
            tags.append("BB>Up")
        elif last["close"] < last["bb_mid20"]:
            reason_lines.append("Price below Bollinger mid band")
            tags.append("BB<Mid")

reason = "; ".join(reason_lines) if reason_lines else "n/a"
tag_str = ",".join(tags[:3])  # keep subject concise (max 3 tags)

subject = f"{PREFIX} [{strat}] – {asof} – {action} (score={score}) [{tag_str}]"
body = (
    f"Strategy: {strat}\n"
    f"Date: {asof}\nSymbol: {symbol}\nClose: {close:.2f}\n"
    f"Score: {score}\nAction: {action}\n\n"
    f"Reason: {reason}\n"
)

msg = EmailMessage()
msg["From"], msg["To"], msg["Subject"] = EMAIL_FROM, EMAIL_TO, subject
msg.set_content(body)

with open(WATCH, "rb") as f:
    msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=WATCH.name)

context = ssl.create_default_context()
with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
    server.starttls(context=context)
    if SMTP_USER and SMTP_PASS:
        server.login(SMTP_USER, SMTP_PASS)
    server.send_message(msg)

print(f"Email sent to {EMAIL_TO}: {subject}")

