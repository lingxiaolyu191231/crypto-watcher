#!/usr/bin/env python3
"""
Build the daily watchlist row using the *selected* strategy **with filters enforced**.
- Reads configs.yaml → picks strategy (or SELECTED_STRATEGY env override)
- Loads recent history from data/daily_with_indicators.csv (or data/daily.csv)
- Computes missing indicators/score if needed
- Applies the strategy (ATR gate / Bollinger / sizing) to the last ~120 bars
- Uses the **latest target** (pos/weight) to decide today's action
- Writes: data/signal_watchlist.csv with columns [asof, symbol, close, score, action, strategy]

Env:
  DATA_DIR=./data
  SELECTED_STRATEGY=balanced  # optional override
  SYMBOL=BTCUSDT               # symbol label in output
"""
import os, pathlib, yaml
import pandas as pd
import numpy as np
from services.lib.strategy import apply_strategy

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "./data")).resolve()
SYMBOL   = os.getenv("SYMBOL", "BTCUSDT").strip() or "BTCUSDT"
CFG_PATH = pathlib.Path("services/configs.yaml")
IN_A     = DATA_DIR / "daily_with_indicators.csv"
IN_B     = DATA_DIR / "daily.csv"
OUT      = DATA_DIR / "signal_watchlist.csv"

LOOKBACK_BARS = 120  # small window is enough to evaluate filters robustly

# ---------- Helpers ----------
def ema(s, span): return s.ewm(span=span, adjust=False).mean()

def rsi(s, period=14):
    d = s.diff(); up = d.clip(lower=0); dn = -d.clip(upper=0)
    up = up.ewm(alpha=1/period, adjust=False).mean()
    dn = dn.ewm(alpha=1/period, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    return 100 - (100/(1+rs))

# ---------- Load config ----------
cfg = yaml.safe_load(open(CFG_PATH))
selected = os.getenv("SELECTED_STRATEGY", cfg.get("selected", "balanced"))
params = cfg["strategies"][selected]

# ---------- Load data ----------
if IN_A.exists():
    df = pd.read_csv(IN_A)
elif IN_B.exists():
    df = pd.read_csv(IN_B)
else:
    raise SystemExit(f"Missing input: {IN_A} or {IN_B}")

# Parse & trim
ts_col = "day_start_iso" if "day_start_iso" in df.columns else ("ts" if "ts" in df.columns else None)
if ts_col is None:
    raise SystemExit("No timestamp column found (expected 'day_start_iso' or 'ts').")

df["ts"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
df["close"] = pd.to_numeric(df["close"], errors="coerce")
df = df.dropna(subset=["ts","close"]).sort_values("ts").reset_index(drop=True)

# Keep the last LOOKBACK_BARS for live decision
if len(df) > LOOKBACK_BARS:
    df = df.iloc[-LOOKBACK_BARS:].reset_index(drop=True)

# Ensure indicators/score exist
need = any(c not in df.columns for c in ["ema50","ema200","macd_line","macd_signal","rsi14","score"])
if need:
    df["ema50"] = ema(df["close"], 50)
    df["ema200"] = ema(df["close"], 200)
    ml = ema(df["close"], 12) - ema(df["close"], 26)
    df["macd_line"] = ml
    df["macd_signal"] = ema(ml, 9)
    df["rsi14"] = rsi(df["close"], 14)
    s_ema_cross   = np.where(df["ema50"]>df["ema200"], 1, -1)
    s_price_trend = np.where(df["close"]>df["ema200"], 1, -1)
    s_macd        = np.where(df["macd_line"]>df["macd_signal"], 1, -1)
    s_rsi         = np.where(df["rsi14"]<30, 1, 0)
    s_rsi         = np.where(df["rsi14"]>70, -1, s_rsi)
    df["score"]  = s_ema_cross + s_price_trend + s_macd + s_rsi

# ---------- Apply strategy ----------
#df_strat = apply_strategy(
#    df[[c for c in ["ts","close","score","high","low"] if c in df.columns]].copy(),
#    sell_threshold=params.get("sell_threshold", -3),
#    atr_q=params.get("atr_q", None),
#    boll_k=params.get("boll_k", None),
#    sized=bool(params.get("sized", False))
#)
df_strat = apply_strategy(
    df[[c for c in ["ts","close","score","score_enriched","high","low","adx14","pdi14","mdi14"] if c in df.columns]].copy(),
    sell_threshold=params.get("sell_threshold", -3),
    atr_q=params.get("atr_q", None),
    boll_k=params.get("boll_k", None),
    sized=bool(params.get("sized", False)),
    score_col=params.get("score_col", "score"),
    adx_min=params.get("adx_min", None),
    donchian_n=params.get("donchian_n", None)
)

# --- After df_strat is computed ---

# Which score column actually got used
score_col = params.get("score_col", "score")
if score_col not in df_strat.columns:
    # fallback if enriched isn't present yet
    score_col = "score"

last = df_strat.iloc[-1]

# Prefer weight when sizing is enabled
target_w = float(last.get("target_w", 1.0 if int(last.get("target_pos", 0)) else 0.0))
in_market = target_w > 0.0
action = "STAY_LONG" if in_market else "RISK_OFF (cash)"

asof   = last["ts"].isoformat()
close  = float(last["close"])
scorev = float(last.get(score_col, float("nan")))  # value from the *used* score column

pd.DataFrame([{
    "asof": asof,
    "symbol": SYMBOL,
    "close": close,
    "score_col": score_col,      # <-- NEW: which score column drove the decision
    "score_value": scorev,       # <-- NEW: the numeric score from that column
    "target_weight": target_w,   # <-- NEW: 0, 0.5, 1.0, etc.
    "action": action,
    "strategy": selected
}]).to_csv(OUT, index=False)

print(f"Wrote: {OUT} | strategy={selected} | score_col={score_col} | weight={target_w:.2f} | action={action}")
