#!/usr/bin/env python3
"""
Compute DAILY indicators + composite score for BTC (or any symbol) from ${DATA_DIR}/daily.csv.

Inputs
------
- ${DATA_DIR}/daily.csv  (columns expected: day_start_iso, close, [open, high, low, volume, quote_volume, trades_count])

Outputs
-------
- ${DATA_DIR}/daily_with_indicators.csv  (full history with expanded indicators)
- ${DATA_DIR}/latest_signal.csv          (single latest row w/ core columns + score)

Notes
-----
- Keeps your existing composite score logic the same (EMA/Trend/MACD/RSI) for compatibility.
- Adds *expanded* indicators that downstream steps can use (ATR, Bollinger, Keltner, Donchian, ADX, Stoch, MFI, OBV, 52w, RVOL, etc.).
- Gracefully handles missing OHLCV columns (falls back where sensible).
"""
import os
import pathlib
import numpy as np
import pandas as pd

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", "./data")).resolve()
IN_CSV   = DATA_DIR / "daily.csv"
OUT_CSV  = DATA_DIR / "daily_with_indicators.csv"
LATEST   = DATA_DIR / "latest_signal.csv"

if not IN_CSV.exists():
    raise SystemExit(f"Missing input: {IN_CSV}")

df = pd.read_csv(IN_CSV)
# Parse schema
if "day_start_iso" not in df.columns or "close" not in df.columns:
    raise SystemExit("daily.csv must contain 'day_start_iso' and 'close' columns")

df["ts"] = pd.to_datetime(df["day_start_iso"], utc=True, errors="coerce")
df["close"] = pd.to_numeric(df["close"], errors="coerce")
for col in ["open","high","low","volume","quote_volume","trades_count"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.dropna(subset=["ts","close"]).sort_values("ts").reset_index(drop=True)

# ---------- helpers ----------
def ema(s, span):
    return pd.Series(s).ewm(span=span, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def true_range(high, low, close):
    cp = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - cp).abs(),
        (low - cp).abs()
    ], axis=1).max(axis=1)
    return tr

def adx_di(high, low, close, period=14):
    # +DM / -DM
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr = true_range(high, low, close)
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1/period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1/period, adjust=False).mean() / atr
    dx = ( (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan) ) * 100
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    return adx, plus_di, minus_di

def stochastic_kd(high, low, close, k_period=14, d_period=3):
    ll = low.rolling(k_period).min()
    hh = high.rolling(k_period).max()
    k = 100 * (close - ll) / (hh - ll)
    d = k.rolling(d_period).mean()
    return k, d

def mfi(high, low, close, volume, period=14):
    tp = (high + low + close) / 3.0
    mf = tp * volume
    sign = np.where(tp.diff() > 0, 1, np.where(tp.diff() < 0, -1, 0))
    pos_mf = pd.Series(np.where(sign > 0, mf, 0.0))
    neg_mf = pd.Series(np.where(sign < 0, mf, 0.0))
    pmf = pos_mf.rolling(period).sum()
    nmf = neg_mf.rolling(period).sum()
    mr = pmf / nmf.replace(0, np.nan)
    return 100 - (100 / (1 + mr))

def obv(close, volume):
    direction = np.sign(close.diff().fillna(0))
    return (direction * volume.fillna(0)).cumsum()

# Short names for availability
have_high = "high" in df.columns and df["high"].notna().any()
have_low  = "low" in df.columns and df["low"].notna().any()
have_vol  = "volume" in df.columns and df["volume"].notna().any()

H = df["high"] if have_high else df["close"]
L = df["low"]  if have_low  else df["close"]
C = df["close"]
V = df["volume"] if have_vol else pd.Series(0.0, index=df.index)

# ---------- core set (existing) ----------
df["ema50"]  = ema(C, 50)
df["ema200"] = ema(C, 200)
macd_line     = ema(C, 12) - ema(C, 26)
df["macd_line"]   = macd_line
df["macd_signal"] = ema(macd_line, 9)
df["rsi14"]   = rsi(C, 14)

# composite score (unchanged)
s_ema_cross   = np.where(df["ema50"] > df["ema200"], 1, -1)
s_price_trend = np.where(C > df["ema200"], 1, -1)
s_macd        = np.where(df["macd_line"] > df["macd_signal"], 1, -1)
s_rsi         = np.where(df["rsi14"] < 30, 1, 0)
s_rsi         = np.where(df["rsi14"] > 70, -1, s_rsi)
df["score"]  = s_ema_cross + s_price_trend + s_macd + s_rsi

# ---------- expanded indicators ----------
# ATR & Volatility
TR = true_range(H, L, C)
df["atr14"]     = TR.rolling(14).mean()
df["atr_rel14"] = df["atr14"] / C  # relative ATR used by filters

# Bollinger Bands (20,2)
mid20 = C.rolling(20).mean()
std20 = C.rolling(20).std()
df["bb_mid20"] = mid20
df["bb_up20"]  = mid20 + 2*std20
df["bb_lo20"]  = mid20 - 2*std20

# Keltner Channels (EMA20 ± 2*ATR14)
ema20 = ema(C, 20)
df["kc_mid20"] = ema20
df["kc_up20"]  = ema20 + 2*df["atr14"]
df["kc_lo20"]  = ema20 - 2*df["atr14"]

# Donchian (20) + breakout flags
roll_high20 = H.rolling(20).max()
roll_low20  = L.rolling(20).min()
df["don_hi20"], df["don_lo20"] = roll_high20, roll_low20
df["don_break_up"]   = (C > roll_high20.shift(1)).astype(int)
df["don_break_down"] = (C < roll_low20.shift(1)).astype(int)

# ADX / DI (14)
adx14, pdi14, mdi14 = adx_di(H, L, C, 14)
df["adx14"], df["pdi14"], df["mdi14"] = adx14, pdi14, mdi14

# Stochastic (14,3)
sto_k, sto_d = stochastic_kd(H, L, C, 14, 3)
df["stoch_k14"], df["stoch_d3"] = sto_k, sto_d

# Money Flow Index (14) if volume available
if have_vol:
    df["mfi14"] = mfi(H, L, C, V, 14)
else:
    df["mfi14"] = np.nan

# On-Balance Volume
if have_vol:
    df["obv"] = obv(C, V)
else:
    df["obv"] = np.nan

# 52-week range (252 trading days) & z-score(20)
df["hi_252"] = C.rolling(252).max()
df["lo_252"] = C.rolling(252).min()
df["pct_to_hi_252"] = (C / df["hi_252"]) - 1.0
# realized volatility (20d, annualized)
log_ret = np.log(C).diff()
df["rv20"] = log_ret.rolling(20).std() * np.sqrt(365)

# Trend strength proxy: R-squared of 50d EMA slope via 20d rolling correlation
ema50 = df["ema50"]
df["ema50_slope5"] = ema50.diff(5)
df["ema200_slope5"] = df["ema200"].diff(5)

# ---------- finalize ----------
# Keep columns tidy; write out
cols_first = [
    "day_start_iso","ts","open","high","low","close","volume","quote_volume","trades_count",
    "ema50","ema200","macd_line","macd_signal","rsi14","score"
]
extra_cols = [
    "atr14","atr_rel14","bb_mid20","bb_up20","bb_lo20","kc_mid20","kc_up20","kc_lo20",
    "don_hi20","don_lo20","don_break_up","don_break_down","adx14","pdi14","mdi14",
    "stoch_k14","stoch_d3","mfi14","obv","hi_252","lo_252","pct_to_hi_252","rv20",
    "ema50_slope5","ema200_slope5"
]
ordered = [c for c in cols_first if c in df.columns] + [c for c in extra_cols if c in df.columns]
df[ordered].to_csv(OUT_CSV, index=False)

latest = df.iloc[[-1]][[c for c in [
    "day_start_iso","close","ema50","ema200","macd_line","macd_signal","rsi14","score",
    "atr_rel14","bb_mid20","bb_up20","bb_lo20","adx14","stoch_k14","stoch_d3","mfi14"
] if c in df.columns]]
latest.to_csv(LATEST, index=False)

print(f"Wrote: {OUT_CSV}")
print(f"Latest signal: {LATEST}")

from services.lib.scoring import enriched_score

weights = {
    "base": 1.0,       # keep original score
    "don_break": 0.5,  # light trend/breakout boost
    "adx_trend": 0.5,
    "stoch": 0.25,
    "mfi": 0.25,
    "rvol": 0.5
}
df["score_enriched"] = enriched_score(df, w=weights)
