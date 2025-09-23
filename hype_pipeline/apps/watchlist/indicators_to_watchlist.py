#!/usr/bin/env python3
import os, pandas as pd, numpy as np
from apps.utils.time_bucket import to_utc_series, hour_bucket
from pathlib import Path

INPUT  = os.getenv("INPUT", "data/hourly_with_indicators_signals.csv")
OUTPUT = os.getenv("OUTPUT", "data/watchlist.csv")
SCORE_MIN = int(os.getenv("SCORE_MIN", "2"))
BEAR_OK   = os.getenv("BEAR_OK", "1") == "1"
LIMIT     = int(os.getenv("LIMIT", "0"))
INCLUDE_RSI   = os.getenv("INCLUDE_RSI", "0") == "1"
INCLUDE_TREND = os.getenv("INCLUDE_TREND", "0") == "1"

KEEP_COLS = [
    "hour_start_iso","hour_start_ms","close","volume",
    "signal_score","rsi_14","macd","macd_signal","macd_hist",
    "bb_percent_b","bb_up_20_2","bb_lo_20_2",
    "vwap_24h","sma_50","sma_200",
    "macd_bull_cross","macd_bear_cross",
    "bb_breakout_up","bb_breakout_down",
    "golden_cross_50_200","death_cross_50_200",
    "trend_up","trend_down",
    "price_above_vwap24h","price_below_vwap24h",
    "rsi_overbought","rsi_oversold",
]

def main():
    if not Path(INPUT).exists():
        raise SystemExit(f"Input not found: {INPUT}")
    df = pd.read_csv(INPUT)
    df = df.sort_values("hour_start_ms")

    bull = (
        (df["signal_score"] >= SCORE_MIN) |
        (df.get("macd_bull_cross", 0) == 1) |
        (df.get("bb_breakout_up", 0) == 1)
    )
    if INCLUDE_RSI:
        bull = bull | (df.get("rsi_oversold", 0) == 1)

    bear = (
        (df["signal_score"] <= -SCORE_MIN) |
        (df.get("macd_bear_cross", 0) == 1)
    )
    if BEAR_OK:
        bear = bear | (df.get("bb_breakout_down", 0) == 1)
        if INCLUDE_RSI:
            bear = bear | (df.get("rsi_overbought", 0) == 1)
    else:
        bear = bear & (False)

    filt = bull | bear

    if INCLUDE_TREND:
        trend_mask = (df.get("trend_up", 0) == 1) | ((df.get("trend_down", 0) == 1) & BEAR_OK)
        filt = filt & trend_mask

    w = df.loc[filt, [c for c in KEEP_COLS if c in df.columns]].copy()
    # ---- Build human-readable reasons for each row ----
    def build_reasons(r):
        reasons = []
        # Bullish
        if r.get("macd_bull_cross", 0) == 1: reasons.append("MACD_Cross_Up")
        if r.get("bb_breakout_up", 0) == 1:   reasons.append("BB_Breakout_Up")
        if r.get("golden_cross_50_200", 0)==1: reasons.append("Golden_Cross")
        if r.get("trend_up", 0) == 1:         reasons.append("Trend_Up")
        if r.get("price_above_vwap24h", 0)==1: reasons.append("Above_VWAP")
        # Bearish (only if BEAR_OK)
        if BEAR_OK:
            if r.get("macd_bear_cross", 0) == 1: reasons.append("MACD_Cross_Down")
            if r.get("bb_breakout_down", 0) == 1: reasons.append("BB_Breakout_Down")
            if r.get("death_cross_50_200", 0) == 1: reasons.append("Death_Cross")
            if r.get("trend_down", 0) == 1:        reasons.append("Trend_Down")
            if r.get("price_below_vwap24h", 0) == 1: reasons.append("Below_VWAP")
        # RSI context
        if r.get("rsi_overbought", 0) == 1: reasons.append("RSI_Overbought")
        if r.get("rsi_oversold", 0) == 1:   reasons.append("RSI_Oversold")
        return ",".join(reasons)

    w["reasons"] = w.apply(build_reasons, axis=1)
    w = w.sort_values("hour_start_ms", ascending=False)

    if LIMIT and LIMIT > 0:
        w = w.head(LIMIT)
    
    Path(os.path.dirname(OUTPUT) or ".").mkdir(parents=True, exist_ok=True)
    w.to_csv(OUTPUT, index=False)
    print(f"[watchlist] wrote -> {OUTPUT} (rows={len(w)})")

if __name__ == "__main__":
    main()
