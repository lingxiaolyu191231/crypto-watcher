#!/usr/bin/env python3
import numpy as np
import pandas as pd

def base_score(df):
    """Your current score for backward compatibility."""
    s_ema_cross   = np.where(df["ema50"] > df["ema200"], 1, -1)
    s_price_trend = np.where(df["close"] > df["ema200"], 1, -1)
    s_macd        = np.where(df["macd_line"] > df["macd_signal"], 1, -1)
    s_rsi         = np.where(df["rsi14"] < 30, 1, 0)
    s_rsi         = np.where(df["rsi14"] > 70, -1, s_rsi)
    return s_ema_cross + s_price_trend + s_macd + s_rsi

def enriched_score(df, w=None):
    """
    Optional expanded score using newly-added indicators.
    w: dict of weights; any key omitted defaults to 0.
    Keys:
      'base', 'don_break', 'adx_trend', 'stoch', 'mfi', 'rvol'
    """
    w = w or {}
    total = np.zeros(len(df), dtype=float)

    # 1) Keep original score as baseline
    total += w.get("base", 1.0) * base_score(df)

    # 2) Donchian breakout bias (+1 up breakout, -1 down breakout)
    if "don_break_up" in df.columns and "don_break_down" in df.columns:
        don = np.where(df["don_break_up"] == 1, 1, 0)
        don = np.where(df["don_break_down"] == 1, -1, don)
        total += w.get("don_break", 0.0) * don

    # 3) Trend strength via ADX (>20 favors trend following)
    if "adx14" in df.columns and "pdi14" in df.columns and "mdi14" in df.columns:
        adx_trend = np.where(df["adx14"] >= 20, np.where(df["pdi14"] > df["mdi14"], 1, -1), 0)
        total += w.get("adx_trend", 0.0) * adx_trend

    # 4) Stochastic (favor entries when K<D and K<20; risk-off when K>80)
    if "stoch_k14" in df.columns and "stoch_d3" in df.columns:
        st_buy = (df["stoch_k14"] < 20) & (df["stoch_k14"] < df["stoch_d3"])
        st_sell = (df["stoch_k14"] > 80)
        st = np.where(st_buy, 1, np.where(st_sell, -1, 0))
        total += w.get("stoch", 0.0) * st

    # 5) Money Flow (buy when MFI<20; sell when >80)
    if "mfi14" in df.columns:
        mfi_s = np.where(df["mfi14"] < 20, 1, np.where(df["mfi14"] > 80, -1, 0))
        total += w.get("mfi", 0.0) * mfi_s

    # 6) Realized vol tilt: avoid top decile RV20
    if "rv20" in df.columns:
        rv_cut = df["rv20"].quantile(0.90)
        rv_s = np.where(df["rv20"] >= rv_cut, -1, 0)
        total += w.get("rvol", 0.0) * rv_s

    return total
