#!/usr/bin/env python3
import numpy as np
import pandas as pd

def _bollinger(series, window=20, k=2.0):
    mid = series.rolling(window).mean()
    sd  = series.rolling(window).std()
    return mid, mid + k*sd, mid - k*sd

def apply_strategy(df, *, sell_threshold=-3, atr_q=None, boll_k=None, sized=False):
    """
    Inputs:
      df: pd.DataFrame with columns ['ts','close','score'] (and optionally 'high','low')
    Returns:
      df_out with columns: target_w (float 0..1), target_pos (0/1), strat_ret, etc.
    """
    out = df.copy()
    
    sc = score_col if score_col in out.columns else "score"
    if sc not in out.columns:
        raise ValueError(f"score column '{score_col}' not in dataframe")

    # --- ATR(14) relative ---
    if "high" in out.columns and "low" in out.columns:
        high = pd.to_numeric(out["high"], errors="coerce")
        low  = pd.to_numeric(out["low"], errors="coerce")
    else:
        high = out["close"]; low = out["close"]
    cp = out["close"].shift(1)
    tr = pd.concat([high-low, (high-cp).abs(), (low-cp).abs()], axis=1).max(axis=1)
    out["ATR14"] = tr.rolling(14).mean()
    out["ATR14_rel"] = out["ATR14"] / out["close"]

    above = out["score"] > sell_threshold

    # ATR gate
    if atr_q is None:
        atr_ok = pd.Series(True, index=out.index)
    else:
        q = float(atr_q)
        thr = out["ATR14_rel"].quantile(q)
        atr_ok = out["ATR14_rel"] <= thr
    
    # --- Optional ADX/DI trend gate ---
    if adx_min is not None and {"adx14","pdi14","mdi14"}.issubset(out.columns):
        trend_ok = (out["adx14"] >= float(adx_min)) & (out["pdi14"] > out["mdi14"])
    else:
        trend_ok = pd.Series(True, index=out.index)

    # --- Optional Donchian gate ---
    if donchian_n is not None:
        n = int(donchian_n)
        hi = out["close"].rolling(n).max()
        lo = out["close"].rolling(n).min()
        don_ok = out["close"] >= hi.shift(1)   # require upside breakout to be long
    else:
        don_ok = pd.Series(True, index=out.index)

    # Bollinger buy/sell filter
    if boll_k is None:
        raw_pos = above.astype(int)
    else:
        mid, up, _ = _bollinger(out["close"], 20, float(boll_k))
        buy_cond  = above & (out["close"] <= mid)
        sell_cond = (~above) | (out["close"] >= up)
        pos = 0; pos_list = []
        for i in range(len(out)):
            if pos == 0 and buy_cond.iloc[i]:
                pos = 1
            elif pos == 1 and sell_cond.iloc[i]:
                pos = 0
            pos_list.append(pos)
        raw_pos = pd.Series(pos_list, index=out.index)

    # Position sizing by signal strength
    if sized:
        w = pd.Series(0.0, index=out.index)
        w = np.where(out["score"] > sell_threshold, 0.5, 0.0)   # weak long
        w = np.where(out["score"] >= sell_threshold + 2, 1.0, w) # strong long
        target_w = pd.Series(w, index=out.index) * raw_pos * atr_ok.astype(int)
        out["target_w"] = pd.Series(target_w).shift(1).fillna(0.0)
        out["target_pos"] = (out["target_w"] > 0).astype(int)
    else:
#        target_pos = (raw_pos.astype(bool) & atr_ok).astype(int)
        target_pos = (raw_pos.astype(bool) & atr_ok & trend_ok & don_ok).astype(int)
        out["target_pos"] = target_pos.shift(1).fillna(0).astype(int)
        out["target_w"] = out["target_pos"].astype(float)

    # Returns
    out["ret"] = out["close"].pct_change().fillna(0.0)
    out["strat_ret"] = out["target_w"] * out["ret"]
    return out

