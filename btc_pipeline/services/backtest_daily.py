#!/usr/bin/env python3
"""
Runs backtests for all configs in configs.yaml and writes outputs each run.
Inputs: data/daily_with_indicators.csv (or data/daily.csv and it will compute indicators)
Outputs:
  - data/backtests/summary.csv        (one row per strategy)
  - data/backtests/equity_<name>.csv  (per-strategy equity/time series)
  - data/backtests/trades_<name>.csv  (optional, binary position entries/exits)
"""
import os, pathlib, math, yaml
import pandas as pd
import numpy as np
from services.lib.strategy import apply_strategy

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR","./data")).resolve()
IN_A = DATA_DIR / "daily_with_indicators.csv"
IN_B = DATA_DIR / "daily.csv"
OUT_DIR = DATA_DIR / "backtests"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def ema(s, span): return s.ewm(span=span, adjust=False).mean()
def rsi(s, period=14):
    d = s.diff(); up = d.clip(lower=0); dn = -d.clip(upper=0)
    up = up.ewm(alpha=1/period, adjust=False).mean()
    dn = dn.ewm(alpha=1/period, adjust=False).mean()
    rs = up / dn.replace(0, np.nan)
    return 100 - (100/(1+rs))

def load_prices():
    if IN_A.exists():
        df = pd.read_csv(IN_A)
    elif IN_B.exists():
        df = pd.read_csv(IN_B)
    else:
        raise SystemExit("Missing daily CSVs.")

    ts = "day_start_iso" if "day_start_iso" in df.columns else ("ts" if "ts" in df.columns else None)
    if ts is None:
        raise SystemExit("No timestamp column.")
    df["ts"] = pd.to_datetime(df[ts], utc=True, errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["ts","close"]).sort_values("ts").reset_index(drop=True)

    # compute indicators if missing (for raw daily.csv)
    need = any(c not in df.columns for c in ["ema50","ema200","macd_line","macd_signal","rsi14"])
    if need:
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()
        ml = df["close"].ewm(span=12, adjust=False).mean() - df["close"].ewm(span=26, adjust=False).mean()
        df["macd_line"] = ml
        df["macd_signal"] = ml.ewm(span=9, adjust=False).mean()
        # RSI(14)
        d = df["close"].diff()
        up = d.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
        dn = (-d.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
        rs = up / dn.replace(0, np.nan)
        df["rsi14"] = 100 - (100/(1+rs))

    # base score if missing
    if "score" not in df.columns:
        s_ema_cross   = np.where(df["ema50"]>df["ema200"], 1, -1)
        s_price_trend = np.where(df["close"]>df["ema200"], 1, -1)
        s_macd        = np.where(df["macd_line"]>df["macd_signal"], 1, -1)
        s_rsi         = np.where(df["rsi14"]<30, 1, 0)
        s_rsi         = np.where(df["rsi14"]>70, -1, s_rsi)
        df["score"] = s_ema_cross + s_price_trend + s_macd + s_rsi

    # Keep any enriched fields if they’re present in daily_with_indicators.csv
    opt_cols = ["score_enriched", "high", "low", "adx14", "pdi14", "mdi14"]
    keep = ["ts", "close", "score"] + [c for c in opt_cols if c in df.columns]
    return df[keep]

def mdd(series):
    roll_max = series.cummax(); dd = series/roll_max - 1.0
    return float(dd.min())

def ann_return(total_ret, n, ppy=365):
    if n <= 1: return float("nan")
    return (1+total_ret) ** (ppy/n) - 1

def sharpe(returns, ppy=365):
    std = returns.std(ddof=0)
    return float("nan") if std == 0 or np.isnan(std) else (returns.mean()*ppy)/(std*np.sqrt(ppy))

def equity_from_strat(df_strat, start_capital=10000.0):
    strat_eq = (1 + df_strat["strat_ret"].fillna(0)).cumprod()
    bh_eq    = (1 + df_strat["close"].pct_change().fillna(0)).cumprod()
    return start_capital*strat_eq, start_capital*bh_eq

def to_trades(df_strat):
    if "target_pos" not in df_strat.columns:
        # derive binary entries if using weights
        tgt = (df_strat["target_w"] > 0).astype(int)
    else:
        tgt = df_strat["target_pos"]

    pc = tgt.diff().fillna(tgt)
    entries = df_strat.index[pc == 1].tolist()
    exits   = df_strat.index[pc == -1].tolist()
    trades = []
    for ent in entries:
        ex = next((x for x in exits if x > ent), None)
        if ex is None: ex = df_strat.index[-1]
        ep, xp = df_strat.loc[ent,"close"], df_strat.loc[ex,"close"]
        trades.append({
            "entry_ts": df_strat.loc[ent,"ts"],
            "exit_ts": df_strat.loc[ex,"ts"],
            "entry_price": float(ep), "exit_price": float(xp),
            "period_return_%": float((xp/ep-1)*100), "bars_held": int(ex-ent)
        })
    return pd.DataFrame(trades)

def main():
    cfg_path = pathlib.Path("services/configs.yaml")
    with cfg_path.open() as f:
        cfg = yaml.safe_load(f)

    df = load_prices()
    summaries = []
    for name, params in cfg["strategies"].items():
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

        strat_val, bh_val = equity_from_strat(df_strat)
        df_strat = df_strat.assign(strat_value=strat_val, bh_value=bh_val)

        total_ret = df_strat["strat_value"].iloc[-1]/df_strat["strat_value"].iloc[0]-1
        ann = ann_return(total_ret, len(df_strat), 365)
        summary = {
            "name": name,
            "sell_T": params.get("sell_threshold",-3),
            "atr_q": params.get("atr_q", None),
            "boll_k": params.get("boll_k", None),
            "sized": bool(params.get("sized", False)),
            "Total Return %": round(total_ret*100,2),
            "CAGR %": round(ann*100,2) if pd.notna(ann) else float("nan"),
            "MaxDD %": round(mdd(df_strat["strat_value"])*100,2),
            "Sharpe": round(sharpe(df_strat["strat_ret"].fillna(0)),2),
            "Final $": round(float(df_strat["strat_value"].iloc[-1]),2),
            "BH Final $": round(float(df_strat["bh_value"].iloc[-1]),2),
            "BH MaxDD %": round(mdd(df_strat["bh_value"])*100,2),
        }
        summaries.append(summary)

        # Save per-strategy artifacts
        df_strat[["ts","close","strat_value","bh_value"] +
                 [c for c in ["target_pos","target_w"] if c in df_strat.columns]
                ].to_csv(OUT_DIR / f"equity_{name}.csv", index=False)
        to_trades(df_strat).to_csv(OUT_DIR / f"trades_{name}.csv", index=False)

    pd.DataFrame(summaries).to_csv(OUT_DIR / "summary.csv", index=False)
    print(f"Wrote backtest results to {OUT_DIR}/summary.csv")

if __name__ == "__main__":
    main()
