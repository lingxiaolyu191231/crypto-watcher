#!/usr/bin/env python3
"""
Auto-select the best strategy based on recent history and update services/configs.yaml.

- Reads price+score from data/daily_with_indicators.csv (or data/daily.csv) and
  computes missing indicators/score if needed.
- Evaluates every strategy in services/configs.yaml on the last LOOKBACK_DAYS.
- Objective: maximize Sharpe, subject to guardrails:
    * MaxDD <= MAX_DD_CAP (default 0.80 i.e., -80%)
    * min trades >= MIN_TRADES (default 3)
- Hysteresis: only switch if Sharpe improves by ≥ MIN_IMPROVE_PCT (default 10%)
  and ≥ MIN_IMPROVE_ABS (default 0.10).
- Cooldown: do not switch more often than COOLDOWN_DAYS (default 14).
- Writes selection changes to data/backtests/selection_history.csv
- Updates `selected:` field in services/configs.yaml when switching.

Env (optional):
  DATA_DIR=./data
  LOOKBACK_DAYS=365
  MAX_DD_CAP=0.80
  MIN_TRADES=3
  MIN_IMPROVE_PCT=0.10
  MIN_IMPROVE_ABS=0.10
  COOLDOWN_DAYS=14
"""

import os, pathlib, yaml, math, datetime as dt
import pandas as pd
import numpy as np

from services.lib.strategy import apply_strategy

DATA_DIR = pathlib.Path(os.getenv("DATA_DIR","./data")).resolve()
IN_A = DATA_DIR / "daily_with_indicators.csv"
IN_B = DATA_DIR / "daily.csv"
OUT_DIR = DATA_DIR / "backtests"
OUT_DIR.mkdir(parents=True, exist_ok=True)
SEL_HISTORY = OUT_DIR / "selection_history.csv"
CFG_PATH = pathlib.Path("services/configs.yaml")

LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "365"))
MAX_DD_CAP      = float(os.getenv("MAX_DD_CAP", "0.80"))
MIN_TRADES      = int(os.getenv("MIN_TRADES", "3"))
MIN_IMPROVE_PCT = float(os.getenv("MIN_IMPROVE_PCT", "0.10"))
MIN_IMPROVE_ABS = float(os.getenv("MIN_IMPROVE_ABS", "0.10"))
COOLDOWN_DAYS   = int(os.getenv("COOLDOWN_DAYS", "14"))

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

    ts_col = "day_start_iso" if "day_start_iso" in df.columns else ("hour_start_iso" if "hour_start_iso" in df.columns else "ts")
    df["ts"] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["ts","close"]).sort_values("ts").reset_index(drop=True)

    # indicators/score if missing
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
        df["score"] = s_ema_cross + s_price_trend + s_macd + s_rsi

    # preserve optional/enriched columns if present
    opt = ["score_enriched","high","low","adx14","pdi14","mdi14"]
    keep = ["ts","close","score"] + [c for c in opt if c in df.columns]
    return df[keep]

def mdd(series):
    roll_max = series.cummax()
    return float((series/roll_max - 1.0).min())

def sharpe(returns, ppy=365):
    std = returns.std(ddof=0)
    return float("nan") if std == 0 or np.isnan(std) else (returns.mean()*ppy)/(std*np.sqrt(ppy))

def to_trades(df_strat):
    tgt = (df_strat["target_w"] > 0).astype(int) if "target_w" in df_strat.columns else df_strat["target_pos"].astype(int)
    pc = tgt.diff().fillna(tgt)
    entries = df_strat.index[pc == 1].tolist()
    exits   = df_strat.index[pc == -1].tolist()
    trades = []
    for ent in entries:
        ex = next((x for x in exits if x > ent), None)
        if ex is None: ex = df_strat.index[-1]
        trades.append((ent, ex))
    return trades

def main():
    # load cfg + prices
    cfg = yaml.safe_load(open(CFG_PATH))
    df = load_prices()

    # rolling lookback
    end = df["ts"].iloc[-1]
    start = end - pd.Timedelta(days=LOOKBACK_DAYS)
    window = df[df["ts"] >= start].reset_index(drop=True)
    if len(window) < 60:
        print("Not enough data in lookback window; skipping auto-select.")
        return

    # evaluate all
    scores = []
    for name, params in cfg["strategies"].items():
        cols = [c for c in ["ts","close","score","score_enriched","high","low","adx14","pdi14","mdi14"] if c in window.columns]
        # Fallback if requested score_col is missing
        score_col = params.get("score_col", "score")
        if score_col not in cols:
            score_col = "score"

        d = apply_strategy(
            window[cols].copy(),
            sell_threshold=params.get("sell_threshold", -3),
            atr_q=params.get("atr_q", None),
            boll_k=params.get("boll_k", None),
            sized=bool(params.get("sized", False)),
            score_col=score_col,
            adx_min=params.get("adx_min", None),
            donchian_n=params.get("donchian_n", None)
        )

        strat_ret = d["strat_ret"].fillna(0)
        eq = (1 + strat_ret).cumprod()
        sh = sharpe(strat_ret)
        maxdd = abs(mdd(eq))
        ntrades = len(to_trades(d))
        scores.append({"name": name, "Sharpe": sh, "MaxDD": maxdd, "Trades": ntrades})


    df_scores = pd.DataFrame(scores)

    # guardrails
    ok = df_scores[(df_scores["MaxDD"] <= MAX_DD_CAP) & (df_scores["Trades"] >= MIN_TRADES)]
    if ok.empty:
        print("No strategy meets guardrails; keeping current selection.")
        return

    # choose best by Sharpe
    best = ok.sort_values("Sharpe", ascending=False).iloc[0]
    challenger_name = best["name"]; challenger_sharpe = best["Sharpe"]

    current = cfg.get("selected", challenger_name)
    current_row = df_scores[df_scores["name"] == current]
    current_sharpe = float(current_row["Sharpe"].iloc[0]) if not current_row.empty else float("nan")

    # cooldown check
    can_switch = True
    if SEL_HISTORY.exists():
        hist = pd.read_csv(SEL_HISTORY)
        if not hist.empty:
            last_change = pd.to_datetime(hist["changed_at"].iloc[-1], utc=True, errors="coerce")
            if pd.notna(last_change) and (end - last_change) < pd.Timedelta(days=COOLDOWN_DAYS):
                can_switch = False

    # hysteresis decision
    improvement_pct = (challenger_sharpe - current_sharpe) / abs(current_sharpe) if current_row.size > 0 and abs(current_sharpe) > 1e-12 else float("inf")
    improvement_abs = challenger_sharpe - current_sharpe

    should_switch = (
        challenger_name != current and
        can_switch and
        (improvement_pct >= MIN_IMPROVE_PCT or improvement_abs >= MIN_IMPROVE_ABS)
    )

    if should_switch:
        prev = current
        cfg["selected"] = challenger_name
        with open(CFG_PATH, "w") as f:
            yaml.safe_dump(cfg, f, sort_keys=False)
        # log
        rec = {
            "changed_at": end.isoformat(),
            "from": prev,
            "to": challenger_name,
            "prev_sharpe": round(current_sharpe,3),
            "new_sharpe": round(challenger_sharpe,3),
            "lookback_days": LOOKBACK_DAYS,
            "cooldown_days": COOLDOWN_DAYS
        }
        if SEL_HISTORY.exists():
            pd.concat([pd.read_csv(SEL_HISTORY), pd.DataFrame([rec])], ignore_index=True).to_csv(SEL_HISTORY, index=False)
        else:
            pd.DataFrame([rec]).to_csv(SEL_HISTORY, index=False)
        print(f"[AUTO-SELECT] switched {prev} -> {challenger_name} (Sharpe {current_sharpe:.2f} -> {challenger_sharpe:.2f})")
    else:
        print(f"[AUTO-SELECT] keep '{current}'. Best candidate: {challenger_name} (Sharpe={challenger_sharpe:.2f}).")

if __name__ == "__main__":
    main()


