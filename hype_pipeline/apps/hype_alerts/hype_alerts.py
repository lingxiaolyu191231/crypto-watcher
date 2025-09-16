"""
hype_alerts.py
---------------
Production-ready alert generator for hype_pipeline.

Features:
- Computes smoothed score, Bollinger %B, bull regime flag.
- Emits buy/sell alerts with confidence and reasons.
- 12h cooldown to reduce alert spam.
- CLI-friendly and importable; optional BigQuery load if configured.

Usage (CLI):
    python hype_alerts.py \
        --input hourly_with_indicators_signals.csv \
        --output hype_alerts.csv \
        --buy-thr -2.75 \
        --sell-thr 0.75 \
        --score-ema-alpha 0.4 \
        --cooldown-hours 12

Optional BigQuery load (set env):
    EXPORT_TO_BQ=true
    BQ_PROJECT=my-project
    BQ_DATASET=hype
    BQ_TABLE=hype_alerts_hourly

As a library:
    from hype_alerts import generate_alerts
    alerts_df = generate_alerts(df, buy_thr=-2.75, sell_thr=0.75)

Requirements (if exporting to BQ):
    pip install google-cloud-bigquery pandas
"""

from __future__ import annotations
import os
import argparse
from typing import Optional, Tuple
import pandas as pd
import numpy as np

BUY_THR_DEFAULT = -2.75
SELL_THR_DEFAULT = 0.75
SCORE_EMA_ALPHA_DEFAULT = 0.4
COOLDOWN_HOURS_DEFAULT = 12

def _ema(series: pd.Series, alpha: float) -> pd.Series:
    return series.ewm(alpha=alpha, adjust=False).mean()

def _compute_bb_pctB(df: pd.DataFrame) -> pd.Series:
    # Handles zeros/NaNs safely
    low = df.get("bb_low_20")
    up  = df.get("bb_up_20")
    if low is None or up is None:
        return pd.Series(np.nan, index=df.index)
    bw = (up - low).replace(0, np.nan)
    return (df["close"] - low) / bw

def _bull_regime(df: pd.DataFrame) -> pd.Series:
    sma200 = df.get("sma_200")
    adx14  = df.get("adx_14")
    cond1 = (df["close"] > sma200) if sma200 is not None else pd.Series(False, index=df.index)
    cond2 = (adx14 >= 20) if adx14 is not None else pd.Series(False, index=df.index)
    return ((cond1.fillna(False)) | (cond2.fillna(False))).astype(int)

def _scaled_exposure(score_s: pd.Series, buy_thr: float, sell_thr: float) -> pd.Series:
    # Linear map: score <= buy_thr -> 1 ; score >= sell_thr -> 0
    return ((sell_thr - score_s) / (sell_thr - buy_thr)).clip(0, 1)

def _apply_cooldown(df: pd.DataFrame, kind: str, cooldown_hours: int) -> None:
    """Suppress duplicate alerts within cooldown_hours for the same symbol & kind."""
    col = f"{kind}_alert"
    if col not in df.columns:
        return
    last_ts = None
    for i in range(len(df)):
        if df.at[i, col] != 1:
            continue
        ts = df.at[i, "ts"]
        if pd.isna(ts):
            continue
        if last_ts is None or (ts - last_ts).total_seconds() / 3600.0 >= cooldown_hours:
            last_ts = ts
        else:
            df.at[i, col] = 0
            df.at[i, "alert_reasons"] = (str(df.at[i, "alert_reasons"]) + "; cooldown").strip("; ")

def _bigquery_load(df: pd.DataFrame) -> None:
    if not (os.getenv("EXPORT_TO_BQ", "false").lower() in ("1","true","yes")):
        return
    project = os.environ["BQ_PROJECT"]
    dataset = os.environ["BQ_DATASET"]
    table   = os.environ["BQ_TABLE"]
    fqtn = f"{project}.{dataset}.{table}" if project else f"{dataset}.{table}"

    from google.cloud import bigquery  # lazy import
    client = bigquery.Client(project=project or None)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    # Convert ts to datetime (BigQuery TIMESTAMP) and booleans to INT64
    tmp = df.copy()
    tmp["buy_alert"] = tmp["buy_alert"].astype("int64")
    tmp["sell_alert"] = tmp["sell_alert"].astype("int64")
    job = client.load_table_from_dataframe(tmp, fqtn, job_config=job_config)
    job.result()

def generate_alerts(
    df: pd.DataFrame,
    buy_thr: float = BUY_THR_DEFAULT,
    sell_thr: float = SELL_THR_DEFAULT,
    score_ema_alpha: float = SCORE_EMA_ALPHA_DEFAULT,
    cooldown_hours: int = COOLDOWN_HOURS_DEFAULT,
    symbol_col: Optional[str] = None,
) -> pd.DataFrame:
    """
    Compute buy/sell alerts for each row.
    If a symbol column is present, cooldown is applied per symbol; otherwise globally.
    """
    out = df.copy()

    # Ensure timestamp
    if "ts" not in out.columns:
        # Try to infer from hour_start_iso
        if "hour_start_iso" in out.columns:
            out["ts"] = pd.to_datetime(out["hour_start_iso"])
        else:
            raise ValueError("No 'ts' or 'hour_start_iso' column found")

    out = out.sort_values("ts").reset_index(drop=True)

    # Needed columns fallback
    for c in ("close","sma_200","adx_14","rsi_14","bb_low_20","bb_up_20","signal_score"):
        if c not in out.columns:
            out[c] = np.nan

    # Derived features
    out["score_smooth"] = _ema(out["signal_score"].astype(float), alpha=score_ema_alpha)
    out["bb_pctB"]      = _compute_bb_pctB(out)
    out["bull_regime"]  = _bull_regime(out)

    # Funding convenience (optional)
    if "funding_rate" in out.columns:
        out["funding_bps"] = out["funding_rate"] * 10000.0
    else:
        out["funding_bps"] = np.nan

    # Scaled exposure (for confidence)
    out["scaled_expo"] = _scaled_exposure(out["score_smooth"], buy_thr, sell_thr)

    # Buy logic
    rsi_ok = (out["rsi_14"] <= 35)
    bb_ok  = (out["bb_pctB"] <= 0.10)
    fund_ok = (out["funding_bps"] < 0) if out["funding_bps"].notna().any() else pd.Series(False, index=out.index)

    buy_core = (out["score_smooth"] <= buy_thr) & (out["bull_regime"] == 1)
    buy_conf_add = rsi_ok.fillna(False).astype(int) + bb_ok.fillna(False).astype(int) + (fund_ok.fillna(False).astype(int))
    out["buy_alert"] = (buy_core & (buy_conf_add >= 1)).astype(int)

    # Sell logic
    sell_rsi = (out["rsi_14"] >= 70)
    sell_bb  = (out["bb_pctB"] >= 0.90)
    sell_core = (out["score_smooth"] >= sell_thr) & (sell_rsi.fillna(False) | sell_bb.fillna(False))
    out["sell_alert"] = sell_core.astype(int)

    # Reasons
    reasons = []
    for i in range(len(out)):
        r = []
        if out.at[i, "buy_alert"] == 1:
            r.append("Score<=buy_thr & bull regime")
            if bool(rsi_ok.iloc[i]) if not pd.isna(out.at[i, "rsi_14"]) else False: r.append("RSI<=35")
            if bool(bb_ok.iloc[i])  if not pd.isna(out.at[i, "bb_pctB"]) else False: r.append("BB%B<=0.10")
            if not pd.isna(out.at[i, "funding_bps"]) and out.at[i, "funding_bps"] < 0: r.append("Funding<0")
        if out.at[i, "sell_alert"] == 1:
            r.append("Score>=sell_thr")
            if bool(sell_rsi.iloc[i]) if not pd.isna(out.at[i, "rsi_14"]) else False: r.append("RSI>=70")
            if bool(sell_bb.iloc[i])  if not pd.isna(out.at[i, "bb_pctB"]) else False: r.append("BB%B>=0.90")
        reasons.append("; ".join(r))
    out["alert_reasons"] = reasons

    # Confidence
    out["alert_confidence"] = 0.0
    out.loc[out["buy_alert"]==1, "alert_confidence"] = (
        (out.loc[out["buy_alert"]==1, "scaled_expo"]*0.6 + (buy_conf_add[out["buy_alert"]==1]/3)*0.4)*100
    )
    out.loc[out["sell_alert"]==1, "alert_confidence"] = (
        ((1-out.loc[out["sell_alert"]==1, "scaled_expo"])*0.6 + 
        ((sell_rsi[out["sell_alert"]==1].fillna(False).astype(int) + sell_bb[out["sell_alert"]==1].fillna(False).astype(int))/2)*0.4)*100
    )

    # Apply cooldown (per symbol if available)
    if symbol_col and symbol_col in out.columns:
        out = out.sort_values([symbol_col, "ts"]).reset_index(drop=True)
        for sym, g in out.groupby(symbol_col, sort=False):
            idx = g.index.tolist()
            _apply_cooldown(out.loc[idx], "buy", COOLDOWN_HOURS_DEFAULT)
            _apply_cooldown(out.loc[idx], "sell", COOLDOWN_HOURS_DEFAULT)
    else:
        _apply_cooldown(out, "buy", COOLDOWN_HOURS_DEFAULT)
        _apply_cooldown(out, "sell", COOLDOWN_HOURS_DEFAULT)

    return out

def _maybe_export_to_bq(df_out: pd.DataFrame) -> None:
    try:
        _bigquery_load(df_out)
    except Exception as e:
        # Don't crash pipeline if BQ export fails; log to stderr
        import sys, traceback
        print(f"[WARN] BigQuery export failed: {e}", file=sys.stderr)
        traceback.print_exc()

def main():
    p = argparse.ArgumentParser(description="Generate buy/sell alerts for hype pipeline")
    p.add_argument("--input", required=True, help="CSV file with hourly indicators (must contain 'ts' or 'hour_start_iso')")
    p.add_argument("--output", required=True, help="Output CSV for alerts")
    p.add_argument("--buy-thr", type=float, default=BUY_THR_DEFAULT)
    p.add_argument("--sell-thr", type=float, default=SELL_THR_DEFAULT)
    p.add_argument("--score-ema-alpha", type=float, default=SCORE_EMA_ALPHA_DEFAULT)
    p.add_argument("--cooldown-hours", type=int, default=COOLDOWN_HOURS_DEFAULT)
    p.add_argument("--symbol-col", type=str, default=None, help="Optional symbol column for per-asset cooldown")
    args = p.parse_args()

    df = pd.read_csv(args.input)
    # Standardize timestamp
    if "ts" not in df.columns and "hour_start_iso" in df.columns:
        df = df.rename(columns={"hour_start_iso":"ts"})
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    alerts = generate_alerts(
        df,
        buy_thr=args.buy_thr,
        sell_thr=args.sell_thr,
        score_ema_alpha=args.score_ema_alpha,
        cooldown_hours=args.cooldown_hours,
        symbol_col=args.symbol_col,
    )

    # Compact output
    keep_cols = ["ts","close","signal_score","score_smooth","rsi_14","bb_pctB","funding_bps","bull_regime",
                 "buy_alert","sell_alert","alert_confidence","alert_reasons"]
    existing = [c for c in keep_cols if c in alerts.columns]
    alerts[existing].to_csv(args.output, index=False)

    # Optional BigQuery write
    _maybe_export_to_bq(alerts[existing])

if __name__ == "__main__":
    main()
