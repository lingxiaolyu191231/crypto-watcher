#!/usr/bin/env python3
import os, numpy as np, pandas as pd
from pathlib import Path

INPUT = os.getenv("INPUT", "data/hourly.csv")
OUTPUT = os.getenv("OUTPUT", "data/hourly_with_indicators_signals.csv")
def ema(series: pd.Series, span: int) -> pd.Series: return series.ewm(span=span, adjust=False).mean()
def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    d=series.diff(); g=np.where(d>0,d,0.0); l=np.where(d<0,-d,0.0)
    ge=pd.Series(g,index=series.index).ewm(alpha=1/period,adjust=False).mean()
    le=pd.Series(l,index=series.index).ewm(alpha=1/period,adjust=False).mean()
    rs=ge/(le.replace(0,np.nan)); return (100-(100/(1+rs))).fillna(0.0)
def atr(df: pd.DataFrame, period:int=14)->pd.Series:
    pc=df['close'].shift(1)
    tr=pd.concat([(df['high']-df['low']).abs(),(df['high']-pc).abs(),(df['low']-pc).abs()],axis=1).max(axis=1)
    return tr.rolling(window=period,min_periods=1).mean()
def obv(close:pd.Series,volume:pd.Series)->pd.Series:
    s=np.sign(close.diff()).fillna(0.0); return (volume*s).cumsum()
def bollinger(s:pd.Series,window:int=20,num_std:float=2.0):
    ma=s.rolling(window=window,min_periods=1).mean(); sd=s.rolling(window=window,min_periods=1).std(ddof=0)
    return ma, ma+num_std*sd, ma-num_std*sd, sd
def rolling_vwap(close:pd.Series, volume:pd.Series, window:int)->pd.Series:
    pv=close*volume; return (pv.rolling(window=window,min_periods=1).sum()/volume.rolling(window=window,min_periods=1).sum()).replace([np.inf,-np.inf],np.nan)
def macd(s:pd.Series,fast:int=12,slow:int=26,signal:int=9):
    ef,es=ema(s,fast),ema(s,slow); m=ef-es; sg=ema(m,signal); return m, sg, m-sg
def main():
    if not Path(INPUT).exists(): raise SystemExit(f"Input CSV not found: {INPUT}")
    df=pd.read_csv(INPUT)
    for c in ["open","high","low","close","volume","trades_count","vwap"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c],errors="coerce")
        
    if "hour_start_iso" in df.columns:
        df["hour_start_iso"] = pd.to_datetime(df["hour_start_iso"], utc=True, errors="coerce")
        df["hour_start_iso"] = df["hour_start_iso"].dt.floor("h")   # <-- add this line
    else:
        # If no column, derive from ms if available
        if "hour_start_ms" in df.columns:
            df["hour_start_iso"] = pd.to_datetime(df["hour_start_ms"], unit="ms", utc=True).dt.floor("h")

    sort_col = "hour_start_ms" if "hour_start_ms" in df.columns else "hour_start_iso"
    df = df.sort_values(sort_col).reset_index(drop=True)
    df["sma_10"]=df["close"].rolling(10,min_periods=1).mean()
    df["sma_20"]=df["close"].rolling(20,min_periods=1).mean()
    df["sma_50"]=df["close"].rolling(50,min_periods=1).mean()
    df["sma_200"]=df["close"].rolling(200,min_periods=5).mean()
    df["ema_12"]=ema(df["close"],12); df["ema_26"]=ema(df["close"],26)
    df["rsi_14"]=rsi(df["close"],14)
    df["macd"],df["macd_signal"],df["macd_hist"]=macd(df["close"],12,26,9)
    bb_mid,bb_up,bb_lo,bb_std=bollinger(df["close"],20,2.0)
    df["bb_mid_20"],df["bb_up_20_2"],df["bb_lo_20_2"],df["bb_std_20"]=bb_mid,bb_up,bb_lo,bb_std
    df["bb_percent_b"]=(df["close"]-df["bb_lo_20_2"])/(df["bb_up_20_2"]-df["bb_lo_20_2"]).replace(0,np.nan)
    # ATR needs high/low; if missing, fill with NaN
    if {"high","low","close"}.issubset(df.columns):
        df["atr_14"] = atr(df,14)
    else:
        df["atr_14"] = pd.Series(np.nan, index=df.index)

    # OBV & VWAP need volume; if missing, fill with NaN
    if "volume" in df.columns:
        df["obv"] = obv(df["close"], df["volume"])
        df["vwap_24h"] = rolling_vwap(df["close"], df["volume"], 24)
        df["vwap_72h"] = rolling_vwap(df["close"], df["volume"], 72)
    else:
        df["obv"] = pd.Series(np.nan, index=df.index)
        df["vwap_24h"] = pd.Series(np.nan, index=df.index)
        df["vwap_72h"] = pd.Series(np.nan, index=df.index)

    df["ret_1h"]=df["close"].pct_change(1); df["ret_24h"]=df["close"].pct_change(24)
    rm=df["close"].rolling(24,min_periods=1).mean(); rs=df["close"].rolling(24,min_periods=1).std(ddof=0)
    df["zscore_24h"]=(df["close"]-rm)/rs.replace(0,np.nan)
    mp=df["macd"].shift(1); sp=df["macd_signal"].shift(1)
    df["macd_bull_cross"]=((mp<sp)&(df["macd"]>=df["macd_signal"])).astype(int)
    df["macd_bear_cross"]=((mp>sp)&(df["macd"]<=df["macd_signal"])).astype(int)
    df["rsi_overbought"]=(df["rsi_14"]>=70).astype(int); df["rsi_oversold"]=(df["rsi_14"]<=30).astype(int)
    df["bb_breakout_up"]=(df["close"]>df["bb_up_20_2"]).astype(int); df["bb_breakout_down"]=(df["close"]<df["bb_lo_20_2"]).astype(int)
    s50p=df["sma_50"].shift(1); s200p=df["sma_200"].shift(1)
    df["golden_cross_50_200"]=((s50p<s200p)&(df["sma_50"]>=df["sma_200"])).astype(int)
    df["death_cross_50_200"]=((s50p>s200p)&(df["sma_50"]<=df["sma_200"])).astype(int)
    df["trend_up"]=(df["ema_12"]>df["ema_26"]).astype(int); df["trend_down"]=(df["ema_12"]<df["ema_26"]).astype(int)
    df["price_above_vwap24h"]=(df["close"]>df["vwap_24h"]).astype(int); df["price_below_vwap24h"]=(df["close"]<df["vwap_24h"]).astype(int)
    df["atr_rising"]=(df["atr_14"]>df["atr_14"].shift(5)).astype(int)
    bull=(df["macd_bull_cross"]+df["bb_breakout_up"]+df["golden_cross_50_200"]+df["trend_up"]+df["price_above_vwap24h"]+(df["rsi_14"].between(30,70)).astype(int))
    bear=(df["macd_bear_cross"]+df["bb_breakout_down"]+df["death_cross_50_200"]+df["trend_down"]+df["price_below_vwap24h"]+df["rsi_overbought"])
    df["signal_score"]=bull-bear
    df.replace([np.inf,-np.inf],np.nan,inplace=True)
    Path(os.path.dirname(OUTPUT) or ".").mkdir(parents=True,exist_ok=True)
    df.to_csv(OUTPUT,index=False)
    print(f"[indicators] wrote -> {OUTPUT} (rows={len(df)})")
if __name__=="__main__": main()
