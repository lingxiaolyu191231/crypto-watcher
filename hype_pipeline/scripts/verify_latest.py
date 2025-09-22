#!/usr/bin/env python3
import os, sys, pathlib, pandas as pd

BASE = pathlib.Path(__file__).resolve().parents[1]
data = BASE / "data"
w_path = data / "watchlist.csv"
a_path = data / "hype_alerts.csv"

def fail(msg, code=1):
    print(f"[FAIL] {msg}")
    sys.exit(code)

def ok(msg):
    print(f"[OK] {msg}")

if not w_path.exists(): fail(f"Missing {w_path}")
if not a_path.exists(): fail(f"Missing {a_path}")

w = pd.read_csv(w_path, parse_dates=["hour_start_iso"])
a = pd.read_csv(a_path, parse_dates=["ts"])
if w.empty: fail("watchlist.csv is empty")

w_last = w["hour_start_iso"].dt.floor("h").max()
a_last = pd.to_datetime(a["ts"], utc=True, errors="coerce").dt.floor("h").max()

print(f"[info] Watchlist last: {w_last}")
print(f"[info] Alerts last   : {a_last}")

if w_last != a_last:
    fail(f"Latest hour mismatch: watchlist={w_last} alerts={a_last}", code=2)
ok("Latest hour aligned")

a["ts_hour"] = pd.to_datetime(a["ts"], utc=True, errors="coerce").dt.floor("h")
hits = a[(a["ts_hour"]==a_last) & ((a.get("buy_alert",0)==1)|(a.get("sell_alert",0)==1))].copy()
if "alert_confidence" in a.columns:
    min_conf = float(os.getenv("ALERT_MIN_CONF","0"))
    hits = hits[hits["alert_confidence"].fillna(0) >= min_conf]

print(f"[info] Alerts in latest hour: {len(hits)}")
if len(hits):
    cols = [c for c in ["ts","buy_alert","sell_alert","signal_score","alert_confidence","close","alert_reasons"] if c in hits.columns]
    print(hits[cols].to_string(index=False))
else:
    print("[info] No qualifying alerts this hour under current thresholds.")
ok("verify_latest complete")

