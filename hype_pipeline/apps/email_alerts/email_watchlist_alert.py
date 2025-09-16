#!/usr/bin/env python3
"""
email_watchlist_alert.py
------------------------
Sends the watchlist email AND appends a section summarizing BUY/SELL alerts
from data/hype_alerts.csv.

Key behavior:
- Watchlist: by default, show ONLY the latest hour (set WATCHLIST_ONLY_LATEST=0 to show a tail).
- HYPE Alerts: by default, show ONLY the latest hour (set ALERT_ONLY_LATEST=0 to show a tail).
- Subject: adds "[HYPE Alerts]" ONLY if the latest hour actually contains qualifying alerts.

ENV (commonly set via .env and auto-sourced by pipeline.sh):
  SMTP_HOST (default smtp.gmail.com)
  SMTP_PORT (default 587)
  SMTP_USER, SMTP_PASS, SMTP_STARTTLS=1
  FROM, TO
  SUBJECT_PREFIX (default "[HYPE Watchlist]")

  INPUT            path to watchlist.csv   (default "data/watchlist.csv")
  DATA_DIR         defaults to directory of INPUT
  INCLUDE_COLUMNS  comma-separated columns to show for watchlist

  STATE            optional JSON file to dedupe identical emails

  # HYPE alerts controls
  ALERTS_FILE          default "hype_alerts.csv" (resolved under DATA_DIR if relative)
  ALERT_MIN_CONF       default "0"   (only include alerts with confidence >= this)
  ALERT_ONLY_LATEST    default "1"
  ALERT_LOOKBACK_ROWS  default "24"  (used only if ALERT_ONLY_LATEST=0)

  # Watchlist controls
  WATCHLIST_ONLY_LATEST   default "1"
  WATCHLIST_LOOKBACK_ROWS default "24" (used only if ONLY_LATEST=0)
"""
import os, sys, json, smtplib, hashlib
from email.mime.text import MIMEText
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def env(key, default=None):
    return os.getenv(key, default)


def _fmt_table(df, cols=None, max_rows=200):
    if cols:
        keep = [c for c in (c.strip() for c in cols.split(",")) if c in df.columns]
        if keep:
            df = df[keep]
    if len(df) > max_rows:
        df = df.tail(max_rows)
    return df.to_string(index=False, max_colwidth=80)


def _load_watchlist(input_csv: Path) -> str:
    """
    Returns a text table for the watchlist.
    Default: ONLY the latest hour. Use WATCHLIST_ONLY_LATEST=0 to show a recent tail.
    """
    if not input_csv.exists():
        return "(watchlist file not found)"
    try:
        df = pd.read_csv(input_csv)
        if df.empty:
            return "(watchlist is empty)"

        # choose ts column
        ts_col = next((c for c in ["hour_start_iso", "ts", "time"] if c in df.columns), None)
        include_cols = env("INCLUDE_COLUMNS", "")

        if ts_col is None:
            # fallback: last row only
            return _fmt_table(df.tail(1), include_cols, max_rows=1)

        df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")

        only_latest = env("WATCHLIST_ONLY_LATEST", "1").lower() in ("1", "true", "yes")
        if only_latest:
            latest_hour = df[ts_col].dt.floor("h").max()
            latest = df[df[ts_col].dt.floor("h") == latest_hour].copy()
            return _fmt_table(latest if not latest.empty else df.tail(1), include_cols, max_rows=len(latest) or 1)
        else:
            lookback = int(env("WATCHLIST_LOOKBACK_ROWS", "24"))
            return _fmt_table(df.tail(lookback), include_cols, max_rows=lookback)
    except Exception as e:
        return f"(failed to read watchlist: {e})"


def _format_alert_rows(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no matching alerts)"
    out = []
    for _, r in df.iterrows():
        side = "BUY" if int(r.get("buy_alert", 0)) == 1 else "SELL"
        ts = r.get("ts")
        if pd.notna(ts):
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%SZ") if hasattr(ts, "strftime") else str(ts)
        else:
            ts_str = ""
        score = r.get("signal_score", "")
        conf = r.get("alert_confidence", "")
        close = r.get("close", "")
        reasons = r.get("alert_reasons", "")
        extras = []
        if "rsi_14" in r and pd.notna(r["rsi_14"]):
            extras.append(f"RSI={float(r['rsi_14']):.1f}")
        if "bb_pctB" in r and pd.notna(r["bb_pctB"]):
            extras.append(f"%B={float(r['bb_pctB']):.2f}")
        meta = (" | " + " ".join(extras)) if extras else ""
        conf_str = f"{int(conf):d}%" if conf == conf and conf != "" else ""
        score_str = f"{float(score):.2f}" if score == score and score != "" else ""
        close_str = f"{float(close):.2f}" if close == close and close != "" else ""
        out.append(f"{ts_str:20} {side:4} close={close_str:>8} score={score_str:>6} conf={conf_str:>3}  {reasons}{meta}")
    return "\n".join(out)


def _load_hype_alerts(alerts_csv: Path):
    """
    Returns (alerts_block_text, has_latest_alerts: bool, latest_hour_str: str|None)

    has_latest_alerts is True ONLY if the **latest hour** contains at least one
    qualifying alert (buy/sell and meets confidence).
    """
    if not alerts_csv.exists():
        return "(no hype_alerts.csv found yet)", False, None

    try:
        df = pd.read_csv(alerts_csv, parse_dates=["ts"])
    except Exception as e:
        return f"(failed to read hype_alerts.csv: {e})", False, None

    if df.empty:
        return "(no alerts yet)", False, None

    # qualifying alerts
    min_conf = float(env("ALERT_MIN_CONF", "0"))
    mask = (df.get("buy_alert", 0) == 1) | (df.get("sell_alert", 0) == 1)
    if "alert_confidence" in df.columns:
        mask &= df["alert_confidence"].fillna(0) >= min_conf
    hits = df[mask].copy()

    # latest hour based on ALL rows (not just hits)
    ts_hour = pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.floor("h")
    latest_hour = ts_hour.max()
    latest_hour_str = latest_hour.strftime("%Y-%m-%d %H:%MZ") if pd.notna(latest_hour) else None

    # restrict to latest hour if configured
    only_latest = env("ALERT_ONLY_LATEST", "1").lower() in ("1", "true", "yes")
    if only_latest:
        if not hits.empty:
            hits["ts_hour"] = pd.to_datetime(hits["ts"], utc=True, errors="coerce").dt.floor("h")
            latest_hits = hits[hits["ts_hour"] == latest_hour].copy()
        else:
            latest_hits = hits
        has_latest_alerts = not latest_hits.empty
        header = f"=== HYPE Alerts (latest hour: {latest_hour_str}) ==="
        view = latest_hits
    else:
        lookback = int(env("ALERT_LOOKBACK_ROWS", "24"))
        view = hits.tail(lookback)
        has_latest_alerts = not view.empty
        header = "=== HYPE Alerts (recent) ==="

    # choose columns (include RSI/%B if present)
    base_cols = ["ts", "buy_alert", "sell_alert", "signal_score", "alert_confidence", "close", "alert_reasons"]
    if {"rsi_14", "bb_pctB"}.issubset(set(df.columns)):
        cols = base_cols + ["rsi_14", "bb_pctB"]
    else:
        cols = base_cols

    text = _format_alert_rows(view[cols] if not view.empty else view)
    return header + "\n" + text, has_latest_alerts, latest_hour_str


def _build_body(watchlist_txt: str, alerts_block: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    parts = [
        f"Timestamp (UTC): {now}",
        "",
        "=== Watchlist ===",
        watchlist_txt.strip() if watchlist_txt else "(empty)",
        "",
        alerts_block.strip() if alerts_block else "=== HYPE Alerts ===\n(none)",
    ]
    return "\n".join(parts)


def _maybe_skip_by_state(state_path: Path, body: str) -> bool:
    if not state_path:
        return False
    try:
        prev = {}
        if state_path.exists():
            prev = json.loads(state_path.read_text())
        new_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        if prev.get("last_hash") == new_hash:
            return True
        state_path.write_text(json.dumps({"last_hash": new_hash, "updated": datetime.now(timezone.utc).isoformat()}))
        return False
    except Exception:
        return False


def send_email(subject: str, body: str):
    host = env("SMTP_HOST", "smtp.gmail.com")
    port = int(env("SMTP_PORT", "587"))
    user = env("SMTP_USER")
    pwd = env("SMTP_PASS")
    starttls = env("SMTP_STARTTLS", "1") in ("1", "true", "True")

    frm = env("FROM") or user or "noreply@localhost"
    to = env("TO")
    if not to:
        print("[email_watchlist_alert] TO env var required", file=sys.stderr)
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = frm
    msg["To"] = to

    try:
        with smtplib.SMTP(host=host, port=port, timeout=30) as s:
            if starttls:
                s.starttls()
            if user:
                s.login(user, pwd or "")
            s.sendmail(frm, [addr.strip() for addr in to.split(",") if addr.strip()], msg.as_string())
        print("[email_watchlist_alert] sent:", subject)
    except Exception as e:
        print(f"[email_watchlist_alert] failed to send: {e}", file=sys.stderr)
    print(f"[email_watchlist_alert] using SUBJECT_PREFIX={env('SUBJECT_PREFIX')!r} from={frm} to={to}")


def main():
    input_path = Path(env("INPUT", "data/watchlist.csv")).resolve()
    data_dir = Path(env("DATA_DIR", str(input_path.parent))).resolve()
    alerts_file = Path(env("ALERTS_FILE", "hype_alerts.csv"))
    alerts_path = alerts_file if alerts_file.is_absolute() else (data_dir / alerts_file)
    alerts_path = alerts_path.resolve()
    state_path = Path(env("STATE", ""))

    # Build sections
    watchlist_txt = _load_watchlist(input_path)
    alerts_block, has_latest_alerts, latest_hour_str = _load_hype_alerts(alerts_path)
    body = _build_body(watchlist_txt, alerts_block)

    # Subject
    base_subj = env("SUBJECT_PREFIX", "[HYPE Watchlist]")
    suffix_time = latest_hour_str or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%MZ")
    if has_latest_alerts:
        subject = f"{base_subj} [HYPE Alerts] {suffix_time}"
    else:
        subject = f"{base_subj} {suffix_time}"

    # Optional dedupe by content
    if state_path:
        try:
            if _maybe_skip_by_state(state_path, body):
                print("[email_watchlist_alert] no changes since last send; skipping")
                return
        except Exception:
            pass

    send_email(subject, body)


if __name__ == "__main__":
    main()
