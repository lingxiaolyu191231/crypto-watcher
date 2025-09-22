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


def _load_watchlist(input_csv: Path, force_hour=None) -> str:
    if not input_csv.exists():
        return "(watchlist file not found)"
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        return f"(failed to read watchlist: {e})"

    ts_col = None
    for c in ("hour_start_iso", "ts", "timestamp", "time"):
        if c in df.columns:
            ts_col = c
            break

    include_cols = env("INCLUDE_COLUMNS", "")
    max_rows = int(env("MAX_ROWS", "50"))

    if ts_col is None:
        return _fmt_table(df.tail(max_rows), include_cols)

    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df = df[df[ts_col].notna()].copy()
    if df.empty:
        return "(watchlist has no valid timestamps)"

    target = (force_hour or df[ts_col].dt.floor("h").max())
    latest = df[df[ts_col].dt.floor("h") == target].copy()

    if latest.empty and force_hour is not None:
        return f"(no watchlist rows for {target.strftime('%Y-%m-%d %H:%MZ')})"
    if latest.empty:
        latest = df.sort_values(ts_col).tail(max_rows)

    return _fmt_table(latest, include_cols, max_rows=len(latest))


def _latest_hour_of_watchlist(path):
    df = pd.read_csv(path, parse_dates=["hour_start_iso"])
    return df["hour_start_iso"].dt.floor("h").max() if not df.empty else None


def _latest_hour_of_alerts(path):
    df = pd.read_csv(path, parse_dates=["ts"])
    return pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.floor("h").max() if not df.empty else None


def _format_alert_rows(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no matching alerts)"
    out = []
    for _, r in df.iterrows():
        buy = int(r.get("buy_alert", 0) or 0)
        sell = int(r.get("sell_alert", 0) or 0)
        score = r.get("signal_score", None)

        if buy == 1:
            side = "BUY"
        elif sell == 1:
            side = "SELL"
        elif score is not None and score == score:
            try:
                side = "BUY" if float(score) < 0 else "SELL"
            except Exception:
                side = ""
        else:
            side = ""

        ts = r.get("ts")
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%SZ") if hasattr(ts, "strftime") else (str(ts) if ts is not None else "")

        conf = r.get("alert_confidence", "")
        close = r.get("close", "")
        extras = []
        if "rsi_14" in r and pd.notna(r["rsi_14"]):
            extras.append(f"RSI={float(r['rsi_14']):.1f}")
        if "bb_pctB" in r and pd.notna(r["bb_pctB"]):
            extras.append(f"%B={float(r['bb_pctB']):.2f}")
        meta = (" | " + " ".join(extras)) if extras else ""

        conf_str  = f"{int(conf):d}%"     if conf == conf and conf != "" else ""
        score_str = f"{float(score):.2f}" if score == score and score != "" else ""
        close_str = f"{float(close):.2f}" if close == close and close != "" else ""

        out.append(f"{ts_str:20} {side:4} close={close_str:>8} score={score_str:>6} conf={conf_str:>3}  {r.get('alert_reasons','')}{meta}")
    return "\n".join(out)


def _load_hype_alerts(alerts_csv: Path, force_hour=None):
    """
    Returns (alerts_block_text, has_latest_alerts: bool, latest_hour_str: str|None)
    Qualifying alerts:
      - Flags (buy/sell) OR score thresholds BUY_THR/SELL_THR
      - Apply ALERT_MIN_CONF if present
      - Restrict to latest hour when ALERT_ONLY_LATEST=1 (with sensible fallback)
    """
    if not alerts_csv.exists():
        return "(no hype_alerts.csv found yet)", False, None
    try:
        df = pd.read_csv(alerts_csv, parse_dates=["ts"])
    except Exception as e:
        return f"(failed to read hype_alerts.csv: {e})", False, None
    if df.empty:
        return "(no alerts yet)", False, None

    # Normalize timestamps early
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df["ts_hour"] = df["ts"].dt.floor("h")

    # Build qualifying mask = flags OR thresholds (and confidence)
    min_conf = float(env("ALERT_MIN_CONF", "0"))
    flags = (df.get("buy_alert", 0) == 1) | (df.get("sell_alert", 0) == 1)
    buy_thr = float(env("BUY_THR", "-3"))
    sell_thr = float(env("SELL_THR", "3"))
    scores = pd.to_numeric(df.get("signal_score", float("nan")), errors="coerce")
    thr = (scores <= buy_thr) | (scores >= sell_thr)

    mask = (flags | thr)
    if "alert_confidence" in df.columns:
        mask &= df["alert_confidence"].fillna(0) >= min_conf

    hits = df[mask].copy()
    only_latest = env("ALERT_ONLY_LATEST", "1").lower() in ("1","true","yes")

    # Choose hour to render
    requested_hour = force_hour or df["ts_hour"].max()
    latest_hits_hour = hits["ts_hour"].max() if not hits.empty else None

    # If we want only-latest but nothing at the requested hour, fall back to the most recent hour that HAS hits
    render_hour = requested_hour
    if only_latest and (hits.empty or hits[hits["ts_hour"] == requested_hour].empty):
        render_hour = latest_hits_hour

    if only_latest:
        view = hits[hits["ts_hour"] == render_hour].copy() if not hits.empty else hits
        header_time = render_hour.strftime("%Y-%m-%d %H:%MZ") if pd.notna(render_hour) else "n/a"
        header = f"=== HYPE Alerts (latest hour: {header_time}) ==="
    else:
        lookback = int(env("ALERT_LOOKBACK_ROWS", "24"))
        view = hits.tail(lookback)
        header = "=== HYPE Alerts (recent) ==="

    text = _format_alert_rows(view if not view.empty else view)
    has_latest_alerts = not view.empty
    latest_hour_str = (render_hour.strftime("%Y-%m-%d %H:%MZ") if pd.notna(render_hour) else None)
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

    # Prefer the alerts hour; fall back to watchlist hour
    watch_last = _latest_hour_of_watchlist(input_path) if input_path.exists() else None
    alert_last = _latest_hour_of_alerts(alerts_path) if alerts_path.exists() else None
    target_hour = alert_last or watch_last

    # NEW: honor FORCE_HOUR if provided (e.g., "2025-09-22T02:00:00Z")
    force = env("FORCE_HOUR", "")
    if force:
        try:
            # requires pandas imported as pd at top
            target_hour = pd.to_datetime(force, utc=True, errors="coerce").floor("h").to_pydatetime()
        except Exception:
            pass  # if parse fails, keep the computed target_hour

    # Build sections (SINGLE PASS) pinned to target_hour
    watchlist_txt = _load_watchlist(input_path, force_hour=target_hour)
    alerts_block, has_latest_alerts, latest_hour_str = _load_hype_alerts(alerts_path, force_hour=target_hour)

    # Email body
    body = _build_body(watchlist_txt, alerts_block)

    # Subject time: prefer the actual hour the alerts block rendered (latest_hour_str),
    # else fall back to target_hour (or now if None).
    if latest_hour_str:
        subject_time = latest_hour_str
    elif target_hour is not None:
        subject_time = target_hour.strftime("%Y-%m-%d %H:%MZ")
    else:
        subject_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%MZ")

    base_subj = env("SUBJECT_PREFIX", "[HYPE Watchlist]")
    subject = f"{base_subj} {'[HYPE Alerts] ' if has_latest_alerts else ''}{subject_time}"

    # Optional dedupe
    state_path = Path(env("STATE", ""))
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

