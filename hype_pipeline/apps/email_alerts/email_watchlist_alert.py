#!/usr/bin/env python3
"""
email_watchlist_alert.py
------------------------

What it sends:
1) === Watchlist ===
   - Latest hour from your watchlist file, showing hour_start_iso, close, signal_score, reasons
2) === HYPE Alerts (last N h) ===
   - All qualifying alerts in the past ALERT_LOOKBACK_HOURS (default 24h)

Qualifying alert = (buy_alert==1 or sell_alert==1) OR (signal_score crosses BUY/SELL thresholds)
                   AND (if alert_confidence column exists) alert_confidence >= ALERT_MIN_CONF

Env vars (common ones):
  INPUT=path/to/watchlist.csv                  (default: data/watchlist.csv)
  DATA_DIR=/base/dir/for/relative/paths
  ALERTS_FILE=hype_alerts.csv                  (relative to DATA_DIR unless absolute)
  INCLUDE_COLUMNS="hour_start_iso,close,signal_score,reasons"  (watchlist visible cols)

  ALERT_ONLY_LATEST=0                          (set to 0 to use 24h window)
  ALERT_LOOKBACK_HOURS=24
  BUY_THR=-3
  SELL_THR=3
  ALERT_MIN_CONF=0

  SUBJECT_PREFIX="[HYPE Watchlist]"
  STATE=/tmp/watch_email_state.json            (optional dedupe by body hash)

  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_STARTTLS
  FROM, TO

Debug:
  DEBUG=1  -> prints chosen hour & counts
"""

import os, sys, json, smtplib, hashlib
from email.mime.text import MIMEText
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


# ---------- helpers ----------
def env(key: str, default: str | None = None) -> str | None:
    return os.getenv(key, default)


def _fmt_table(df: pd.DataFrame, cols: str | None, max_rows: int = 200) -> str:
    if cols:
        keep = [c.strip() for c in cols.split(",") if c.strip() in df.columns]
        if keep:
            df = df[keep]
    if len(df) > max_rows:
        df = df.tail(max_rows)
    # pretty string; rely on pandas defaults for spacing
    return df.to_string(index=False, max_colwidth=80)


def _latest_hour_of_watchlist(path: Path):
    try:
        df = pd.read_csv(path, parse_dates=["hour_start_iso"])
    except Exception:
        return None
    if df.empty:
        return None
    return df["hour_start_iso"].dt.floor("h").max()


def _latest_hour_of_alerts(path: Path):
    try:
        df = pd.read_csv(path, parse_dates=["ts"])
    except Exception:
        return None
    if df.empty:
        return None
    return pd.to_datetime(df["ts"], utc=True, errors="coerce").dt.floor("h").max()


def _load_watchlist(input_csv: Path, force_hour=None) -> str:
    """Show watchlist rows for the chosen hour (or latest if not forced)."""
    if not input_csv.exists():
        return "(watchlist file not found)"
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        return f"(failed to read watchlist: {e})"

    ts_col = "hour_start_iso" if "hour_start_iso" in df.columns else None
    include_cols = env("INCLUDE_COLUMNS", "hour_start_iso,close,signal_score,reasons")

    if ts_col is None:
        # fallback: just show tail
        return _fmt_table(df.tail(20), include_cols)

    # normalize
    df[ts_col] = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    df = df[df[ts_col].notna()].copy()
    if df.empty:
        return "(watchlist has no valid timestamps)"

    target = (force_hour or df[ts_col].dt.floor("h").max())
    latest = df[df[ts_col].dt.floor("h") == target].copy()

    note = ""
    if latest.empty and force_hour is not None:
        # fallback to most recent available hour in watchlist
        target = df[ts_col].dt.floor("h").max()
        latest = df[df[ts_col].dt.floor("h") == target].copy()
        if not latest.empty:
            note = f"(no watchlist rows at requested hour; showing {target.strftime('%Y-%m-%d %H:%MZ')})\n"

    if latest.empty:
        return f"(no watchlist rows found at any hour)"

    return note + _fmt_table(latest, include_cols, max_rows=len(latest))


def _format_alert_rows(df: pd.DataFrame) -> str:
    if df.empty:
        return "(no matching alerts)"
    out = []
    for _, r in df.iterrows():
        buy = int(r.get("buy_alert", 0) or 0)
        sell = int(r.get("sell_alert", 0) or 0)
        score = r.get("signal_score", None)
        side = ""
        if buy == 1:
            side = "BUY"
        elif sell == 1:
            side = "SELL"
        elif score is not None and score == score:
            try:
                side = "BUY" if float(score) < 0 else "SELL"
            except Exception:
                side = ""

        ts = r.get("ts")
        ts = pd.to_datetime(ts, utc=True, errors="coerce")
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%SZ") if pd.notna(ts) else ""

        conf = r.get("alert_confidence", "")
        close = r.get("close", "")
        reasons = r.get("alert_reasons", "") or r.get("reasons", "")

        extras = []
        if "rsi_14" in r and pd.notna(r["rsi_14"]):
            try:
                extras.append(f"RSI={float(r['rsi_14']):.1f}")
            except Exception:
                pass
        if "bb_pctB" in r and pd.notna(r["bb_pctB"]):
            try:
                extras.append(f"%B={float(r['bb_pctB']):.2f}")
            except Exception:
                pass
        meta = (" | " + " ".join(extras)) if extras else ""

        conf_str  = (f"{int(conf):d}%"     if isinstance(conf, (int, float)) and conf == conf else
                     (f"{int(float(conf))}%" if isinstance(conf, str) and conf.strip() else ""))
        score_str = (f"{float(score):.2f}" if isinstance(score, (int, float)) and score == score else
                     (f"{float(score):.2f}" if isinstance(score, str) and score.strip() else ""))
        close_str = (f"{float(close):.2f}" if isinstance(close, (int, float)) and close == close else
                     (f"{float(close):.2f}" if isinstance(close, str) and close.strip() else ""))

        out.append(f"{ts_str:20} {side:4} close={close_str:>8} score={score_str:>6} conf={conf_str:>4}  {reasons}{meta}")
    return "\n".join(out)


def _load_hype_alerts(alerts_csv: Path, reference_hour=None):
    """
    Returns (alerts_block_text, has_alerts: bool, reference_hour_str: str|None)

    Behavior (last-24h window):
      - Qualify rows by flags OR thresholds, with optional confidence
      - Show all qualifying rows whose ts >= (reference_hour - ALERT_LOOKBACK_HOURS)
      - Header reads: '=== HYPE Alerts (last Xh) ==='
    """
    lookback_hours = int(env("ALERT_LOOKBACK_HOURS", "24"))
    if not alerts_csv.exists():
        return "(no hype_alerts.csv found yet)", False, None

    try:
        df = pd.read_csv(alerts_csv, parse_dates=["ts"])
    except Exception as e:
        return f"(failed to read hype_alerts.csv: {e})", False, None
    if df.empty:
        return "(no alerts yet)", False, None

    # normalize timestamps and compute hour bucket
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df[df["ts"].notna()].copy()
    if df.empty:
        return "(no alerts yet)", False, None
    df["ts_hour"] = df["ts"].dt.floor("h")

    # thresholds + flags
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

    # pick reference hour for window end
    ref = reference_hour or df["ts_hour"].max()
    ref_str = ref.strftime("%Y-%m-%d %H:%MZ") if pd.notna(ref) else None
    cutoff = (ref - pd.Timedelta(hours=lookback_hours)) if pd.notna(ref) else None

    if cutoff is not None:
        view = hits[hits["ts"] >= cutoff].copy()
    else:
        view = hits.copy()

    header = f"=== HYPE Alerts (last {lookback_hours}h) ==="
    text = _format_alert_rows(view if not view.empty else view)
    has_alerts = not view.empty
    return header + "\n" + text, has_alerts, ref_str


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
    DEBUG = env("DEBUG", "0").lower() in ("1", "true", "yes")

    # paths
    input_path = Path(env("INPUT", "data/watchlist.csv")).resolve()
    data_dir = Path(env("DATA_DIR", str(input_path.parent))).resolve()
    alerts_file = Path(env("ALERTS_FILE", "hype_alerts.csv"))
    alerts_path = alerts_file if alerts_file.is_absolute() else (data_dir / alerts_file)
    alerts_path = alerts_path.resolve()

    # reference hour (alerts preferred, fallback to watchlist)
    watch_last = _latest_hour_of_watchlist(input_path) if input_path.exists() else None
    alert_last = _latest_hour_of_alerts(alerts_path) if alerts_path.exists() else None
    reference_hour = alert_last or watch_last

    # allow a manual pin (optional)
    force = env("FORCE_HOUR", "")
    if force:
        try:
            reference_hour = pd.to_datetime(force, utc=True, errors="coerce").floor("h").to_pydatetime()
        except Exception:
            pass

    if DEBUG:
        print(f"[debug] watch_last={watch_last} alert_last={alert_last} reference_hour={reference_hour}")

    # build sections
    watchlist_txt = _load_watchlist(input_path, force_hour=None)
    alerts_block, has_alerts, ref_hour_str = _load_hype_alerts(alerts_path, reference_hour=reference_hour)

    body = _build_body(watchlist_txt, alerts_block)

    # subject: include [HYPE Alerts] iff 24h window has hits; stamp with reference_hour
    base_subj = env("SUBJECT_PREFIX", "[HYPE Watchlist]")
    subject_time = (ref_hour_str or
                    (reference_hour.strftime("%Y-%m-%d %H:%MZ") if reference_hour is not None
                     else datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%MZ")))
    subject = f"{base_subj} {'[HYPE Alerts] ' if has_alerts else ''}{subject_time}"

    # optional dedupe
    state_path = Path(env("STATE", ""))
    if state_path:
        try:
            if _maybe_skip_by_state(state_path, body):
                print("[email_watchlist_alert] no changes since last send; skipping")
                return
        except Exception:
            pass

    if DEBUG:
        print(f"[debug] subject={subject!r} has_alerts={has_alerts}")

    send_email(subject, body)


if __name__ == "__main__":
    main()

