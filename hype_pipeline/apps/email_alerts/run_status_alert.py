# apps/email_alerts/run_status_alert.py
import os, sys, smtplib, traceback
from email.mime.text import MIMEText
from datetime import datetime, timezone

"""
Sends a simple email on pipeline success/failure.

Usage:
  python apps/email_alerts/run_status_alert.py \
    --status success|failure \
    --stage "<stage name>" \
    --log "hype_pipeline.log" \
    --duration_sec 123

Config via env:
  ALERT_TO        (required, comma-separated)
  ALERT_FROM      (default: noreply@localhost)
  SMTP_HOST       (default: localhost)
  SMTP_PORT       (default: 25)
  SMTP_USER       (optional)
  SMTP_PASS       (optional)
  SMTP_STARTTLS   (optional, "1" to enable)
"""

def parse_argv(argv):
    args = {"--status": None, "--stage": None, "--log": None, "--duration_sec": None}
    it = iter(argv)
    for a in it:
        if a in args:
            args[a] = next(it, None)
    return args

def build_body(status, stage, log_path, duration_sec):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    tail = ""
    if log_path and os.path.exists(log_path):
        try:
            with open(log_path, "rb") as f:
                # last ~200 lines
                lines = f.read().splitlines()[-200:]
                tail = "\n".join([l.decode("utf-8", "ignore") for l in lines])
        except Exception:
            tail = "(failed to read log tail)\n" + traceback.format_exc()

    body = [
        f"Timestamp (UTC): {now}",
        f"Status: {status.upper()}",
        f"Stage: {stage or '(n/a)'}",
        f"Duration (sec): {duration_sec or '(n/a)'}",
        "",
        "=== Log tail (last 200 lines) ===",
        tail,
    ]
    return "\n".join(body)

def main():
    args = parse_argv(sys.argv[1:])
    status = args["--status"] or "unknown"
    stage = args["--stage"] or ""
    log_path = args["--log"] or ""
    duration = args["--duration_sec"] or ""

    # Prefer ALERT_* vars, but fall back to FROM/TO (used by watchlist emailer)
    to = os.getenv("ALERT_TO") or os.getenv("TO")
    if not to:
        print("[run_status_alert] ALERT_TO/TO env var required", file=sys.stderr)
        sys.exit(0)  # don't fail pipeline due to missing config

    frm = os.getenv("ALERT_FROM") or os.getenv("FROM") or os.getenv("SMTP_USER", "noreply@localhost")

    host = os.getenv("SMTP_HOST", "localhost")
    port = int(os.getenv("SMTP_PORT", "25"))
    user = os.getenv("SMTP_USER", "")
    pwd  = os.getenv("SMTP_PASS", "")
    starttls = os.getenv("SMTP_STARTTLS", "0") in ("1", "true", "True")

    subject = f"[hype_pipeline] {status.upper()} @ {stage or 'pipeline'}"
    body = build_body(status, stage, log_path, duration)

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = frm
    msg["To"] = to

    try:
        with smtplib.SMTP(host=host, port=port, timeout=30) as s:
            if starttls:
                s.starttls()
            if user:
                s.login(user, pwd)
            s.sendmail(frm, to.split(","), msg.as_string())
        print("[run_status_alert] sent", subject)
    except Exception as e:
        print(f"[run_status_alert] failed to send email: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()

