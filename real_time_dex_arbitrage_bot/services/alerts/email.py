# services/alerts/email.py
from __future__ import annotations
import os
from email.message import EmailMessage
import aiosmtplib

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"

async def send(subject: str, body: str):
    if not (SMTP_HOST and SMTP_PORT and EMAIL_FROM and EMAIL_TO):
        return
    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.set_content(body)

    await aiosmtplib.send(
        msg,
        hostname=SMTP_HOST,
        port=SMTP_PORT,
        username=SMTP_USER,
        password=SMTP_PASS,
        start_tls=SMTP_USE_TLS,
    )
