import os
import socket
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

BASE = Path.home() / "social-bot"
LOGS_DIR = BASE / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("API_KEY", "").strip()
DEVICE_NAME = os.getenv("DEVICE_NAME", "").strip() or socket.gethostname()

# Replace this if Base44 shows you a different exact path for BotAlert
BOT_ALERT_URL = "https://app.base44.com/api/apps/69ad9b8c06689adb44280cf2/entities/BotAlert"


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def append_local_log(line: str):
    log_file = LOGS_DIR / "alerts.log"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def send_alert(service: str, status: str, reason: str, message: str, screenshot_path: str | None = None, extra: dict | None = None):
    payload = {
        "service": service,
        "status": status,
        "reason": reason,
        "message": message,
        "screenshot_path": screenshot_path or "",
        "timestamp": utc_now_iso(),
        "device": DEVICE_NAME,
        "hostname": socket.gethostname(),
        "extra": extra or {},
    }

    append_local_log(f"{payload['timestamp']} | {service} | {status} | {reason} | {message}")

    if not API_KEY:
        return {"ok": False, "error": "missing API_KEY", "payload": payload}

    headers = {
        "api_key": API_KEY,
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            BOT_ALERT_URL,
            json=payload,
            headers=headers,
            timeout=20,
        )

        return {
            "ok": response.ok,
            "status_code": response.status_code,
            "response_text": response.text[:1000],
            "payload": payload,
        }
    except Exception as e:
        append_local_log(f"{utc_now_iso()} | ALERT_POST_FAILED | {type(e).__name__} | {e}")
        return {"ok": False, "error": str(e), "payload": payload}
