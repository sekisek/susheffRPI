import os
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

BASE = Path.home() / "social-bot"
LOGS_DIR = BASE / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("API_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip()
DEVICE_NAME = os.getenv("DEVICE_NAME", "").strip() or socket.gethostname()

BOT_ALERT_URL = f"{SUPABASE_URL}/rest/v1/bot_alerts" if SUPABASE_URL else ""
ALERT_FAILURE_COOLDOWN_MINUTES = int(os.getenv("ALERT_FAILURE_COOLDOWN_MINUTES", "30") or "30")
ALERT_CRITICAL_COOLDOWN_MINUTES = int(os.getenv("ALERT_CRITICAL_COOLDOWN_MINUTES", "30") or "30")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def append_local_log(line: str):
    log_file = LOGS_DIR / "alerts.log"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _parse_alert_timestamp(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _cooldown_minutes_for_status(status: str) -> int:
    normalized = str(status or "").strip().lower()
    if normalized == "failure":
        return max(ALERT_FAILURE_COOLDOWN_MINUTES, 0)
    if normalized == "critical":
        return max(ALERT_CRITICAL_COOLDOWN_MINUTES, 0)
    return 0


def _build_headers():
    return {
        "apikey": SUPABASE_ANON_KEY or SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _latest_matching_alert(service: str, status: str, reason: str, device: str, headers: dict) -> dict | None:
    query_params = {
        "select": "id,timestamp,created_at",
        "service": f"eq.{service}",
        "status": f"eq.{status}",
        "reason": f"eq.{reason}",
        "device": f"eq.{device}",
        "order": "timestamp.desc.nullslast,created_at.desc.nullslast,id.desc",
        "limit": "1",
    }
    response = requests.get(BOT_ALERT_URL, headers=headers, params=query_params, timeout=20)
    if not response.ok:
        raise RuntimeError(f"cooldown lookup failed {response.status_code}: {response.text[:600]}")
    result = response.json()
    if isinstance(result, list) and result:
        return result[0]
    return None


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

    if not BOT_ALERT_URL:
        return {"ok": False, "error": "missing SUPABASE_URL", "payload": payload}

    headers = _build_headers()
    cooldown_minutes = _cooldown_minutes_for_status(status)

    try:
        if cooldown_minutes > 0:
            latest = _latest_matching_alert(service, status, reason, DEVICE_NAME, headers)
            latest_ts = _parse_alert_timestamp((latest or {}).get("timestamp") or (latest or {}).get("created_at"))
            if latest_ts and latest_ts >= datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes):
                append_local_log(
                    f"{utc_now_iso()} | ALERT_SUPPRESSED | {service} | {status} | {reason} | cooldown={cooldown_minutes}m"
                )
                return {
                    "ok": True,
                    "suppressed": True,
                    "payload": payload,
                    "suppression": {
                        "cooldown_minutes": cooldown_minutes,
                        "latest_alert_timestamp": latest_ts.isoformat(),
                        "key": {
                            "service": service,
                            "status": status,
                            "reason": reason,
                            "device": DEVICE_NAME,
                        },
                    },
                }

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
