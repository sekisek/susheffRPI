import os
import socket
import subprocess
import requests
from dotenv import load_dotenv

load_dotenv("/home/bamanio/social-bot/.env")

HEARTBEAT_URL = os.getenv("HEARTBEAT_URL", "").strip()
BOT_SECRET = os.getenv("BOT_SECRET", "").strip()
DEVICE_NAME = os.getenv("DEVICE_NAME", "").strip() or socket.gethostname()

PHONE_WORKER_ENABLED = os.getenv("PHONE_WORKER_ENABLED", "").strip().lower() in {
    "1", "true", "yes", "on"
}
COLLECTOR_ACCOUNT_LABEL = os.getenv("COLLECTOR_ACCOUNT_LABEL", "").strip()
ADB_PATH = os.getenv("ADB_PATH", "adb").strip() or "adb"

if not HEARTBEAT_URL:
    raise RuntimeError("Missing HEARTBEAT_URL in .env")
if not BOT_SECRET:
    raise RuntimeError("Missing BOT_SECRET in .env")


def service_state(name: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        return result.stdout.strip() or "unknown"
    except Exception:
        return "unknown"


def adb_phone_message() -> tuple[bool, str]:
    """
    Returns:
      (ok, message)
      ok=True  => phone state is good or phone monitoring disabled
      ok=False => phone monitoring enabled and adb does not show a connected device
    """
    if not PHONE_WORKER_ENABLED:
        return True, "phone-monitor=disabled"

    label = COLLECTOR_ACCOUNT_LABEL or "phone-worker"

    try:
        # Safe to call; starts adb server if needed.
        subprocess.run(
            [ADB_PATH, "start-server"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

        result = subprocess.run(
            [ADB_PATH, "devices", "-l"],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()

        if result.returncode != 0:
            reason = stderr or stdout or f"adb_exit_{result.returncode}"
            return False, f"phone[{label}]=adb_error ({reason[:200]})"

        device_lines = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("List of devices attached"):
                continue
            if line.startswith("* daemon"):
                continue
            device_lines.append(line)

        good = []
        bad = []

        for line in device_lines:
            parts = line.split()
            serial = parts[0] if len(parts) >= 1 else "unknown"
            state = parts[1] if len(parts) >= 2 else "unknown"

            if state == "device":
                good.append(serial)
            else:
                bad.append(f"{serial}:{state}")

        if good:
            summary = f"{len(good)} connected"
            if bad:
                summary += f"; other={','.join(bad[:3])}"
            return True, f"phone[{label}]=connected ({summary})"

        if bad:
            return False, f"phone[{label}]=not_ready ({','.join(bad[:3])})"

        return False, f"phone[{label}]=missing (adb sees no devices)"

    except FileNotFoundError:
        return False, f"phone[{label}]=adb_missing"
    except subprocess.TimeoutExpired:
        return False, f"phone[{label}]=adb_timeout"
    except Exception as e:
        return False, f"phone[{label}]=adb_exception ({type(e).__name__}: {str(e)[:120]})"


job_worker = service_state("job-worker.service")
instagram_monitor = service_state("instagram-monitor.service")
phone_ok, phone_message = adb_phone_message()

status = "ok"
if job_worker != "active" or instagram_monitor != "active" or not phone_ok:
    status = "degraded"

message = (
    f"job-worker={job_worker}; "
    f"instagram-monitor={instagram_monitor}; "
    f"{phone_message}"
)

resp = requests.post(
    HEARTBEAT_URL,
    headers={
        "Content-Type": "application/json",
        "x-bot-secret": BOT_SECRET,
    },
    json={
        "device_name": DEVICE_NAME,
        "status": status,
        "message": message,
        "platform": "instagram",
        "ip_or_hostname": socket.gethostname(),
    },
    timeout=30,
)

print(resp.status_code)
print(resp.text[:1000])
