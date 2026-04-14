#!/usr/bin/env python3
"""
Standalone Edge Function warmup script.
Runs via cron every 4 minutes to keep Supabase Edge Functions warm.
Prevents intermittent 405 errors from cold starts.

Crontab entry (add via: crontab -e):
  */4 * * * * cd /home/bamanio/social-bot/app && /usr/bin/python3 warmup_edge_functions.py >> /tmp/warmup.log 2>&1
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load env from the same .env the main worker uses
env_path = Path(__file__).resolve().parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

import requests

BOT_SECRET = os.getenv("BOT_SECRET", "").strip()
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY", "").strip()

WARMUP_URLS = [
    os.getenv("UPLOAD_BOT_SCREENSHOT_URL", "").strip(),
    os.getenv("SUBMIT_BOT_EVIDENCE_URL", "").strip(),
    os.getenv("CLAIM_NEXT_JOB_URL", "").strip(),
    os.getenv("HEARTBEAT_URL", "").strip(),
]


def headers():
    h = {"Content-Type": "application/json"}
    if BOT_SECRET:
        h["x-bot-secret"] = BOT_SECRET
    if SUPABASE_ANON_KEY:
        h["Authorization"] = f"Bearer {SUPABASE_ANON_KEY}"
    return h


def main():
    warmed = 0
    for url in WARMUP_URLS:
        if not url:
            continue
        try:
            resp = requests.post(url, headers=headers(), json={"warmup": True}, timeout=5)
            warmed += 1
        except Exception:
            pass
    print(f"Warmed {warmed}/{len([u for u in WARMUP_URLS if u])} functions")


if __name__ == "__main__":
    main()
