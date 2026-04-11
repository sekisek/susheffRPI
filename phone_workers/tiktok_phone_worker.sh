#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$SCRIPT_DIR/captures"

URL="${1:?Usage: $0 <tiktok_url> [job_id]}"
JOB_ID="${2:-latest}"
CLEAN_URL="${URL%%\?*}"

REMOTE_FILE="/sdcard/tiktok_${JOB_ID}.png"
LOCAL_FILE="$OUT_DIR/tiktok_${JOB_ID}.png"

mkdir -p "$OUT_DIR"

echo "Opening: $CLEAN_URL"
adb shell input keyevent KEYCODE_WAKEUP || true
adb shell "am start -W -a android.intent.action.VIEW -d '$CLEAN_URL' -p com.zhiliaoapp.musically"

sleep 8

echo "Capturing screenshot..."
adb shell screencap -p "$REMOTE_FILE"
adb pull "$REMOTE_FILE" "$LOCAL_FILE"
adb shell rm -f "$REMOTE_FILE" || true

echo "Saved to: $LOCAL_FILE"
ls -lh "$LOCAL_FILE"
