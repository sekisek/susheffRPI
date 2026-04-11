#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="$SCRIPT_DIR/captures"

URL="${1:?Usage: $0 <tiktok_url>}"
CLEAN_URL="${URL%%\?*}"

mkdir -p "$OUT_DIR"

adb shell input keyevent KEYCODE_WAKEUP || true
adb shell "am start -W -a android.intent.action.VIEW -d '$CLEAN_URL' -p com.zhiliaoapp.musically"
sleep 8

echo "=== FOCUS ==="
adb shell dumpsys window | grep -E 'mCurrentFocus|mFocusedApp' || true

adb shell screencap -p /sdcard/ui_check.png
adb pull /sdcard/ui_check.png "$OUT_DIR/ui_check.png" >/dev/null

adb shell uiautomator dump /sdcard/uidump.xml || true
adb pull /sdcard/uidump.xml "$OUT_DIR/uidump.xml" >/dev/null || true

echo "=== TEXT HITS ==="
sed 's/></>\n</g' "$OUT_DIR/uidump.xml" 2>/dev/null | grep -niE 'more|see more|description|caption' | head -50 || true

echo "Saved:"
echo "  $OUT_DIR/ui_check.png"
echo "  $OUT_DIR/uidump.xml"
