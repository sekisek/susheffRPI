#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$SCRIPT_DIR/.venv/bin/python"
FIND_MORE="$SCRIPT_DIR/find_tiktok_more.py"

URL="${1:?Usage: $0 <tiktok_url> [job_id]}"
JOB_ID="${2:-latest}"
CLEAN_URL="${URL%%\?*}"

OUT_DIR="$SCRIPT_DIR/captures/$JOB_ID"
mkdir -p "$OUT_DIR"

LOG_FILE="$OUT_DIR/steps.log"
: > "$LOG_FILE"

ADB_TIMEOUT_SECONDS="${TIKTOK_PHONE_ADB_TIMEOUT_SECONDS:-20}"
OPEN_WAIT_SECONDS="${TIKTOK_PHONE_OPEN_WAIT_SECONDS:-8}"
ATTEMPT_COUNT="${TIKTOK_PHONE_MORE_ATTEMPTS:-3}"
CANDIDATE_COUNT="${TIKTOK_PHONE_MORE_CANDIDATES:-3}"
MORE_DETECT_TIMEOUT_SECONDS="${TIKTOK_PHONE_MORE_DETECT_TIMEOUT_SECONDS:-15}"
TAP_SETTLE_SECONDS="${TIKTOK_PHONE_TAP_SETTLE_SECONDS:-2}"
POST_BACK_SETTLE_SECONDS="${TIKTOK_PHONE_POST_BACK_SETTLE_SECONDS:-2}"
RETRY_SLEEP_SECONDS="${TIKTOK_PHONE_RETRY_SLEEP_SECONDS:-1}"
APP_PACKAGE="com.zhiliaoapp.musically"

REMOTE_EXPANDED="/sdcard/${JOB_ID}_02_expanded.png"
REMOTE_EXPANDED_XML="/sdcard/${JOB_ID}_02_expanded.xml"

log_step() {
  local message="$*"
  local stamp
  stamp="$(date -Is)"
  echo "${stamp} | ${message}" | tee -a "$LOG_FILE"
}

run_with_timeout() {
  timeout --foreground "${ADB_TIMEOUT_SECONDS}" "$@"
}

adb_safe() {
  if ! run_with_timeout adb "$@" >>"$LOG_FILE" 2>&1; then
    return 1
  fi
  return 0
}

run_detector() {
  local output=""
  local rc=0
  set +e
  output="$(timeout --foreground "${MORE_DETECT_TIMEOUT_SECONDS}" "$PY" "$@" 2>>"$LOG_FILE")"
  rc=$?
  set -e
  printf '%s' "$output"
  return "$rc"
}

capture_screen() {
  local remote_file="$1"
  local local_file="$2"
  adb_safe shell screencap -p "$remote_file" || return 1
  adb_safe pull "$remote_file" "$local_file" >/dev/null || return 1
  adb_safe shell rm -f "$remote_file" >/dev/null || true
  return 0
}

dump_ui() {
  local remote_file="$1"
  local local_file="$2"
  adb_safe shell uiautomator dump "$remote_file" >/dev/null || return 1
  adb_safe pull "$remote_file" "$local_file" >/dev/null || return 1
  adb_safe shell rm -f "$remote_file" >/dev/null || true
  return 0
}

get_focus() {
  local output_file="$1"
  if adb_safe shell dumpsys window | grep -E 'mCurrentFocus|mFocusedApp' >"$output_file" 2>>"$LOG_FILE"; then
    cat "$output_file"
    return 0
  fi
  return 1
}

profile_focus_detected() {
  local focus_text="$1"
  echo "$focus_text" | grep -qiE 'profile|com\.zhiliaoapp\.musically/com\.ss\.android\.ugc\.aweme\.profile'
}

comments_focus_detected() {
  local focus_text="$1"
  echo "$focus_text" | grep -qiE 'comment'
}

more_exists() {
  local image_path="$1"
  local xml_path="${2:-}"
  local rc=0
  if [[ -n "$xml_path" && -f "$xml_path" ]]; then
    run_detector "$FIND_MORE" --exists "$image_path" "$xml_path" >/dev/null || rc=$?
  else
    run_detector "$FIND_MORE" --exists "$image_path" >/dev/null || rc=$?
  fi

  if [[ "$rc" -eq 0 ]]; then
    return 0
  fi
  if [[ "$rc" -eq 2 ]]; then
    return 1
  fi
  return 2
}

collect_candidates() {
  local image_path="$1"
  local xml_path="${2:-}"
  if [[ -n "$xml_path" && -f "$xml_path" ]]; then
    run_detector "$FIND_MORE" --all --top-k "$CANDIDATE_COUNT" "$image_path" "$xml_path"
  else
    run_detector "$FIND_MORE" --all --top-k "$CANDIDATE_COUNT" "$image_path"
  fi
}

if [[ ! -x "$PY" ]]; then
  echo "Missing python venv at $PY" >&2
  exit 1
fi

if [[ ! -f "$FIND_MORE" ]]; then
  echo "Missing detector script at $FIND_MORE" >&2
  exit 1
fi

OPEN_ATTEMPTS="${TIKTOK_PHONE_OPEN_ATTEMPTS:-3}"

open_tiktok_url() {
  local attempt="$1"
  log_step "OPEN_ATTEMPT attempt=$attempt mode=package_pinned url=$CLEAN_URL"
  adb_safe shell input keyevent KEYCODE_WAKEUP || true
  if adb_safe shell am start -W -a android.intent.action.VIEW -d "$CLEAN_URL" -p "$APP_PACKAGE"; then
    return 0
  fi

  log_step "OPEN_FAILED attempt=$attempt mode=package_pinned"
  log_step "OPEN_ATTEMPT attempt=$attempt mode=fallback_no_package url=$CLEAN_URL"
  adb_safe shell input keyevent KEYCODE_WAKEUP || true
  if adb_safe shell am start -W -a android.intent.action.VIEW -d "$CLEAN_URL"; then
    return 0
  fi

  log_step "OPEN_FAILED attempt=$attempt mode=fallback_no_package"
  return 1
}

capture_open_probe() {
  local attempt="$1"
  local remote_probe="/sdcard/${JOB_ID}_00_open_probe_attempt${attempt}.png"
  local local_probe="$OUT_DIR/00_open_probe_attempt${attempt}.png"
  if capture_screen "$remote_probe" "$local_probe"; then
    log_step "OPEN_PROBE_SAVED attempt=$attempt path=$local_probe"
    if [[ ! -f "$OUT_DIR/01_open.png" ]]; then
      cp "$local_probe" "$OUT_DIR/01_open.png" || true
    fi
    return 0
  fi
  log_step "OPEN_PROBE_FAILED attempt=$attempt"
  return 1
}

log_step "Opening: $CLEAN_URL"
OPEN_OK=""
for open_attempt in $(seq 1 "$OPEN_ATTEMPTS"); do
  if open_tiktok_url "$open_attempt"; then
    OPEN_OK="yes"
    sleep "$OPEN_WAIT_SECONDS"
    capture_open_probe "$open_attempt" || true
    break
  fi
  sleep "$RETRY_SLEEP_SECONDS"
done

if [[ -z "$OPEN_OK" ]]; then
  log_step "OPEN_ALL_ATTEMPTS_FAILED url=$CLEAN_URL"
  exit 1
fi

SUCCESS=""
LAST_OPEN=""
LAST_ATTEMPT=""
BEST_ATTEMPT=""
BEST_CANDIDATE=""

for attempt in $(seq 1 "$ATTEMPT_COUNT"); do
  REMOTE_OPEN="/sdcard/${JOB_ID}_01_open_attempt${attempt}.png"
  LOCAL_OPEN="$OUT_DIR/01_open_attempt${attempt}.png"
  REMOTE_XML="/sdcard/${JOB_ID}_01_open_attempt${attempt}.xml"
  LOCAL_XML="$OUT_DIR/01_open_attempt${attempt}.xml"

  log_step "Capture open screenshot attempt=$attempt"
  if ! capture_screen "$REMOTE_OPEN" "$LOCAL_OPEN"; then
    log_step "CAPTURE_OPEN_FAILED attempt=$attempt"
    sleep "$RETRY_SLEEP_SECONDS"
    continue
  fi
  if [[ ! -f "$OUT_DIR/01_open.png" ]]; then
    cp "$LOCAL_OPEN" "$OUT_DIR/01_open.png" || true
  fi

  if dump_ui "$REMOTE_XML" "$LOCAL_XML"; then
    log_step "UI_DUMP_OK attempt=$attempt xml=$LOCAL_XML"
  else
    log_step "UI_DUMP_FAILED attempt=$attempt"
    rm -f "$LOCAL_XML" >/dev/null 2>&1 || true
  fi

  LAST_OPEN="$LOCAL_OPEN"
  LAST_ATTEMPT="$attempt"

  log_step "Finding more candidates attempt=$attempt"
  CANDIDATE_OUTPUT=""
  CANDIDATE_RC=0
  set +e
  CANDIDATE_OUTPUT="$(collect_candidates "$LOCAL_OPEN" "$LOCAL_XML")"
  CANDIDATE_RC=$?
  set -e

  if [[ "$CANDIDATE_RC" -eq 124 ]]; then
    log_step "MORE_DETECT_TIMEOUT attempt=$attempt"
    sleep "$RETRY_SLEEP_SECONDS"
    continue
  fi

  mapfile -t CANDIDATES <<< "$CANDIDATE_OUTPUT"
  if [[ "${#CANDIDATES[@]}" -eq 0 ]]; then
    log_step "MORE_NOT_FOUND attempt=$attempt rc=$CANDIDATE_RC"
    sleep "$RETRY_SLEEP_SECONDS"
    continue
  fi

  local_candidate_index=0
  for coords in "${CANDIDATES[@]}"; do
    [[ -z "$coords" ]] && continue
    read -r MORE_X MORE_Y <<< "$coords"
    [[ -z "${MORE_X:-}" || -z "${MORE_Y:-}" ]] && continue

    local_candidate_index=$((local_candidate_index + 1))
    log_step "MORE_CANDIDATE attempt=$attempt candidate=$local_candidate_index x=$MORE_X y=$MORE_Y"

    adb_safe shell input tap "$MORE_X" "$MORE_Y" || {
      log_step "TAP_FAILED attempt=$attempt candidate=$local_candidate_index"
      continue
    }
    sleep "$TAP_SETTLE_SECONDS"

    FOCUS_FILE="$OUT_DIR/focus_after_tap_attempt${attempt}_candidate${local_candidate_index}.txt"
    FOCUS_TEXT="$(get_focus "$FOCUS_FILE" || true)"
    log_step "FOCUS_AFTER_TAP attempt=$attempt candidate=$local_candidate_index focus=${FOCUS_TEXT:-unknown}"

    if profile_focus_detected "$FOCUS_TEXT"; then
      log_step "BAD_TAP_PROFILE attempt=$attempt candidate=$local_candidate_index"
      adb_safe shell input keyevent KEYCODE_BACK || true
      sleep "$POST_BACK_SETTLE_SECONDS"
      continue
    fi

    if comments_focus_detected "$FOCUS_TEXT"; then
      log_step "BAD_TAP_COMMENTS attempt=$attempt candidate=$local_candidate_index"
      adb_safe shell input keyevent KEYCODE_BACK || true
      sleep "$POST_BACK_SETTLE_SECONDS"
      continue
    fi

    EXPANDED_CANDIDATE_REMOTE="/sdcard/${JOB_ID}_02_expanded_attempt${attempt}_candidate${local_candidate_index}.png"
    EXPANDED_CANDIDATE_LOCAL="$OUT_DIR/02_expanded_attempt${attempt}_candidate${local_candidate_index}.png"
    EXPANDED_CANDIDATE_XML_REMOTE="/sdcard/${JOB_ID}_02_expanded_attempt${attempt}_candidate${local_candidate_index}.xml"
    EXPANDED_CANDIDATE_XML_LOCAL="$OUT_DIR/02_expanded_attempt${attempt}_candidate${local_candidate_index}.xml"

    log_step "Capture expanded screenshot attempt=$attempt candidate=$local_candidate_index"
    if ! capture_screen "$EXPANDED_CANDIDATE_REMOTE" "$EXPANDED_CANDIDATE_LOCAL"; then
      log_step "CAPTURE_EXPANDED_FAILED attempt=$attempt candidate=$local_candidate_index"
      continue
    fi

    if dump_ui "$EXPANDED_CANDIDATE_XML_REMOTE" "$EXPANDED_CANDIDATE_XML_LOCAL"; then
      log_step "UI_DUMP_EXPANDED_OK attempt=$attempt candidate=$local_candidate_index"
    else
      log_step "UI_DUMP_EXPANDED_FAILED attempt=$attempt candidate=$local_candidate_index"
      rm -f "$EXPANDED_CANDIDATE_XML_LOCAL" >/dev/null 2>&1 || true
    fi

    MORE_EXISTS_RC=0
    if more_exists "$EXPANDED_CANDIDATE_LOCAL" "$EXPANDED_CANDIDATE_XML_LOCAL"; then
      MORE_EXISTS_RC=0
    else
      MORE_EXISTS_RC=$?
    fi

    if [[ "$MORE_EXISTS_RC" -eq 0 ]]; then
      log_step "MORE_STILL_VISIBLE attempt=$attempt candidate=$local_candidate_index"
      continue
    fi
    if [[ "$MORE_EXISTS_RC" -eq 2 ]]; then
      log_step "MORE_EXISTS_CHECK_TIMEOUT attempt=$attempt candidate=$local_candidate_index"
      continue
    fi

    cp "$LOCAL_OPEN" "$OUT_DIR/01_open.png"
    cp "$EXPANDED_CANDIDATE_LOCAL" "$OUT_DIR/02_expanded.png"
    if [[ -f "$EXPANDED_CANDIDATE_XML_LOCAL" ]]; then
      cp "$EXPANDED_CANDIDATE_XML_LOCAL" "$OUT_DIR/02_expanded.xml"
    fi

    BEST_ATTEMPT="$attempt"
    BEST_CANDIDATE="$local_candidate_index"
    SUCCESS="yes"
    break 2
  done

  sleep "$RETRY_SLEEP_SECONDS"
done

if [[ -z "$SUCCESS" ]]; then
  log_step "No successful caption expand after retries."
  if [[ -n "$LAST_OPEN" && -f "$LAST_OPEN" ]]; then
    cp "$LAST_OPEN" "$OUT_DIR/01_open.png"
  fi
  {
    echo "LAST_ATTEMPT=$LAST_ATTEMPT"
    echo "EXPAND_SUCCESS=false"
    echo "LAST_OPEN=$LAST_OPEN"
    echo "LOG_FILE=$LOG_FILE"
  } > "$OUT_DIR/expand_result.txt"
else
  {
    echo "EXPAND_SUCCESS=true"
    echo "BEST_ATTEMPT=$BEST_ATTEMPT"
    echo "BEST_CANDIDATE=$BEST_CANDIDATE"
    echo "OPEN_IMAGE=$OUT_DIR/01_open.png"
    echo "EXPANDED_IMAGE=$OUT_DIR/02_expanded.png"
    echo "LOG_FILE=$LOG_FILE"
  } > "$OUT_DIR/expand_result.txt"
fi

log_step "Saved to: $OUT_DIR"
ls -lh "$OUT_DIR" | tee -a "$LOG_FILE"
