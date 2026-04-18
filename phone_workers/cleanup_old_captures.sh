#!/bin/bash
# Delete phone worker capture folders older than CAPTURES_RETENTION_DAYS.
# Runs daily via cron; logs to /tmp/captures_cleanup.log.
#
# Config (can be overridden in .env or cron environment):
#   CAPTURES_RETENTION_DAYS — how many days to keep (default 7)
#   CAPTURES_DIR           — captures root (default /home/bamanio/social-bot/app/phone_workers/captures)
set -euo pipefail

# Load .env if present (for shared config)
if [[ -f /home/bamanio/social-bot/.env ]]; then
  set -a
  # shellcheck disable=SC1091
  source /home/bamanio/social-bot/.env
  set +a
fi

RETENTION_DAYS="${CAPTURES_RETENTION_DAYS:-7}"
CAPTURES_DIR="${CAPTURES_DIR:-/home/bamanio/social-bot/app/phone_workers/captures}"

STAMP="$(date -Is)"
LOG_FILE="/tmp/captures_cleanup.log"

log() {
  echo "${STAMP} | $*" | tee -a "$LOG_FILE"
}

# Safety: refuse to run if the captures dir doesn't exist or looks wrong
if [[ ! -d "$CAPTURES_DIR" ]]; then
  log "ABORT captures_dir_missing path=$CAPTURES_DIR"
  exit 0
fi

case "$CAPTURES_DIR" in
  /home/bamanio/social-bot/app/phone_workers/captures*) ;;
  *)
    log "ABORT unsafe_captures_dir path=$CAPTURES_DIR"
    exit 1
    ;;
esac

# Find folders older than retention window whose names look like job IDs
# (UUID-like: hex chars, dashes, underscores; 10+ chars long)
BEFORE_SIZE=$(du -sh "$CAPTURES_DIR" 2>/dev/null | awk '{print $1}')
BEFORE_COUNT=$(find "$CAPTURES_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l)

DELETED_COUNT=0
while IFS= read -r dir; do
  [[ -z "$dir" ]] && continue
  base="$(basename "$dir")"
  # Only delete things that look like job IDs — no spaces, no weird chars
  if [[ ! "$base" =~ ^[a-zA-Z0-9_-]{10,}$ ]]; then
    log "SKIP_NON_JOB_DIR path=$dir"
    continue
  fi
  log "DELETE path=$dir age_days>$RETENTION_DAYS"
  rm -rf "$dir"
  DELETED_COUNT=$((DELETED_COUNT + 1))
done < <(find "$CAPTURES_DIR" -mindepth 1 -maxdepth 1 -type d -mtime "+$RETENTION_DAYS" 2>/dev/null)

AFTER_SIZE=$(du -sh "$CAPTURES_DIR" 2>/dev/null | awk '{print $1}')
AFTER_COUNT=$(find "$CAPTURES_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l)

log "SUMMARY deleted=$DELETED_COUNT before_count=$BEFORE_COUNT after_count=$AFTER_COUNT before_size=$BEFORE_SIZE after_size=$AFTER_SIZE"
