#!/bin/bash
#
# The nightly archive job. Run it once a day from a scheduler, shortly after
# 00:00 Pacific time.
#
# Usage: nightly.sh
#
# Starts tonight's public and partnership runs and the rerunner in parallel,
# each with its own log file, and waits for all three. If any fails, it says
# which, posts to Slack if a webhook is configured, and exits non-zero.
#
# Environment, in addition to what run_archivist.sh reads (defaults in
# parentheses):
#   ZTF_LOG_DIR                 per-run log files (/data/logs)
#   ZTF_LOG_RETENTION_DAYS      delete log files older than this (90)
#   ZTF_SLACK_WEBHOOK_FILE      file holding a Slack incoming-webhook URL
#                               (/run/secrets/slack_webhook). If it does not
#                               exist, nothing is posted.

set -uo pipefail
umask 002

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVE_ROOT=${ZTF_ARCHIVE_ROOT:-/data/alerts}
SCRATCH_DIR=${ZTF_SCRATCH_DIR:-/data/scratch}
LOG_DIR=${ZTF_LOG_DIR:-/data/logs}
LOG_RETENTION_DAYS=${ZTF_LOG_RETENTION_DAYS:-90}
SLACK_WEBHOOK_FILE=${ZTF_SLACK_WEBHOOK_FILE:-/run/secrets/slack_webhook}

log() {
    echo "$(date --rfc-3339=ns) | nightly.sh | $*"
}

# notify_slack TEXT: post TEXT to the webhook. Never fails the job.
notify_slack() {
    [[ -r $SLACK_WEBHOOK_FILE ]] || { log "no Slack webhook at ${SLACK_WEBHOOK_FILE}; not notifying"; return 0; }
    local url text
    url=$(head -n 1 "$SLACK_WEBHOOK_FILE" | tr -d '[:space:]')
    [[ -n $url ]] || { log "Slack webhook file is empty; not notifying"; return 0; }
    # JSON-escape: backslash, double quote, then newlines and tabs.
    text=$(printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/\t/\\t/g' | tr -d '\000-\010\013-\037' \
        | awk 'BEGIN { ORS = "\\n" } { print }')
    if ! curl --silent --show-error --fail --max-time 30 \
            -H 'Content-Type: application/json' \
            --data "{\"text\": \"${text}\"}" "$url" > /dev/null; then
        log "posting to Slack failed"
    fi
    return 0
}

DATE=$(date -u +%Y%m%d)
log "starting nightly run for ${DATE}"

# Leftovers of runs that were killed (VM reboot, container restart). Anything
# this old cannot belong to a run that is still going.
find "$SCRATCH_DIR" -mindepth 1 -maxdepth 1 -name 'ztf-archivist-scratch_*' -mtime +1 \
    -exec rm -rf {} + 2>/dev/null
find "$ARCHIVE_ROOT/public" "$ARCHIVE_ROOT/partnership" -maxdepth 1 -name '.tmp.*' -mtime +1 \
    -delete 2>/dev/null
find "$LOG_DIR" -maxdepth 1 -name '*.log' -mtime "+${LOG_RETENTION_DAYS}" -delete 2>/dev/null

NAMES=(programid1 programid2 rerunner)
PIDS=()
LOGS=()
for NAME in "${NAMES[@]}"; do
    LOGFILE="${LOG_DIR}/${NAME}_${DATE}.log"
    LOGS+=("$LOGFILE")
    if [[ $NAME == rerunner ]]; then
        "${SCRIPT_DIR}/rerun_archivist.sh" >> "$LOGFILE" 2>&1 &
    else
        "${SCRIPT_DIR}/run_archivist.sh" "$NAME" >> "$LOGFILE" 2>&1 &
    fi
    PIDS+=($!)
    log "started ${NAME} (pid $!), logging to ${LOGFILE}"
done

FAILURES=""
for i in "${!NAMES[@]}"; do
    EXITCODE=0
    wait "${PIDS[$i]}" || EXITCODE=$?
    if [[ $EXITCODE -ne 0 ]]; then
        log "${NAMES[$i]} exited with code ${EXITCODE}. Last 10 lines of ${LOGS[$i]}:"
        tail -n 10 "${LOGS[$i]}"
        FAILURES+="${NAMES[$i]} exited with code ${EXITCODE}. Last lines of ${LOGS[$i]}:"$'\n'
        FAILURES+="$(tail -n 10 "${LOGS[$i]}" | cut -c1-300)"$'\n'
    else
        log "${NAMES[$i]} finished"
    fi
done

if [[ -n $FAILURES ]]; then
    notify_slack "ZTF alert archive: nightly run for ${DATE} failed on $(hostname -f 2>/dev/null || hostname)."$'\n'"${FAILURES}"
    log "nightly run for ${DATE} finished with failures"
    exit 1
fi
log "nightly run for ${DATE} finished"
