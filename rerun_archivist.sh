#!/bin/bash
#
# Re-archive any night in the last week whose tarball is missing.
#
# Usage: rerun_archivist.sh
#
# For each of the previous ZTF_RERUN_DAYS (default 7) UTC days and each
# program, runs run_archivist.sh for that day if its tarball does not exist.
# Runs are sequential. One failing does not stop the others; the exit status
# is non-zero if any failed.
#
# Uses the same environment as run_archivist.sh.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVE_ROOT=${ZTF_ARCHIVE_ROOT:-/data/alerts}
RERUN_DAYS=${ZTF_RERUN_DAYS:-7}

log() {
    echo "$(date --rfc-3339=ns) | rerun_archivist.sh | $*"
}

FAILED=0
for ((DAYS_AGO = RERUN_DAYS; DAYS_AGO >= 1; DAYS_AGO--)); do
    TIMESTAMP=$(date -u -d "${DAYS_AGO} days ago" +%Y%m%d)
    for PROGRAM in public:programid1 partnership:programid2; do
        PROGRAM_CLASS=${PROGRAM%%:*}
        PROGRAM_ID=${PROGRAM##*:}
        TARBALL_PATH="${ARCHIVE_ROOT}/${PROGRAM_CLASS}/ztf_${PROGRAM_CLASS}_${TIMESTAMP}.tar.gz"
        if [[ ! -f $TARBALL_PATH ]]; then
            log "${TARBALL_PATH} is missing, rerunning"
            if ! "${SCRIPT_DIR}/run_archivist.sh" "${PROGRAM_ID}" "${TIMESTAMP}"; then
                log "rerun of ${PROGRAM_ID} ${TIMESTAMP} failed"
                FAILED=1
            fi
        fi
    done
done

exit $FAILED
