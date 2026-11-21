#!/bin/bash
#
# Archive one night of one ZTF program to a gzipped tarball.
#
# Usage: run_archivist.sh [--force] PROGRAMID [DATE]
#
# PROGRAMID is 'programid1' for public data or 'programid2' for partnership
# data.
#
# DATE is optional, a ZTF-style UTC date (YYYYMMDD). If unset, today (UTC) is
# used. A run for any other date is a "past-date" run: the night's topic is
# already complete, so it reads from the start under a fresh consumer group
# and stops soon after the topic goes quiet.
#
# --force replaces a tarball that already exists. Without it, the script
# refuses, because a second run under a consumer group that has already read
# the topic would replace a good tarball with an empty one.
#
# Nothing is published unless ztf-go-archivist exits successfully. The tarball
# is compressed next to its destination under a hidden name and renamed into
# place, so readers never see a partial file.
#
# Environment (defaults in parentheses):
#   ZTF_ARCHIVE_ROOT          directory holding public/ and partnership/
#                             (/data/alerts)
#   ZTF_SCRATCH_DIR           where the uncompressed tar is written
#                             (/data/scratch)
#   ZTF_BROKER                Kafka broker (partnership.alerts.ztf.uw.edu:9092)
#   ZTF_ARCHIVIST_GROUP       consumer group for tonight's run
#                             (ztf-go-archivist)
#   ZTF_ARCHIVIST_BIN         the archivist binary (ztf-go-archivist)
#   ZTF_MAX_RUNTIME           passed as -max-runtime if set
#   ZTF_MAX_QUIET_PERIOD      passed as -max-quiet-period for tonight's run
#                             if set
#   ZTF_RERUN_QUIET_PERIOD    -max-quiet-period for past-date runs (10m)

set -euo pipefail

log() {
    echo "$(date --rfc-3339=ns) | run_archivist.sh | $*"
}

usage() {
    echo "usage: run_archivist.sh [--force] PROGRAMID [DATE]" >&2
    exit 2
}

FORCE=0
if [[ "${1:-}" == "--force" ]]; then
    FORCE=1
    shift
fi
[[ $# -ge 1 && $# -le 2 ]] || usage

PROGRAMID=$1
TODAY=$(date -u +%Y%m%d)
ZTF_TIMESTAMP=${2:-$TODAY}
if [[ ! $ZTF_TIMESTAMP =~ ^[0-9]{8}$ ]]; then
    echo "Invalid argument: DATE should be YYYYMMDD (got '$ZTF_TIMESTAMP')" >&2
    exit 2
fi

case $PROGRAMID in
    programid1) PROGRAM_CLASS=public ;;
    programid2) PROGRAM_CLASS=partnership ;;
    *)
        echo "Invalid argument: PROGRAMID should be either 'programid1' or 'programid2' (got '$PROGRAMID')" >&2
        exit 2
        ;;
esac

ARCHIVE_ROOT=${ZTF_ARCHIVE_ROOT:-/data/alerts}
SCRATCH_DIR=${ZTF_SCRATCH_DIR:-/data/scratch}
BROKER=${ZTF_BROKER:-partnership.alerts.ztf.uw.edu:9092}
ARCHIVIST_BIN=${ZTF_ARCHIVIST_BIN:-ztf-go-archivist}

ZTF_TOPIC="ztf_${ZTF_TIMESTAMP}_${PROGRAMID}"
DEST_DIR="${ARCHIVE_ROOT}/${PROGRAM_CLASS}"
DEST_NAME="ztf_${PROGRAM_CLASS}_${ZTF_TIMESTAMP}.tar.gz"
DESTINATION="${DEST_DIR}/${DEST_NAME}"

check_destination() {
    if [[ -e $DESTINATION && $FORCE -eq 0 ]]; then
        log "${DESTINATION} already exists; not replacing it without --force"
        exit 3
    fi
}

[[ -d $DEST_DIR && -w $DEST_DIR ]] || { log "${DEST_DIR} is not a writable directory"; exit 1; }
[[ -d $SCRATCH_DIR && -w $SCRATCH_DIR ]] || { log "${SCRATCH_DIR} is not a writable directory"; exit 1; }
check_destination

ARCHIVIST_ARGS=(-broker="${BROKER}" -topic="${ZTF_TOPIC}")
if [[ $ZTF_TIMESTAMP == "$TODAY" ]]; then
    GROUP=${ZTF_ARCHIVIST_GROUP:-ztf-go-archivist}
    if [[ -n ${ZTF_MAX_QUIET_PERIOD:-} ]]; then
        ARCHIVIST_ARGS+=(-max-quiet-period="${ZTF_MAX_QUIET_PERIOD}")
    fi
else
    # A group nobody has used reads the topic from its first offset. The
    # topic is finished, so there is no point waiting hours after its last
    # message.
    GROUP="${ZTF_ARCHIVIST_GROUP:-ztf-go-archivist}-rerun-$(date -u +%Y%m%dT%H%M%S)-$$"
    ARCHIVIST_ARGS+=(-max-quiet-period="${ZTF_RERUN_QUIET_PERIOD:-10m}")
fi
ARCHIVIST_ARGS+=(-group="${GROUP}")
if [[ -n ${ZTF_MAX_RUNTIME:-} ]]; then
    ARCHIVIST_ARGS+=(-max-runtime="${ZTF_MAX_RUNTIME}")
fi

# The uncompressed tar goes in a scratch directory; the compressed one is
# written next to its destination, so the final rename is atomic.
TMP_DIR=$(mktemp -d "${SCRATCH_DIR}/ztf-archivist-scratch_${PROGRAMID}_${ZTF_TIMESTAMP}_XXXXXXXXXX")
TMP_TAR="${TMP_DIR}/ztf-go-archivist_tmp_${PROGRAMID}_${ZTF_TIMESTAMP}.tar"
TMP_TGZ="${DEST_DIR}/.tmp.${DEST_NAME}.$$"

cleanup() {
    rm -rf "${TMP_DIR}" "${TMP_TGZ}"
}
trap cleanup EXIT

log "invoking ztf-go-archivist: ${ARCHIVIST_ARGS[*]} -dest=${TMP_TAR}"
if ! "${ARCHIVIST_BIN}" "${ARCHIVIST_ARGS[@]}" -dest="${TMP_TAR}"; then
    log "ztf-go-archivist failed; not publishing ${DEST_NAME}"
    exit 1
fi

log "gzipping result"
gzip --best --to-stdout "${TMP_TAR}" > "${TMP_TGZ}"
chmod 664 "${TMP_TGZ}"
MD5=$(md5sum < "${TMP_TGZ}" | cut -d' ' -f1)

log "moving file into place"
check_destination
mv -f "${TMP_TGZ}" "${DESTINATION}"

log "recording md5 checksum"
# One line per tarball, bare filename, so `md5sum -c MD5SUMS` works from
# inside the directory. Older lines may carry an absolute path; a line for
# this file in either form is replaced. The lock keeps a nightly run and a
# rerun from rewriting the file at the same time.
MD5SUMS="${DEST_DIR}/MD5SUMS"
(
    flock 9
    TMP_SUMS="${DEST_DIR}/.tmp.MD5SUMS.$$"
    if [[ -f $MD5SUMS ]]; then
        awk -v name="${DEST_NAME}" '{ f = $2; sub(/.*\//, "", f); if (f != name) print }' \
            "${MD5SUMS}" > "${TMP_SUMS}"
    else
        : > "${TMP_SUMS}"
    fi
    echo "${MD5}  ${DEST_NAME}" >> "${TMP_SUMS}"
    chmod 664 "${TMP_SUMS}"
    mv -f "${TMP_SUMS}" "${MD5SUMS}"
) 9> "${SCRATCH_DIR}/.MD5SUMS_${PROGRAM_CLASS}.lock"

log "done: ${DESTINATION}"
