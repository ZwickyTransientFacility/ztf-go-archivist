#!/bin/bash
set -e -o pipefail
export TZ=UTC

log() {
    echo "$(date --rfc-3339=ns) | append_missing_data.sh | $@"
}

# Usage: append_missing_data.sh
#
# Appends any unwritten data to existing ZTF tarballs for the last 7 days.

set -u


ztf_timestamp() {
    # Returns the ZTF-style date (YYYYMMDD) N days ago.
    TZ=UTC echo $(date -d "$1 days ago" "+%Y%m%d")
}

ztf_tarball_path() {
    TIMESTAMP=$1
    PROGRAM_CLASS=$2
    echo "/epyc/data/ztf/alerts/${PROGRAM_CLASS}/ztf_${PROGRAM_CLASS}_${TIMESTAMP}.tar.gz"
}

ztf_program_class_to_id() {
    if [[ $1 = "public" ]]; then
        echo "programid1"
    else
        echo "programid2"
    fi
}

TARGETS=()
TARFILES=()
for DAYS_AGO in 1 2 3 4 5 6 7; do
    TIMESTAMP=$(ztf_timestamp $DAYS_AGO)
    for PROGRAM in public partnership; do
        PROGRAM_ID=$(ztf_program_class_to_id PROGRAM)

        TOPIC="ztf_${TIMESTAMP}_{PROGRAM_ID}"
        TARFILE="/epyc/data/ztf/alerts/public/ztf_${PROGRAM}_${TIMESTAMP}.tar.gz"
        TARFILES+=($TARFILE)
        TARGETS+=(-target="${TOPIC}:${TARFILE}")
    done
done

set -x
/epyc/projects/ztf-go-archivist/bin/ztf-tarball-append-missing-data \
    -broker="partnership.alerts.ztf.uw.edu:9092" \
    -group="${ZTF_ARCHIVIST_GROUP:-ztf-go-archivist}" \
    "${TARGETS[@]}"
set +x

log "updating md5 checksums"
for DESTINATION in "${TARFILES[@]}"; do
    NEW_SUM=$(md5sum "${DESTINATION}")
    sed --in-place "/$(basename $DESTINATION)/c\\$NEW_SUM" $(dirname ${DESTINATION})/MD5SUMS
done

log "done"
