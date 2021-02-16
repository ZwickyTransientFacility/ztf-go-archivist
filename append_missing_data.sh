#!/bin/bash
set -e -o pipefail
export TZ=UTC

log() {
    echo "$(date --rfc-3339=ns) | append_missing_data.sh | $@"
}

log "invoking ztf-go-archivist"


# Usage: append_missing_data.sh PROGRAMID DATE
#
# Appends any unwritten data to an existing ZTF tarball.
#
# PROGRAMID should be 'programid1' for public data, and 'programid2'
# for partnerships data.
#
# DATE should be a ZTF-style timestamp for the day to check for unwritten data.

# Parse input arguments to the script:
if [[ -z $1 ]]; then
    echo "usage: append_missing_data.sh PROGRAMID DATE"
    exit 1
fi

if [[ -z $2 ]]; then
    echo "usage: append_missing_data.sh PROGRAMID DATE"
    exit 1
fi

set -u

PROGRAMID=$1
ZTF_TIMESTAMP=$2

if [[ $PROGRAMID = "programid1" ]]; then
    ZTF_TOPIC="ztf_${ZTF_TIMESTAMP}_programid1"
    DESTINATION="/epyc/data/ztf/alerts/public/ztf_public_${ZTF_TIMESTAMP}.tar.gz"
elif [[ $PROGRAMID = "programid2" ]]; then
    ZTF_TOPIC="ztf_${ZTF_TIMESTAMP}_programid2"
    DESTINATION="/epyc/data/ztf/alerts/partnership/ztf_partnership_${ZTF_TIMESTAMP}.tar.gz"
else
    echo "Invalid argument: PROGRAMID should be either 'programid1' or 'programid2' (got '$PROGRAMID')"
    exit 1
fi


# Make a temporary directory where we copy the existing tar file, and then move
# it into place at the end.

TMP_DIR=$(mktemp --directory "/epyc/ssd/tmp/ztf-archivist-scratch_${PROGRAMID}_${ZTF_TIMESTAMP}_XXXXXXXXXX")
TMP_TGZ="${TMP_DIR}/ztf-go-archivist_tmp_${PROGRAMID}_${ZTF_TIMESTAMP}.tar.gz"

cp $DESTINATION $TMP_TGZ

set -x
/epyc/projects/ztf-go-archivist/bin/ztf-go-archivist \
    -broker="partnership.alerts.ztf.uw.edu:9092" \
    -group="${ZTF_ARCHIVIST_GROUP:-ztf-go-archivist}" \
    -topic="${ZTF_TOPIC}" \
    -dest="${TMP_TGZ}" \
    -max-quiet-period=10m \
    -max-runtime=2h
set +x

log "moving file into place"
mv --force "${TMP_TGZ}" "${DESTINATION}"

log "updating md5 checksum"
NEW_SUM=$(md5sum "${DESTINATION}")
sed --in-place "/$(basename $DESTINATION)/c\\$NEW_SUM" $(dirname ${DESTINATION})/MD5SUMS

log "cleaning up"
rm --recursive --force "${TMP_DIR}"

log "done"
