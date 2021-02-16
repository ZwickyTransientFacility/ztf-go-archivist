#!/bin/bash

set -eo pipefail

log() {
    echo "$(date --rfc-3339=ns) | run_archivist.sh | $@"
}

log "invoking ztf-go-archivist"

# Usage: run_archivist.sh PROGRAMID [DATE]
#
# PROGRAMID should be 'programid1' for public data, and 'programid2'
# for partnerships data.
#
# DATE is optional. If unset, today is used. If set, it should be a ZTF-style
# timestamp for the day to rerun data.
if [[ -z $1 ]]; then
    echo "usage: run_archivist.sh PROGRAMID [DATE]"
    exit 1
fi

if [[ -z $2 ]]; then
    ZTF_TIMESTAMP=$(TZ=UTC printf '%(%Y%m%d)T' -1)
else
    ZTF_TIMESTAMP=$2
fi

set -u

PROGRAMID=$1
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


# Make a temporary directory where we create the tar file, and then move it into
# place at the end.

TMP_DIR=$(mktemp -d "/epyc/ssd/tmp/ztf-archivist-scratch_${PROGRAMID}_${ZTF_TIMESTAMP}_XXXXXXXXXX")
TMP_TGZ="${TMP_DIR}/ztf-go-archivist_tmp_${PROGRAMID}_${ZTF_TIMESTAMP}.tar.gz"
set -x
/epyc/projects/ztf-go-archivist/bin/ztf-go-archivist \
    -broker="partnership.alerts.ztf.uw.edu:9092" \
    -group="${ZTF_ARCHIVIST_GROUP:-ztf-go-archivist}" \
    -topic="${ZTF_TOPIC}" \
    -dest="${TMP_TGZ}"
set +x

log "moving file into place"
mv "${TMP_TGZ}" "${DESTINATION}"

log "adding md5 checksum"
md5sum "${DESTINATION}" >> $(dirname ${DESTINATION})/MD5SUMS

log "cleaning up"
rm -rf "${TMP_DIR}"

log "done"
