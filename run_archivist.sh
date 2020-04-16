#!/bin/bash

set -euo pipefail

log() {
    echo "$(date) | run_archivist.sh | $@"
}

log "starting archivist run"

ZTF_BROKER="partnership.alerts.ztf.uw.edu:9092"
ZTF_TIMESTAMP=$(TZ=UTC printf '%(%Y%m%d)T' -1)
ZTF_TOPIC="ztf_${ZTF_TIMESTAMP}_programid1"
TAR_DESTINATION="/astro/users/swnelson/ztf-archive/ztf_public_${ZTF_TIMESTAMP}.tar"
GROUPID="ztf-go-archivist"
log "topic: ${ZTF_TOPIC}"
log "tar destination: ${TAR_DESTINATION}"
mkdir -p $(dirname ${TAR_DESTINATION})

log "invoking ztf-go-archivist"
set -x
/astro/users/swnelson/bin/ztf-go-archivist \
    -broker="${ZTF_BROKER}" \
    -group="ztf-go-archivist" \
    -topic="${ZTF_TOPIC}" \
    -dest="${TAR_DESTINATION}"

set +x

log "gzipping result"
gzip --verbose --best "${TAR_DESTINATION}"

log "done"
