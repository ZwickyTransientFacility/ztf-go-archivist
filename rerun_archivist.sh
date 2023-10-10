#!/bin/bash
set -e -o pipefail
export TZ=UTC

ztf_timestamp() {
    # Returns the ZTF-style date (YYYYMMDD) N days ago.
    TZ=UTC echo $(date -d "$1 days ago" "+%Y%m%d")
}

ztf_tarball_path() {
    # echoes 'yes' if a tarball exists for given timestamp and program class,
    # else echoes 'no'.
    #
    # Timestamp should be ZTF-style, that is, it should be like YYYYMMDD. The
    # program class should be 'public' or 'partnership'.
    #
    # Example: ztf_tarball_exists 20200526 public
    BASE_DIR="/astro/users/ecbellm/tmp_ztf_alert_archive/"
    #BASE_DIR="/epyc/data/ztf/alerts/"

    TIMESTAMP=$1
    PROGRAM_CLASS=$2
    echo "${BASE_DIR}/${PROGRAM_CLASS}/ztf_${PROGRAM_CLASS}_${TIMESTAMP}.tar.gz"
}

ztf_program_class_to_id() {
    if [[ $1 = "public" ]]; then
        echo "programid1"
    else
        echo "programid2"
    fi
}

# When running the archivist, override the default group ID so that we start
# from the oldest offset, regardless of any progress made.
ZTF_ARCHIVIST_GROUP="ztf-go-archivist-rerun-$(date '+%Y%m%d')"
export ZTF_ARCHIVIST_GROUP

for DAYS_AGO in 0 1 2 3 4 5 6 7; do
    TIMESTAMP=$(ztf_timestamp $DAYS_AGO)
    for PROGRAM in public partnership; do
        TARBALL_PATH=$(ztf_tarball_path $TIMESTAMP $PROGRAM)
        if [ ! -f $TARBALL_PATH ]; then
            echo "$TARBALL_PATH is missing, rerunning"
            PROGRAM_ID=$(ztf_program_class_to_id $PROGRAM)
            /astro/users/ecbellm/ztf-go-archivist/run_archivist.sh $PROGRAM_ID $TIMESTAMP
            #/epyc/projects/ztf-go-archivist/bin/run_archivist.sh $PROGRAM_ID $TIMESTAMP
        fi
    done
done
