#!/bin/bash
set -e -o pipefail
export TZ=UTC

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

for DAYS_AGO in 1 2 3 4 5 6 7; do
    TIMESTAMP=$(ztf_timestamp $DAYS_AGO)
    for PROGRAM in public partnership; do
        PROGRAM_ID=$(ztf_program_class_to_id $PROGRAM)
        TARBALL_PATH=$(ztf_tarball_path $TIMESTAMP $PROGRAM)
        if [ ! -f $TARBALL_PATH ]; then
            echo "$TARBALL_PATH is missing, rerunning"
            /epyc/projects/ztf-go-archivist/bin/run_archivist.sh $PROGRAM_ID $TIMESTAMP
        fi
    done
done
