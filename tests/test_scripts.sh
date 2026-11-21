#!/bin/bash
#
# Tests for run_archivist.sh, rerun_archivist.sh and nightly.sh, using a stub
# in place of the ztf-go-archivist binary. No Kafka, no network.
#
#   tests/test_scripts.sh
#
# Exits non-zero if any check fails.

set -uo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

PASS=0
FAIL=0
check() {
    # check DESCRIPTION COMMAND...
    local desc=$1
    shift
    if "$@" > /dev/null 2>&1; then
        PASS=$((PASS + 1))
        echo "ok   - $desc"
    else
        FAIL=$((FAIL + 1))
        echo "FAIL - $desc"
    fi
}
not() { ! "$@"; }

# A stub archivist: records its arguments, writes a one-entry tar to -dest,
# and exits with $STUB_EXIT (default 0). With STUB_EXIT != 0 it still leaves
# a partial tar behind, as the real binary does when Kafka fails mid-run.
mkdir -p "$WORK/bin"
cat > "$WORK/bin/stub-archivist" <<'EOF'
#!/bin/bash
echo "$*" >> "$STUB_CALLS"
for a in "$@"; do
    case $a in -dest=*) dest=${a#-dest=} ;; esac
done
d=$(mktemp -d)
echo "${STUB_CONTENT:-alert}" > "$d/1.avro"
tar -cf "$dest" -C "$d" 1.avro
rm -rf "$d"
exit "${STUB_EXIT:-0}"
EOF
chmod +x "$WORK/bin/stub-archivist"

setup() {
    rm -rf "$WORK/data"
    mkdir -p "$WORK/data/alerts/public" "$WORK/data/alerts/partnership" \
        "$WORK/data/scratch" "$WORK/data/logs"
    export ZTF_ARCHIVE_ROOT="$WORK/data/alerts"
    export ZTF_SCRATCH_DIR="$WORK/data/scratch"
    export ZTF_LOG_DIR="$WORK/data/logs"
    export ZTF_ARCHIVIST_BIN="$WORK/bin/stub-archivist"
    export ZTF_SLACK_WEBHOOK_FILE="$WORK/no-such-webhook"
    export STUB_CALLS="$WORK/calls"
    : > "$STUB_CALLS"
    unset STUB_EXIT STUB_CONTENT ZTF_ARCHIVIST_GROUP ZTF_MAX_RUNTIME ZTF_MAX_QUIET_PERIOD
}

TODAY=$(date -u +%Y%m%d)
days_ago() { date -u -d "$1 days ago" +%Y%m%d; }
PUB="$WORK/data/alerts/public"
PART="$WORK/data/alerts/partnership"

echo "# run_archivist.sh: tonight's run"
setup
check "succeeds" "$REPO_DIR/run_archivist.sh" programid1
check "publishes the public tarball" test -f "$PUB/ztf_public_${TODAY}.tar.gz"
check "tarball is a valid gzip" gzip -t "$PUB/ztf_public_${TODAY}.tar.gz"
check "tarball holds the alert" sh -c "tar -tzf '$PUB/ztf_public_${TODAY}.tar.gz' | grep -qx 1.avro"
check "MD5SUMS has one bare-filename line" \
    sh -c "[ \"\$(cat '$PUB/MD5SUMS' | wc -l)\" = 1 ] && grep -q '  ztf_public_${TODAY}.tar.gz\$' '$PUB/MD5SUMS'"
check "md5sum -c passes from inside the directory" sh -c "cd '$PUB' && md5sum -c MD5SUMS"
check "uses the right topic" grep -q -- "-topic=ztf_${TODAY}_programid1" "$STUB_CALLS"
check "uses the default group" grep -q -- "-group=ztf-go-archivist\( \|\$\)" "$STUB_CALLS"
check "does not shorten the quiet period" not grep -q -- "-max-quiet-period" "$STUB_CALLS"
check "leaves no scratch directory" sh -c "[ -z \"\$(ls '$WORK/data/scratch')\" ]"
check "leaves no hidden temp file" sh -c "[ -z \"\$(find '$PUB' -name '.tmp.*')\" ]"

setup
ZTF_ARCHIVIST_GROUP=my-group ZTF_MAX_RUNTIME=1m "$REPO_DIR/run_archivist.sh" programid2 > /dev/null 2>&1
check "programid2 goes to partnership" test -f "$PART/ztf_partnership_${TODAY}.tar.gz"
check "honours ZTF_ARCHIVIST_GROUP" grep -q -- "-group=my-group" "$STUB_CALLS"
check "honours ZTF_MAX_RUNTIME" grep -q -- "-max-runtime=1m" "$STUB_CALLS"

echo "# run_archivist.sh: failure"
setup
STUB_EXIT=1 "$REPO_DIR/run_archivist.sh" programid1 > /dev/null 2>&1
check "exits non-zero when the binary fails" test $? -ne 0
check "publishes no tarball" not test -e "$PUB/ztf_public_${TODAY}.tar.gz"
check "writes no MD5SUMS" not test -e "$PUB/MD5SUMS"
check "leaves no scratch directory" sh -c "[ -z \"\$(ls '$WORK/data/scratch')\" ]"
check "leaves no hidden temp file" sh -c "[ -z \"\$(find '$PUB' -name '.tmp.*')\" ]"

echo "# run_archivist.sh: bad arguments"
setup
check "rejects no arguments" not "$REPO_DIR/run_archivist.sh"
check "rejects programid3" not "$REPO_DIR/run_archivist.sh" programid3
check "rejects a malformed date" not "$REPO_DIR/run_archivist.sh" programid1 2026-11-20
check "never called the binary" not test -s "$STUB_CALLS"

echo "# run_archivist.sh: existing tarball"
setup
"$REPO_DIR/run_archivist.sh" programid1 > /dev/null 2>&1
FIRST_MD5=$(md5sum < "$PUB/ztf_public_${TODAY}.tar.gz")
: > "$STUB_CALLS"
STUB_CONTENT=different "$REPO_DIR/run_archivist.sh" programid1 > /dev/null 2>&1
check "refuses to replace it (exit 3)" test $? -eq 3
check "does not call the binary" not test -s "$STUB_CALLS"
check "leaves the tarball unchanged" test "$(md5sum < "$PUB/ztf_public_${TODAY}.tar.gz")" = "$FIRST_MD5"
STUB_CONTENT=different "$REPO_DIR/run_archivist.sh" --force programid1 > /dev/null 2>&1
check "--force replaces it" test "$(md5sum < "$PUB/ztf_public_${TODAY}.tar.gz")" != "$FIRST_MD5"
check "MD5SUMS still has one line for it" test "$(grep -c "ztf_public_${TODAY}" "$PUB/MD5SUMS")" = 1
check "md5sum -c passes after the replace" sh -c "cd '$PUB' && md5sum -c MD5SUMS"

echo "# run_archivist.sh: MD5SUMS with legacy lines"
setup
D3=$(days_ago 3)
{
    echo "0123456789abcdef0123456789abcdef  ztf_public_20180601.tar.gz"
    echo "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa  /epyc/data/ztf/alerts/public/ztf_public_${D3}.tar.gz"
    echo "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  /epyc/data/ztf/alerts/public/ztf_public_20200101.tar.gz"
} > "$PUB/MD5SUMS"
"$REPO_DIR/run_archivist.sh" programid1 "$D3" > /dev/null 2>&1
check "replaces an absolute-path line for the same file" \
    sh -c "[ \"\$(grep -c 'ztf_public_${D3}' '$PUB/MD5SUMS')\" = 1 ] && ! grep -q aaaaaaaa '$PUB/MD5SUMS'"
check "keeps other lines untouched" \
    sh -c "grep -q '^0123456789abcdef0123456789abcdef  ztf_public_20180601.tar.gz\$' '$PUB/MD5SUMS' \
        && grep -q '^bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb  /epyc/data/ztf/alerts/public/ztf_public_20200101.tar.gz\$' '$PUB/MD5SUMS'"
check "file has three lines" test "$(wc -l < "$PUB/MD5SUMS")" = 3

echo "# run_archivist.sh: past-date run"
check "uses the past date's topic" grep -q -- "-topic=ztf_${D3}_programid1" "$STUB_CALLS"
check "uses a fresh rerun group" grep -q -- "-group=ztf-go-archivist-rerun-" "$STUB_CALLS"
check "uses the short quiet period" grep -q -- "-max-quiet-period=10m" "$STUB_CALLS"

echo "# rerun_archivist.sh"
setup
for d in 1 2 3 4 5 6 7; do
    touch "$PUB/ztf_public_$(days_ago $d).tar.gz" "$PART/ztf_partnership_$(days_ago $d).tar.gz"
done
check "does nothing when the week is complete" "$REPO_DIR/rerun_archivist.sh"
check "called the binary zero times" not test -s "$STUB_CALLS"
rm "$PUB/ztf_public_$(days_ago 2).tar.gz" "$PART/ztf_partnership_$(days_ago 5).tar.gz"
check "succeeds with two nights missing" "$REPO_DIR/rerun_archivist.sh"
check "called the binary exactly twice" test "$(wc -l < "$STUB_CALLS")" = 2
check "reran the missing public night" test -s "$PUB/ztf_public_$(days_ago 2).tar.gz"
check "reran the missing partnership night" test -s "$PART/ztf_partnership_$(days_ago 5).tar.gz"
check "did not touch tonight" not test -e "$PUB/ztf_public_${TODAY}.tar.gz"
check "did not look 8 days back" not test -e "$PUB/ztf_public_$(days_ago 8).tar.gz"

setup
for d in 1 2 3 4 5 6 7; do
    touch "$PUB/ztf_public_$(days_ago $d).tar.gz" "$PART/ztf_partnership_$(days_ago $d).tar.gz"
done
rm "$PUB/ztf_public_$(days_ago 4).tar.gz" "$PUB/ztf_public_$(days_ago 1).tar.gz"
STUB_EXIT=1 "$REPO_DIR/rerun_archivist.sh" > /dev/null 2>&1
check "exits non-zero when a rerun fails" test $? -ne 0
check "still tried every missing night" test "$(wc -l < "$STUB_CALLS")" = 2
check "published nothing for the failed nights" not test -e "$PUB/ztf_public_$(days_ago 4).tar.gz"

echo "# nightly.sh"
setup
for d in 1 2 3 4 5 6 7; do
    touch "$PUB/ztf_public_$(days_ago $d).tar.gz" "$PART/ztf_partnership_$(days_ago $d).tar.gz"
done
mkdir "$WORK/data/scratch/ztf-archivist-scratch_programid1_20200101_OLD"
touch -d "5 days ago" "$WORK/data/scratch/ztf-archivist-scratch_programid1_20200101_OLD"
mkdir "$WORK/data/scratch/ztf-archivist-scratch_programid1_${TODAY}_LIVE"
touch -d "200 days ago" "$WORK/data/logs/programid1_20200101.log"
touch -d "5 days ago" "$PUB/.tmp.ztf_public_20200101.tar.gz.123"
check "succeeds" "$REPO_DIR/nightly.sh"
check "published public" test -s "$PUB/ztf_public_${TODAY}.tar.gz"
check "published partnership" test -s "$PART/ztf_partnership_${TODAY}.tar.gz"
check "wrote a log per job" \
    test -s "$WORK/data/logs/programid1_${TODAY}.log" -a -s "$WORK/data/logs/programid2_${TODAY}.log" \
    -a -e "$WORK/data/logs/rerunner_${TODAY}.log"
check "removed the stale scratch directory" not test -e "$WORK/data/scratch/ztf-archivist-scratch_programid1_20200101_OLD"
check "kept the recent scratch directory" test -d "$WORK/data/scratch/ztf-archivist-scratch_programid1_${TODAY}_LIVE"
check "removed the stale hidden temp file" not test -e "$PUB/.tmp.ztf_public_20200101.tar.gz.123"
check "removed the old log" not test -e "$WORK/data/logs/programid1_20200101.log"

setup
for d in 1 2 3 4 5 6 7; do
    touch "$PUB/ztf_public_$(days_ago $d).tar.gz" "$PART/ztf_partnership_$(days_ago $d).tar.gz"
done
# A stand-in for curl that records what would be posted to Slack.
cat > "$WORK/bin/curl" <<EOF
#!/bin/bash
printf '%s\n' "\$@" > "$WORK/curl-args"
EOF
chmod +x "$WORK/bin/curl"
echo "https://hooks.slack.example/services/T000/B000/XXXX" > "$WORK/webhook"
OUT=$(STUB_EXIT=1 ZTF_SLACK_WEBHOOK_FILE="$WORK/webhook" PATH="$WORK/bin:$PATH" "$REPO_DIR/nightly.sh" 2>&1)
check "exits non-zero when the runs fail" test $? -ne 0
check "reports programid1's exit code" sh -c "echo \"\$1\" | grep -q 'programid1 exited with code 1'" _ "$OUT"
check "reports programid2's exit code" sh -c "echo \"\$1\" | grep -q 'programid2 exited with code 1'" _ "$OUT"
check "does not report the rerunner" not sh -c "echo \"\$1\" | grep -q 'rerunner exited'" _ "$OUT"
check "posts to the webhook URL" grep -qx "https://hooks.slack.example/services/T000/B000/XXXX" "$WORK/curl-args"
check "posts valid JSON naming the failure" \
    sh -c "grep -- '^{\"text\"' '$WORK/curl-args' | python3 -c 'import json,sys; d=json.load(sys.stdin); assert \"programid1 exited with code 1\" in d[\"text\"]'"
check "does not print the webhook URL" not sh -c "echo \"\$1\" | grep -q hooks.slack" _ "$OUT"

setup
for d in 1 2 3 4 5 6 7; do
    touch "$PUB/ztf_public_$(days_ago $d).tar.gz" "$PART/ztf_partnership_$(days_ago $d).tar.gz"
done
rm -f "$WORK/curl-args"
PATH="$WORK/bin:$PATH" ZTF_SLACK_WEBHOOK_FILE="$WORK/webhook" "$REPO_DIR/nightly.sh" > /dev/null 2>&1
check "does not post to Slack on success" not test -e "$WORK/curl-args"

echo
echo "$PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
