# ztf-go-archivist #

A small Go program which reads data from ZTF's alert stream and writes it to a
.tar file, and the shell scripts that run it every night to build the ZTF
alert archive.

Building requires `go1.13+`. Clone the repo, and then run `go build .`.

A Makefile is provided with aliases to lint, to build, to test the scripts,
and to generate Avro schema code.

## Nightly scripts

- `run_archivist.sh [--force] PROGRAMID [DATE]` archives one night of one
  program (`programid1` is public, `programid2` is partnership) to
  `ztf_{public,partnership}_YYYYMMDD.tar.gz` and records its checksum in
  `MD5SUMS`. It publishes nothing if the archivist fails, and will not replace
  an existing tarball without `--force`.
- `rerun_archivist.sh` re-archives any night in the last 7 days whose tarball
  is missing.
- `nightly.sh` is what the scheduler runs: both programs for tonight plus the
  rerunner, in parallel, one log file each. On failure it posts to a Slack
  webhook if one is configured.

Paths, the broker and the consumer group come from environment variables;
each script's header lists them. `crontab` is an example schedule.

`make test` runs `tests/test_scripts.sh`, which exercises the scripts against
a stub in place of the Go binary. It needs no Kafka and no network.

The scripts are deployed by
[middle-earth-ztf](https://github.com/dirac-institute/middle-earth-ztf),
which builds this repo into a container at a pinned commit.
