# ztf-go-archivist #

A small Go program which reads data from ZTF's alert stream and writes it to a
.tar file.

Building requires `go1.13+`. Clone the repo, and then run `go build .`.

A Makefile is provided with aliases to lint, to buil, and to generate Avro
schema code.
