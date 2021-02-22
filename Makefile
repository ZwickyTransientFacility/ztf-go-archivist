.PHONY: build generate lint

build:
	go build -o ./bin/ztf-new-tarball ./cmd/ztf-new-tarball
	cp ./bin/ztf-new-tarball ./bin/ztf-go-archivist
	go build -o ./bin/ztf-tarball-append-missing-data ./cmd/ztf-tarball-append-missing-data

generate:
	go generate ./internal/schema

lint:
	go vet ./...
