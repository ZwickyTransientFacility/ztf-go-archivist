.PHONY: build generate lint

build:
	go build -o ./bin/ztf-new-tarball ./cmd/ztf-new-tarball
	cp ./bin/ztf-new-tarball ./bin/ztf-go-archivist

generate:
	go generate ./internal/schema

lint:
	go vet ./...
