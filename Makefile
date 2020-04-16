.PHONY: build generate lint

build:
	go build -o ./bin/ztf-go-archivist .

generate:
	go generate ./schema

lint:
	go vet ./...
