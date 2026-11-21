.PHONY: build generate lint test

build:
	go build -o ./bin/ztf-go-archivist .

generate:
	go generate ./schema

lint:
	go vet ./...

test:
	./tests/test_scripts.sh
