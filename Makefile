.PHONY: build generate lint

build:
	go build .

generate:
	go generate ./schema

lint:
	go vet ./...
