BUILD_OS ?= 
export GOPATH ?= /tmp
export PATH := $(PATH):$(GOPATH)/bin

all: deps install check test

deps:
	go get -v github.com/constabulary/gb/...
	go get -v github.com/golang/lint/golint

install:
	GOOS=$(BUILD_OS) $(GOPATH)/bin/gb build cmd/...

test:
	$(GOPATH)/bin/gb test utils/... -gocheck.v -test.short

check:
	go vet ./src/cmd/...
	go vet ./src/utils/...
	$(GOPATH)/bin/golint ./src/cmd/...
	$(GOPATH)/bin/golint ./src/utils/...

clean:
	@rm -rf bin pkg tmp

docker-build:
	@rm -rf bin/ tmp/
	@mkdir -p bin tmp
	docker run --rm \
		-e BUILD_OS=$(shell uname -s | tr A-Z a-z) \
		-v "$$PWD/src":/s3datagen/src \
		-v "$$PWD/vendor":/s3datagen/vendor \
		-v "$$PWD/Makefile":/s3datagen/Makefile \
		-v "$$PWD/tmp":/s3datagen/bin \
		-w /s3datagen \
		golang:1.7 \
		make deps install check test
	@cp tmp/* bin/
	@rm -rf tmp/

.PHONY: deps install check test clean docker-build
