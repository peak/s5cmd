#
# This Makefile is used for development only.
# For installation, refer to the Installation section in README.md.
#

SRCDIR ?= .

default: all

all: fmt build

dist: generate all

fmt:
	find ${SRCDIR} ! -path "*/vendor/*" -type f -name '*.go' -exec gofmt -l -s -w {} \;

generate:
	go generate ${SRCDIR}

build:
	go build ${GCFLAGS} -ldflags "${LDFLAGS}" ${SRCDIR}

test:
	go test -mod=vendor ./...

clean:
	rm -vf ${SRCDIR}/s5cmd

.PHONY: all dist fmt generate build clean

.NOTPARALLEL:
