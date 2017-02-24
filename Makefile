#
# This Makefile is used for development only.
# For installation, refer to the Installation section in README.md.
#

SRCDIR ?= .
GOROOT ?= /usr/local/go

default: all

all: fmt build

dist: generate all

fmt:
	find ${SRCDIR} ! -path "*/vendor/*" -type f -name '*.go' -exec ${GOROOT}/bin/gofmt -l -s -w {} \;

generate:
	${GOROOT}/bin/go generate ${SRCDIR}

build:
	${GOROOT}/bin/go build ${GCFLAGS} -ldflags "${LDFLAGS}" ${SRCDIR}

clean:
	rm -vf ${SRCDIR}/s5cmd

.PHONY: all dist fmt generate build clean

.NOTPARALLEL:
