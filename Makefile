#
# This Makefile is used for development only.
# For installation, refer to the Installation section in README.md.
#

SRCDIR ?= .

default: all

.PHONY: all
all: fmt build staticcheck test

.PHONY: dist
dist: generate all

.PHONY: fmt
fmt:
	find ${SRCDIR} ! -path "*/vendor/*" -type f -name '*.go' -exec gofmt -l -s -w {} \;

.PHONY: generate
generate:
	go generate ${SRCDIR}

.PHONY: build
build:
	go build ${GCFLAGS} -ldflags "${LDFLAGS}" ${SRCDIR}

.PHONY: test
test:
	go test -mod=vendor ./...

.PHONY: staticcheck
staticcheck:
	staticcheck -tags=integration -checks all ./...

.PHONY: check-vet
check-vet:
	@go vet -mod=vendor ./...

.PHONY: lean
clean:
	rm -vf ${SRCDIR}/s5cmd

.NOTPARALLEL:
