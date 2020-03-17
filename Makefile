SRCDIR ?= .

default: all

.PHONY: all
all: clean build test check

.PHONY: dist
dist: generate all

.PHONY: generate
generate:
	@go generate ${SRCDIR}

.PHONY: build
build:
	@go build ${GCFLAGS} -ldflags "${LDFLAGS}" ${SRCDIR}

.PHONY: test
test:
	@go test -mod=vendor ./...

.PHONY: check
check: vet staticcheck check-fmt

.PHONY: staticcheck
staticcheck:
	@staticcheck -checks 'inherit,-SA4009,-U1000' ./...

.PHONY: vet
vet:
	@go vet -mod=vendor ./...

.PHONY: check-fmt
check-fmt:
	@sh -c 'unfmt_files="$$(go fmt ./...)"; if [ -n "$$unfmt_files"  ]; then echo "$$unfmt_files"; echo "Go code is not formatted, run <make fmt>"; exit 1; fi'

.PHONY: clean
clean:
	@rm -f ${SRCDIR}/s5cmd

.NOTPARALLEL:
