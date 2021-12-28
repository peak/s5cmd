default: all

.PHONY: all
all: clean build test check

VERSION := `git describe --abbrev=0 --tags || echo "0.0.0"`
BUILD := `git rev-parse --short HEAD`
LDFLAGS=-ldflags "-X=github.com/afontani/s5cmd/version.Version=$(VERSION) -X=github.com/afontani/s5cmd/version.GitCommit=$(BUILD)"

.PHONY: build
build:
	@go build ${GCFLAGS} ${LDFLAGS} -mod=vendor .

RACE_FLAG := -race
ifeq ($(OS),Windows_NT)
	RACE_FLAG =
endif

.PHONY: test
test:
	@go test -mod=vendor -count=1 ${RACE_FLAG} ./...

.PHONY: check
check: vet staticcheck unparam check-fmt

.PHONY: staticcheck
staticcheck:
	@staticcheck -checks 'all,-U1000,-ST1000,-ST1003' ./...

.PHONY: unparam
unparam:
	@unparam ./...

.PHONY: vet
vet:
	@go vet -mod=vendor ./...

.PHONY: check-fmt
check-fmt:
	@if [ $$(go fmt -mod=vendor ./...) ]; then\
		echo "Go code is not formatted";\
		exit 1;\
	fi

.PHONY: mock
mock:
	@mockery -inpkg -dir=storage -name=Storage -case=underscore

.PHONY: clean
clean:
	@rm -f ./s5cmd

.NOTPARALLEL:
