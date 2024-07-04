default: all

.PHONY: all
all: clean build test check checkgomod

VERSION := `git describe --abbrev=0 --tags || echo "0.0.0"`
BUILD := `git rev-parse --short HEAD`
LDFLAGS=-ldflags "-X=github.com/peak/s5cmd/v2/version.Version=$(VERSION) -X=github.com/peak/s5cmd/v2/version.GitCommit=$(BUILD)"

.PHONY: build
build:
	@go build ${GCFLAGS} ${LDFLAGS} -mod=vendor .

TEST_TYPE:=test_with_race
ifeq ($(OS),Windows_NT)
	TEST_TYPE=test_without_race
endif

.PHONY: test
test: $(TEST_TYPE)

.PHONY: test_with_race
test_with_race:
	@S5CMD_BUILD_BINARY_WITHOUT_RACE_FLAG=0 go test -mod=vendor -count=1 -race ./...

.PHONY: test_without_race
test_without_race:
	@S5CMD_BUILD_BINARY_WITHOUT_RACE_FLAG=1 go test -mod=vendor -count=1 ./...

.PHONY: check
check: vet staticcheck unparam check-fmt check-codegen

.PHONY: staticcheck
staticcheck:
	@staticcheck -checks 'all,-ST1000' ./...

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

.PHONY: check-codegen
check-codegen: gogenerate ## Check generated code is up-to-date
	@git diff --exit-code --

.PHONY: gogenerate
gogenerate:
	@go generate -mod vendor ./...

.PHONY: clean
clean:
	@rm -f ./s5cmd

.PHONY: checkgomod
checkgomod: ## Check go.mod file
	@go mod tidy
	@git diff --exit-code -- go.sum go.mod

.NOTPARALLEL:
