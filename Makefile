default: all

.PHONY: all
all: clean build check test

VERSION := `git describe --abbrev=0 --tags || echo "0.0.0"`
BUILD := `git rev-parse --short HEAD`
LDFLAGS=-ldflags "-X=github.com/peak/s5cmd/v2/version.Version=$(VERSION) -X=github.com/peak/s5cmd/v2/version.GitCommit=$(BUILD)"

TEST_TYPE:=test_with_race
ifeq ($(OS),Windows_NT)
	TEST_TYPE=test_without_race
endif

semgrep ?= -
ifeq (,$(shell which semgrep))
	semgrep=echo "-- Running inside Docker --"; docker run --rm -v $$(pwd):/src returntocorp/semgrep:1.65.0 semgrep
else
	semgrep=semgrep
endif

.PHONY: build
build:
	@go build ${GCFLAGS} ${LDFLAGS} -mod=vendor .

.PHONY: test
test: $(TEST_TYPE)

.PHONY: test_with_race
test_with_race:
	@S5CMD_BUILD_BINARY_WITHOUT_RACE_FLAG=0 go test -mod=vendor -count=1 -race ./...

.PHONY: test_without_race
test_without_race:
	@S5CMD_BUILD_BINARY_WITHOUT_RACE_FLAG=1 go test -mod=vendor -count=1 ./...

##@ Bootstrap
# See following issues for why errors are ignored with `-e` flag:
# 	* https://github.com/golang/go/issues/61857
# 	* https://github.com/golang/go/issues/59186
.PHONY: bootstrap
bootstrap: ## Install tooling
	@go install $$(go list -e -f '{{join .Imports " "}}' ./internal/tools/tools.go)

.PHONY: check
check: vet staticcheck unparam semgrep check-fmt check-codegen check-gomod

.PHONY: staticcheck
staticcheck:
	@staticcheck -checks 'all,-ST1000' ./...

.PHONY: unparam
unparam:
	@unparam ./...

.PHONY: semgrep
semgrep: ## Run semgrep
	@$(semgrep) --quiet --metrics=off --error --config="r/dgryski.semgrep-go" --config .github/semgrep-rules.yaml .

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

.PHONY: check-gomod
check-gomod: ## Check go.mod file
	@go mod tidy
	@git diff --exit-code -- go.sum go.mod

.PHONY: gogenerate
gogenerate:
	@go generate -mod vendor ./...

.PHONY: clean
clean:
	@rm -f ./s5cmd

.NOTPARALLEL:
