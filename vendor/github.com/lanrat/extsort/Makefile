default: test

RELEASE_DEPS=test fmt lint examples readme 

include release.mk

ALL_SOURCES := $(shell find . -type f -name '*.go')

.PHONY: fmt lint test cover coverhtml examples readme

test:
	go test -timeout=60s $(shell go list ./... | grep -v "/examples") 
	@echo "< ALL TESTS PASS >"

update-deps: go.mod
	GOPROXY=direct go get -u ./...
	go mod tidy

deps: go.mod
	go mod download

fmt:
	go fmt ./...

coverage.out: $(ALL_SOURCES)
	go test -coverprofile=coverage.out $(shell go list ./... | grep -v examples)

cover: coverage.out
	go tool cover -func=coverage.out

coverhtml: coverage.out
	go tool cover -html=coverage.out

lint:
	golangci-lint run ./...

benchmark:
	./run_benchmarks.sh

examples:
	@for dir in examples/*/; do \
		if [ -f "$$dir"*.go ]; then \
			echo "Running example in $$dir"; \
			(cd "$$dir" && go run *.go > /dev/null); \
		fi; \
	done

readme:
	go run examples/update_readme_examples.go
