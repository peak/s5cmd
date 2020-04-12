default: all

.PHONY: all
all: clean build test check

.PHONY: build
build:
	@go build ${GCFLAGS} -ldflags "${LDFLAGS}" .

.PHONY: test
test:
	@go test -mod=vendor ./...

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
	@sh -c 'if [ -n "$(go fmt -mod=vendor ./...)" ]; then echo "Go code is not formatted"; exit 1; fi'

.PHONY: mock
mock:
	@mockery -inpkg -dir=storage -name=Storage -case=underscore

.PHONY: clean
clean:
	@rm -f ./s5cmd


.PHONY: release
release:
	@echo "Latest tag is" $$(git describe --tags)
	@echo "Are you sure you want to release '$$version'? [y/N]" && read ans && [ $${ans:-N} = y ]
	rm -rf ./dist/
	git tag $$version
	git push --tags
	goreleaser

.NOTPARALLEL:
