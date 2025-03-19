VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio/conduit-connector-influxdb.version=${VERSION}'" -o conduit-connector-influxdb cmd/connector/main.go

.PHONY: test
test:
	# run required docker containers, execute integration tests, stop containers after tests
	docker compose -f test/docker-compose.yml up --force-recreate --quiet-pull -d --wait
	go test $(GOTEST_FLAGS) -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down --volumes; \
		exit $$ret

.PHONY: generate
generate:
	go generate ./...
	conn-sdk-cli readmegen -w

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

.PHONY: fmt
fmt:
	gofumpt -l -w .

.PHONY: lint
lint:
	golangci-lint run -v
