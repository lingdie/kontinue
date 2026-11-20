SHELL := /bin/bash
BIN_DIR := $(CURDIR)/bin
GO := go
CARGO := $(shell if [ -x "$(CURDIR)/.toolchain/cargo/bin/cargo" ]; then echo "$(CURDIR)/.toolchain/cargo/bin/cargo"; else echo "cargo"; fi)
export GOPATH := $(CURDIR)/.gopath
export GOCACHE := $(CURDIR)/.gocache
export RUSTUP_HOME := $(CURDIR)/.toolchain/rustup
export CARGO_HOME := $(CURDIR)/.toolchain/cargo
export PATH := $(CURDIR)/.toolchain/cargo/bin:$(PATH)

GO_BINARIES := kontinue-apiserver kontinue-gateway kontinued kontinue-cri-shim ktctl
RUST_BINARIES := ktd kontinue-runsc kontinue-arena-helper kontinue-carrier

.PHONY: all build build-go build-rust build-linux-go test test-go test-rust clean kind-up kind-deploy kind-e2e kind-down kind-cilium bench-overflow

all: build

build: build-go build-rust

build-go:
	@mkdir -p $(BIN_DIR)
	@set -e; for bin in $(GO_BINARIES); do \
		echo "[Go] Building $$bin..."; \
		CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN_DIR)/$$bin ./cmd/$$bin; \
	done

build-rust:
	@mkdir -p $(BIN_DIR)
	@echo "[Rust] Building workspace release binaries..."
	@$(CARGO) build --release --workspace --target-dir $(CURDIR)/target
	@set -e; for bin in $(RUST_BINARIES); do \
		cp $(CURDIR)/target/release/$$bin $(BIN_DIR)/$$bin; \
	done

build-linux-go:
	@mkdir -p $(BIN_DIR)/linux-amd64 $(BIN_DIR)/linux-arm64
	@set -e; for bin in $(GO_BINARIES); do \
		echo "[Go] Cross-compiling $$bin (linux/amd64 & linux/arm64)..."; \
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -trimpath -o $(BIN_DIR)/linux-amd64/$$bin ./cmd/$$bin; \
		CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -trimpath -o $(BIN_DIR)/linux-arm64/$$bin ./cmd/$$bin; \
	done

test: test-go test-rust

test-go:
	CGO_ENABLED=0 $(GO) test -race -v ./...

test-rust:
	$(CARGO) test --workspace

clean:
	rm -rf $(BIN_DIR) target coverage.txt

kind-up:
	./hack/deploy-kind.sh

kind-deploy:
	./hack/deploy-kind.sh

kind-e2e:
	./hack/e2e-kind-test.sh

kind-down:
	kind delete cluster --name kontinue-dev

kind-cilium:
	./hack/setup-kind-cilium.sh

bench-overflow:
	@mkdir -p tmp
	BENCH_MODE=$${BENCH_MODE:-overflow} python3 ./hack/benchmark_overflow.py
