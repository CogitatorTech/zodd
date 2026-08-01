################################################################################
# Configuration and Variables
################################################################################
ZIG_LOCAL  := $(HOME)/.local/share/zig/0.16.0/zig
ZIG        ?= $(shell test -x $(ZIG_LOCAL) && echo $(ZIG_LOCAL) || which zig)
ZIG_VERSION   := $(shell $(ZIG) version)
BUILD_TYPE    ?= Debug
BUILD_OPTS      = -Doptimize=$(BUILD_TYPE)
JOBS          ?= $(shell nproc || echo 2)
SRC_DIR       := src
TEST_DIR      := tests
BUILD_DIR     := zig-out
CACHE_DIR     := .zig-cache
DOC_SRC       := src/lib.zig
DOC_OUT       := docs/api
RELEASE_MODE := ReleaseSmall

# Get all .zig files in the examples directory and extract their stem names
EXAMPLES      := $(patsubst %.zig,%,$(notdir $(wildcard examples/*.zig)))
EXAMPLE       ?= all

SHELL         := /usr/bin/env bash
.SHELLFLAGS   := -eu -o pipefail -c

################################################################################
# Targets
################################################################################

.PHONY: all build rebuild example test lint format docs docs-serve clean install-deps release help coverage \
 setup-hooks test-hooks web web-serve web-test cli
.DEFAULT_GOAL := help

help: ## Show the help messages for all targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

all: build test lint docs  ## build, test, lint, and docs

build: ## Build project (Mode=$(BUILD_TYPE))
	@echo "Building project in $(BUILD_TYPE) mode with $(JOBS) concurrent jobs..."
	$(ZIG) build $(BUILD_OPTS) -j$(JOBS)

rebuild: clean build  ## clean and build

example: ## Run examples (default: all tests, or 'make example EXAMPLE=e1_transitive_closure')
ifeq ($(EXAMPLE),all)
	@for ex in $(EXAMPLES); do \
		echo "--> Running example: $$ex"; \
		$(ZIG) build run-$$ex $(BUILD_OPTS); \
	done
else
	@echo "--> Running example: $(EXAMPLE)"
	$(ZIG) build run-$(EXAMPLE) $(BUILD_OPTS)
endif

test: ## Run all tests (unit tests and tests in the `tests/` directory)
	@echo "Running tests..."
	$(ZIG) build test $(BUILD_OPTS) -j$(JOBS) --summary all

release: ## Build in Release mode
	@echo "Building the project in Release mode..."
	@$(MAKE) BUILD_TYPE=$(RELEASE_MODE) build

clean: ## Remove docs, build artifacts, and cache directories
	@echo "Removing build artifacts, cache, generated docs, and coverage files..."
	rm -rf $(BUILD_DIR) $(CACHE_DIR) $(DOC_OUT) *.profraw

lint: ## Check code style and formatting of Zig files
	@echo "Running code style checks..."
	$(ZIG) fmt --check $(SRC_DIR) $(TEST_DIR) web/zodd_wasm.zig

cli: ## Build the zodd CLI into zig-out/bin
	@echo "Building the zodd CLI..."
	$(ZIG) build cli $(BUILD_OPTS)

web: ## Build the web frontend Wasm module and stage it under `web/`
	@echo "Building the web frontend Wasm module..."
	$(ZIG) build wasm
	cp $(BUILD_DIR)/web/zodd.wasm web/zodd.wasm
	cp -f logo.svg web/logo.svg

web-serve: web ## Build and serve the web frontend locally
	@echo "Serving web/ at http://localhost:8085 ..."
	python3 -m http.server 8085 --directory web

web-test: web ## Run the Node.js smoke test against the Wasm module
	@echo "Running the Wasm smoke test..."
	node web/smoke_test.mjs

format: ## Format Zig files
	@echo "Formatting Zig files..."
	$(ZIG) fmt .

docs: ## Generate API documentation
	@echo "Generating documentation..."
	$(ZIG) build docs
	@echo "Copying documentation to $(DOC_OUT)..."
	rm -rf $(DOC_OUT)
	mkdir -p $(DOC_OUT)
	cp -r $(BUILD_DIR)/docs/* $(DOC_OUT)

docs-serve: docs ## Serve API documentation locally
	@echo "Serving documentation locally"
	cd $(DOC_OUT) && python3 -m http.server 8000

install-deps: ## Install system dependencies (for Debian-based systems)
	@echo "Installing system dependencies..."
	sudo apt-get update
	sudo apt-get install -y make llvm snapd
	sudo snap install zig  --beta --classic # Use `--edge --classic` to install the latest version

setup-hooks: ## Install Git hooks (pre-commit and pre-push)
	@echo "Installing Git hooks..."
	@pre-commit install --hook-type pre-commit
	@pre-commit install --hook-type pre-push
	@pre-commit install-hooks

test-hooks: ## Run Git hooks on all files manually
	@echo "Running Git hooks..."
	@pre-commit run --all-files
