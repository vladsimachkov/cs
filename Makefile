.PHONY: build build-debug fmt fmt-check clippy test unit test-unit vtl-unit vtl-check hook-test metadata-locked check check-all check-safe clean help install-hooks install-tools setup lint run-metadata run-gateway run-scheduler run-cache run-tape run-node run-node-no-tape

.DEFAULT_GOAL := help

BUILD_MODE ?= release

build:
	@echo "Building all crates ($(BUILD_MODE))..."
	@if [ "$(BUILD_MODE)" = "release" ]; then cargo build --release; else cargo build; fi

build-debug:
	@$(MAKE) BUILD_MODE=debug build

fmt:
	@cargo fmt --all
fmt-check:
	@cargo fmt --all -- --check
clippy:
	@cargo clippy --workspace --all-targets --all-features -- -D warnings
test:
	@cargo test --workspace
unit test-unit:
	@cargo test --workspace --lib --bins
vtl-unit:
	@cargo test -p coldstore-vtl --lib --tests
vtl-check:
	@cargo check -p coldstore-vtl

# Behavior of the AGENTS.md §2.9 review gate. Needs git and bash only, never cargo.
hook-test:
	@scripts/test-commit-msg.sh
check:
	@cargo check --all-targets --all-features
# `cargo metadata --locked` fails when Cargo.lock no longer matches the manifests, which is the
# only mechanical guard now that the lock is committed (AGENTS.md §3.1).
metadata-locked:
	@cargo metadata --locked --format-version 1 > /dev/null
check-all: fmt-check clippy test
check-safe: fmt-check clippy metadata-locked unit vtl-unit build-debug hook-test
	@echo "Safe verification passed (fmt/clippy/lock/unit/vtl-unit/build/hook-test only)."
clean:
	@cargo clean

run-metadata:
	@cargo run -p coldstore-metadata
run-gateway:
	@cargo run -p coldstore-gateway
run-scheduler:
	@cargo run -p coldstore-scheduler
run-cache:
	@cargo run -p coldstore-cache
run-tape:
	@cargo run -p coldstore-tape
run-node:
	@cargo run -p coldstore-node
run-node-no-tape:
	@cargo run -p coldstore-node -- --config configs/single-node-no-tape.yaml

lint: fmt-check clippy check unit
	@echo "All lint checks passed (unit-only)."

# `git rev-parse --git-path hooks` so worktrees, submodules and core.hooksPath all land in the
# right place; hardcoding .git/hooks does not.
HOOKS_DIR := $(shell git rev-parse --git-path hooks 2>/dev/null || echo .git/hooks)
# --git-common-dir, not --git-dir: inside a linked worktree the hooks still live in the main
# .git/hooks, and comparing against the worktree dir would misfire and blame core.hooksPath.
REPO_HOOKS_DIR := $(shell git rev-parse --git-common-dir 2>/dev/null || echo .git)/hooks

# core.hooksPath may point somewhere outside the repository (husky-style). Overwriting hooks
# there silently would replace someone else's hooks, so require an explicit FORCE=1.
install-hooks:
	@h="$(HOOKS_DIR)"; r="$(REPO_HOOKS_DIR)"; \
	ha=$$(cd "$$h" 2>/dev/null && pwd || printf '%s' "$$h"); \
	ra=$$(cd "$$r" 2>/dev/null && pwd || printf '%s' "$$r"); \
	if [ "$$ha" != "$$ra" ] && [ -z "$(FORCE)" ]; then \
		echo "git hooks resolve to $$ha, which is not the repository's own $$ra."; \
		echo "Refusing to overwrite hooks outside the repository; re-run with FORCE=1 to proceed."; \
		exit 1; \
	fi
	@mkdir -p "$(HOOKS_DIR)"
	@cp scripts/pre-commit "$(HOOKS_DIR)/pre-commit"
	@chmod +x "$(HOOKS_DIR)/pre-commit"
	@cp scripts/commit-msg "$(HOOKS_DIR)/commit-msg"
	@chmod +x "$(HOOKS_DIR)/commit-msg"
	@echo "Hooks installed in $(HOOKS_DIR): pre-commit (skips when no host cargo) and commit-msg (review gate, AGENTS.md §2.9)."

install-tools:
	@rustup component add rustfmt clippy
	@echo "protobuf-compiler required: apt install protobuf-compiler"

setup: install-tools install-hooks
	@echo "Development environment ready."

help:
	@echo "ColdStore Workspace Makefile"
	@echo ""
	@echo "Build:   build build-debug clean"
	@echo "Quality: fmt fmt-check clippy metadata-locked test unit vtl-unit vtl-check hook-test check check-all check-safe lint"
	@echo "Run:     run-node run-node-no-tape run-metadata run-gateway run-scheduler run-cache run-tape"
	@echo "Setup:   setup install-tools install-hooks (add FORCE=1 to install outside the repo's hooks dir)"
