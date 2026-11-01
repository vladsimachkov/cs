#!/usr/bin/env bash
# Run ColdStore build/lint/test commands inside Docker. Nothing is installed on the host.
#
#   scripts/docker.sh build            # cargo build --workspace
#   scripts/docker.sh test             # cargo test --workspace
#   scripts/docker.sh check-safe       # fmt-check, clippy, lock-check, unit, vtl-unit, build, hook-test
#   scripts/docker.sh clippy           # cargo clippy --workspace --all-targets --all-features -- -D warnings
#   scripts/docker.sh fmt              # cargo fmt --all
#   scripts/docker.sh fmt-check        # cargo fmt --all -- --check
#   scripts/docker.sh vtl-unit         # cargo test -p coldstore-vtl --lib --tests
#   scripts/docker.sh lock-check       # cargo metadata --locked (Cargo.lock vs manifests)
#   scripts/docker.sh hook-test        # behavior of the scripts/commit-msg review gate
#   scripts/docker.sh shell            # interactive shell in the dev image
#   scripts/docker.sh run <args...>    # any command, e.g. run cargo tree -p coldstore-tape
#
# Caches live in named docker volumes (coldstore-cargo-registry, coldstore-cargo-git,
# coldstore-target) so repeated runs are incremental.
#
#   scripts/docker.sh clean            # drop the target cache
set -euo pipefail

IMAGE=${IMAGE:-coldstore-dev}
DOCKERFILE=${DOCKERFILE:-Dockerfile.dev}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VOLUMES=(
  -v "$REPO:/work"
  -v coldstore-cargo-registry:/usr/local/cargo/registry
  -v coldstore-cargo-git:/usr/local/cargo/git
  -v coldstore-target:/work/target
)

# rust-toolchain.toml is the source of truth for the toolchain: the image tag follows the `channel`
# there, and Dockerfile.dev fails the build if the image is not exactly that version.
toolchain_pin() {
  sed -n 's/^channel = "\(.*\)"/\1/p' "$REPO/rust-toolchain.toml"
}

build_image() {
  local pin
  pin=$(toolchain_pin)
  if [ -z "$pin" ]; then
    echo "scripts/docker.sh: cannot read channel from rust-toolchain.toml" >&2
    exit 1
  fi
  docker build --build-arg RUST_VERSION="$pin" -f "$REPO/$DOCKERFILE" -t "$IMAGE" "$REPO"
}

run() {
  docker run --rm "${VOLUMES[@]}" -w /work "$IMAGE" "$@"
}

cmd=${1:-help}
shift || true

case "$cmd" in
  build)      build_image; run cargo build --workspace ;;
  test)       build_image; run cargo test --workspace ;;
  clippy)     build_image; run cargo clippy --workspace --all-targets --all-features -- -D warnings ;;
  fmt)        build_image; run cargo fmt --all ;;
  fmt-check)  build_image; run cargo fmt --all -- --check ;;
  vtl-unit)   build_image; run cargo test -p coldstore-vtl --lib --tests ;;
  lock-check) build_image; run cargo metadata --locked --format-version 1 > /dev/null
              echo "Cargo.lock matches the manifests." ;;
  hook-test)  build_image; run scripts/test-commit-msg.sh ;;
  check-safe)
    build_image
    run cargo fmt --all -- --check
    run cargo clippy --workspace --all-targets --all-features -- -D warnings
    run cargo metadata --locked --format-version 1 > /dev/null
    run cargo test --workspace --lib --bins
    run cargo test -p coldstore-vtl --lib --tests
    run cargo build --workspace
    run scripts/test-commit-msg.sh
    echo "Safe verification passed (fmt/clippy/lock/unit/vtl-unit/build/hook-test, all in Docker)."
    ;;
  shell)      build_image; run bash ;;
  run)        build_image; run "$@" ;;
  clean)      docker volume rm coldstore-target ;;
  help|*)
    # Print the leading comment block (the usage), stopping at the first non-comment line, so the
    # list cannot go stale when header lines are added or removed (a fixed line range did).
    awk 'NR>1 { if ($0 ~ /^#/) { s = $0; sub(/^# ?/, "", s); print s } else { exit } }' "${BASH_SOURCE[0]}"
    ;;
esac
