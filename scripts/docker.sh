#!/usr/bin/env bash
# Run ColdStore build/lint/test commands inside Docker. Nothing is installed on the host.
#
#   scripts/docker.sh build            # cargo build --workspace
#   scripts/docker.sh test             # cargo test --workspace
#   scripts/docker.sh check-safe       # fmt-check, clippy, unit, vtl-unit, build-debug
#   scripts/docker.sh fmt              # cargo fmt --all
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

build_image() {
  docker build -f "$REPO/$DOCKERFILE" -t "$IMAGE" "$REPO"
}

run() {
  docker run --rm "${VOLUMES[@]}" -w /work "$IMAGE" "$@"
}

cmd=${1:-help}
shift || true

case "$cmd" in
  build)      build_image; run cargo build --workspace ;;
  test)       build_image; run cargo test --workspace ;;
  clippy)     build_image; run cargo clippy --workspace --all-targets -- -D warnings ;;
  fmt)        build_image; run cargo fmt --all ;;
  fmt-check)  build_image; run cargo fmt --all -- --check ;;
  vtl-unit)   build_image; run cargo test -p coldstore-vtl --lib --tests ;;
  check-safe)
    build_image
    run cargo fmt --all -- --check
    run cargo clippy --workspace --all-targets -- -D warnings
    run cargo test --workspace --lib --bins
    run cargo test -p coldstore-vtl --lib --tests
    run cargo build --workspace
    echo "Safe verification passed (fmt/clippy/unit/vtl-unit/build, all in Docker)."
    ;;
  shell)      build_image; run bash ;;
  run)        build_image; run "$@" ;;
  clean)      docker volume rm coldstore-target ;;
  help|*)
    sed -n '2,16p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    ;;
esac
