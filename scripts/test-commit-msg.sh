#!/usr/bin/env bash
# Smoke suite for scripts/commit-msg, the AGENTS.md §2.9 review gate.
#
# Runs entirely in a throwaway repository: no network, no root, no cargo, no devices
# (AGENTS.md §2.8). Run it with `make hook-test` or directly:
#
#     scripts/test-commit-msg.sh
#
# Exit status is 0 only if every case behaves as documented in AGENTS.md §2.9 / §7.5.
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HOOK="$HERE/commit-msg"
[ -f "$HOOK" ] || { echo "test-commit-msg: cannot find $HOOK" >&2; exit 2; }

TMP=$(mktemp -d "${TMPDIR:-/tmp}/commit-msg-suite.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
# The commit-message file must live OUTSIDE the test repository: `git add -A` would otherwise
# stage it, and an unlisted non-*.md path counts as a code change.
MSGDIR=$(mktemp -d "${TMPDIR:-/tmp}/commit-msg-msgs.XXXXXX")
trap 'rm -rf "$TMP" "$MSGDIR"' EXIT

pass=0
fail=0
n=0

t() { # t <name> <ACCEPT|REJECT> <message> <file to touch>
    local name=$1 want=$2 msg=$3 file=$4 got staged
    n=$((n + 1))
    printf '%s\n' "$msg" > "$MSGDIR/msg"
    printf '\n%s\n' "$n" >> "$TMP/$file"
    ( cd "$TMP" && git add -A >/dev/null )
    # If staging produced nothing, the hook sees an empty list and exits 0, so an ACCEPT case
    # would pass without the hook ever looking at the path.
    staged=$( cd "$TMP" && git -c core.quotePath=false diff --cached --name-only )
    if [ -z "$staged" ]; then
        fail=$((fail + 1))
        printf '  FAIL [setup ] %s: nothing staged, the hook was never consulted\n' "$name"
        return
    fi
    if ( cd "$TMP" && git commit -q -F "$MSGDIR/msg" >/dev/null 2>&1 ); then
        got=ACCEPT
    else
        got=REJECT
    fi
    ( cd "$TMP" && git reset -q >/dev/null 2>&1; git checkout -q -- . >/dev/null 2>&1; git clean -qfd >/dev/null 2>&1 )
    if [ "$got" = "$want" ]; then
        pass=$((pass + 1)); printf '  ok   [%-6s] %s\n' "$got" "$name"
    else
        fail=$((fail + 1)); printf '  FAIL [got %s want %s] %s\n' "$got" "$want" "$name"
    fi
}

echo "git: $(git --version)"
echo "bash: ${BASH_VERSION:-unknown}"

# Hermetic: ignore ambient git config (a global core.hooksPath would make every REJECT case
# fail and every ACCEPT case pass vacuously) and any inherited git plumbing environment.
GIT_CONFIG_COUNT=0
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_CONFIG_COUNT
# GIT_CONFIG_KEY_* / GIT_CONFIG_VALUE_* act like `-c` overrides injected by the parent.
# compgen, not ${!PREFIX*}: the latter trips `set -u` on bash < 4.4 when nothing matches.
for v in $(compgen -v GIT_CONFIG_KEY_; compgen -v GIT_CONFIG_VALUE_); do unset "$v"; done

cd "$TMP" || exit 2
git init -q .
git config user.name "hook-test"
git config user.email "hook-test@example.invalid"
git config commit.gpgsign false
mkdir -p crates/proto/proto .github/workflows docs scripts
echo x > crates/a.rs
echo y > README.md
echo z > AGENTS.md
echo p > crates/proto/proto/common.proto
echo g > .github/workflows/rust.yml
echo b > docs/BUILD.ru.md
echo c > scripts/helper.sh
git add -A
git commit -q -m init --no-verify
cp "$HOOK" .git/hooks/commit-msg
chmod +x .git/hooks/commit-msg
SHA=$(git rev-parse HEAD)

echo
echo "review trailers"
t "code path without trailers" REJECT "feat: a" crates/a.rs
t "code path with valid trailers" ACCEPT "feat: a

Reviewed-by: reviewer-session-1
Review-scope: $SHA-abcdef123456" crates/a.rs
t "Review-scope without a digest part" REJECT "feat: a

Reviewed-by: reviewer-session-1
Review-scope: $SHA" crates/a.rs
t "Review-scope with a short digest" REJECT "feat: a

Reviewed-by: reviewer-session-1
Review-scope: $SHA-abc" crates/a.rs
t "Review-scope non-hex (HEAD-...)" REJECT "feat: a

Reviewed-by: reviewer-session-1
Review-scope: HEAD-abcdef123456" crates/a.rs
t "Review-scope hex but not a commit" REJECT "feat: a

Reviewed-by: reviewer-session-1
Review-scope: deadbeefabcdef-abcdef123456" crates/a.rs
t "Review-scope short hex prefix accepted" ACCEPT "feat: a

Reviewed-by: reviewer-session-1
Review-scope: ${SHA:0:8}-abcdef123456" crates/a.rs
t "Review-scope without space after colon" ACCEPT "feat: a

Reviewed-by: reviewer-session-1
Review-scope:${SHA}-abcdef123456" crates/a.rs
t "trailers quoted inside the body" REJECT "feat: a

The reviewer wrote: Reviewed-by: reviewer-session-1 / Review-scope: $SHA-abcdef123456" crates/a.rs
t "trailers followed by a prose paragraph" REJECT "feat: a

Reviewed-by: reviewer-session-1
Review-scope: $SHA-abcdef123456

See the linked report for details." crates/a.rs
t "empty Reviewed-by value" REJECT "feat: a

Reviewed-by:
Review-scope: $SHA-abcdef123456" crates/a.rs

echo
echo "path classification"
t "plain documentation is exempt" ACCEPT "doc: readme" README.md
t "docs/BUILD.ru.md counts as code" REJECT "doc: build" docs/BUILD.ru.md
t "path with spaces is still code" REJECT "feat: sp" "crates/with space.rs"
t "non-ASCII documentation path is exempt" ACCEPT "doc: readme" "docs/README_日本語.md"
t "unlisted non-md path counts as code" REJECT "chore: notes" NOTES.txt
t "doc path containing a quote is exempt" ACCEPT "doc: q" 'docs/notes".md'

echo
echo "waiver floor (AGENTS.md §2.9 'Waiver')"
t "waiver allowed on ordinary code" ACCEPT "feat: x

Waived-by: operator - explicit instruction" crates/a.rs
t "waiver rejected on AGENTS.md" REJECT "doc: x

Waived-by: operator - reason" AGENTS.md
t "waiver rejected on .github" REJECT "ci: x

Waived-by: operator - reason" .github/workflows/rust.yml
t "waiver rejected on proto definitions" REJECT "feat: x

Waived-by: operator - reason" crates/proto/proto/common.proto
t "waiver rejected on docs/BUILD.ru.md" REJECT "doc: x

Waived-by: operator - reason" docs/BUILD.ru.md
t "waiver rejected on the gate itself" REJECT "chore: neuter the gate

Waived-by: operator - reason" scripts/commit-msg
t "waiver rejected on the gate's own suite" REJECT "chore: neuter the gate tests

Waived-by: operator - reason" scripts/test-commit-msg.sh
t "empty Waived-by value is not a waiver" REJECT "feat: x

Waived-by:" crates/a.rs
t "valid review wins over a stray waiver" ACCEPT "feat: x

Reviewed-by: reviewer-session-1
Review-scope: $SHA-abcdef123456

Waived-by: operator - redundant" crates/a.rs

echo
echo "git plumbing"
echo
echo "rename of a protected file (git mv AGENTS.md docs/AGENTS.md)"
if ! git mv AGENTS.md docs/AGENTS.md 2>/dev/null || ! git add -A >/dev/null 2>&1; then
    fail=$((fail + 1)); echo "  FAIL setup: git mv/add failed, the hook was never consulted"
elif git commit -q -m "chore: move AGENTS.md" >/dev/null 2>&1; then
    fail=$((fail + 1)); echo "  FAIL [got ACCEPT want REJECT] rename is caught"
else
    pass=$((pass + 1)); echo "  ok   [REJECT] rename is caught"
fi
git reset -q >/dev/null 2>&1; git checkout -q -- . >/dev/null 2>&1; git clean -qfd >/dev/null 2>&1

echo
echo "type change only (crates/a.rs replaced by a symlink)"
rm -f "$TMP/crates/a.rs"; ln -s /etc/hostname "$TMP/crates/a.rs"
( cd "$TMP" && git add -A >/dev/null )
printf 'feat: no trailers\n' > "$MSGDIR/msg"
if ( cd "$TMP" && git commit -q -F "$MSGDIR/msg" >/dev/null 2>&1 ); then
    fail=$((fail + 1)); echo "  FAIL [got ACCEPT want REJECT] a type-only change is caught"
else
    pass=$((pass + 1)); echo "  ok   [REJECT] a type-only change is caught"
fi
( cd "$TMP" && git reset -q --hard HEAD >/dev/null 2>&1 )

git checkout -q -b feature 2>/dev/null
printf 'unique feature content\n' > crates/feature.rs
git add -A && git commit -q -m "feat: on a branch" --no-verify
git checkout -q - 2>/dev/null
printf 'main-only\n' > README.md && git add -A && git commit -q -m "docs: main moved" --no-verify

echo
echo "cherry-pick does not invoke commit-msg at all (documented bypass, AGENTS.md §7.5)"
if git cherry-pick feature >"$MSGDIR/cp.log" 2>&1; then
    echo "  ok   [ACCEPT] cherry-pick bypasses the hook, as documented"; pass=$((pass + 1))
else
    echo "  FAIL cherry-pick did not succeed; diagnose before blaming the hook:"; sed -n '1,3p' "$MSGDIR/cp.log"
    fail=$((fail + 1))
fi
git cherry-pick --abort >/dev/null 2>&1 || true

echo
echo "revert does not invoke commit-msg either (documented bypass, AGENTS.md §7.5)"
if git revert --no-edit HEAD >"$MSGDIR/rv.log" 2>&1; then
    echo "  ok   [ACCEPT] revert bypasses the hook, as documented"; pass=$((pass + 1))
else
    echo "  FAIL revert did not succeed; diagnose before blaming the hook:"; sed -n '1,3p' "$MSGDIR/rv.log"
    fail=$((fail + 1))
fi
git revert --abort >/dev/null 2>&1 || true

echo
echo "git am does not invoke commit-msg either (documented bypass)"
( cd "$TMP" && printf 'am me\n' >> crates/a.rs \
  && git add -A >/dev/null \
  && git commit -q --no-verify -m "feat: patch source" >/dev/null 2>&1 \
  && git format-patch -1 HEAD -o "$MSGDIR" >/dev/null 2>&1 \
  && git reset -q --hard HEAD~1 >/dev/null 2>&1 )
if ( cd "$TMP" && git am "$MSGDIR"/0001-*.patch > "$MSGDIR/am.log" 2>&1 ); then
    echo "  ok   [ACCEPT] git am bypasses the hook, as documented"; pass=$((pass + 1))
else
    echo "  FAIL git am did not succeed; diagnose before blaming the hook:"
    sed -n '1,3p' "$MSGDIR/am.log"; fail=$((fail + 1))
fi
( cd "$TMP" && git am --abort >/dev/null 2>&1; git reset -q --hard HEAD >/dev/null 2>&1 )

echo
echo "merge commit is exempt (content reviewed on the parents)"
if git merge -q --no-ff -m "Merge branch 'feature'" feature >/dev/null 2>&1; then
    echo "  ok   [ACCEPT] merge without trailers"; pass=$((pass + 1))
else
    echo "  FAIL merge was blocked"; fail=$((fail + 1))
fi

echo
echo "fail-closed when git cannot list the staged paths"
FAKE="$MSGDIR/fakebin"; mkdir -p "$FAKE"
printf '#!/bin/sh\nexit 1\n' > "$FAKE/git"; chmod +x "$FAKE/git"
printf 'feat: no trailers\n' > "$MSGDIR/msg"
if PATH="$FAKE:$PATH" "$TMP/.git/hooks/commit-msg" "$MSGDIR/msg" > "$MSGDIR/fc.log" 2>&1; then
    echo "  FAIL the hook failed OPEN when git could not list staged paths"; fail=$((fail + 1))
elif grep -q "cannot list the staged paths" "$MSGDIR/fc.log"; then
    echo "  ok   [REJECT] hook fails closed when git fails"; pass=$((pass + 1))
else
    echo "  FAIL hook rejected for another reason: $(sed -n '1p' "$MSGDIR/fc.log")"; fail=$((fail + 1))
fi

echo
echo "hook syntax"
if bash -n "$HOOK" >/dev/null 2>&1; then
    echo "  ok   bash -n"; pass=$((pass + 1))
else
    echo "  FAIL bash -n"; fail=$((fail + 1))
fi

echo
echo "PASS=$pass FAIL=$fail"
[ "$fail" -eq 0 ]
