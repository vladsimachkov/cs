//! Digest comparison shared across crate boundaries.

/// Compare a declared SHA-256 against a computed hex digest.
///
/// Surrounding whitespace and an optional `sha256:` / `SHA256:` prefix are tolerated,
/// and hex case does not matter.
///
/// This is deliberately the only implementation in the tree. The same `checksum` string
/// reaches the cache's staging-trailer check and the scheduler's archive-time check, and
/// AGENTS.md §6.5 wants one convention across a crate boundary instead of two rules that
/// can drift apart — without one shared rule, a `sha256:`-prefixed value would pass the
/// archive gate and fail the staging gate.
///
/// Only the two literal prefixes are stripped, so `Sha256:` is not recognized. That is
/// the archive side's long-standing behavior and this copy is a move, not a behavior
/// change (AGENTS.md §11); widening the strip would silently accept a form the archive
/// side never accepted, and belongs in a change of its own that changes both sides.
///
/// How an empty declaration is read is the caller's choice, not this function's: the
/// cache rejects an empty trailer, because nothing matches it. The scheduler instead
/// reads an empty `checksum` as "nothing declared" and skips its staging gate — but in
/// the live PutObject path the digest is always non-empty (`sha256_hex` of the body),
/// so the archive-time metadata gate still runs.
pub fn sha256_matches(expected: &str, computed_hex: &str) -> bool {
    let expected = expected.trim();
    let expected = expected
        .strip_prefix("sha256:")
        .or_else(|| expected.strip_prefix("SHA256:"))
        .unwrap_or(expected);
    expected.eq_ignore_ascii_case(computed_hex)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIGEST: &str = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

    #[test]
    fn accepts_bare_hex_in_either_case() {
        assert!(sha256_matches(DIGEST, DIGEST));
        assert!(sha256_matches(&DIGEST.to_uppercase(), DIGEST));
    }

    #[test]
    fn accepts_the_prefixed_and_padded_forms_the_archive_side_always_tolerated() {
        assert!(sha256_matches(&format!("sha256:{DIGEST}"), DIGEST));
        assert!(sha256_matches(&format!("SHA256:{DIGEST}"), DIGEST));
        assert!(sha256_matches(&format!("  sha256:{DIGEST} "), DIGEST));
    }

    #[test]
    fn rejects_a_different_digest_and_an_empty_declaration() {
        assert!(!sha256_matches("dead", DIGEST));
        assert!(!sha256_matches("", DIGEST));
        assert!(!sha256_matches("sha256:", DIGEST));
        // The documented boundary: only the two literal prefixes are recognized, so a
        // differently-cased prefix is treated as part of the digest and must not match.
        assert!(!sha256_matches(&format!("Sha256:{DIGEST}"), DIGEST));
    }
}
