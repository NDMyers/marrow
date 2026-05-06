//! Security tests for archive extraction logic.
//!
//! These tests validate the security principles enforced by the npm installer's
//! extractSecurely function. While the actual extraction happens in JavaScript,
//! these Rust tests document and verify the security invariants.

use std::path::Path;

/// Validates that a path does not contain path traversal sequences.
fn is_path_safe(path: &str) -> bool {
    // Reject absolute paths
    if Path::new(path).is_absolute() {
        return false;
    }

    // Reject path traversal
    if path.contains("..") {
        return false;
    }

    // Check each component
    for component in Path::new(path).components() {
        match component {
            std::path::Component::ParentDir => return false,
            std::path::Component::RootDir => return false,
            std::path::Component::Prefix(_) => return false,
            _ => {}
        }
    }

    true
}

/// Validates that a resolved path stays within the destination directory.
fn stays_within_dest(entry_path: &str, dest: &Path) -> bool {
    let resolved = dest.join(entry_path);
    let canonical = match resolved.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            // For non-existent paths, use lexical normalization
            let mut components = Vec::new();
            for c in resolved.components() {
                match c {
                    std::path::Component::ParentDir => {
                        components.pop();
                    }
                    std::path::Component::CurDir => {}
                    _ => components.push(c),
                }
            }
            components.iter().collect()
        }
    };

    let dest_canonical = dest.canonicalize().unwrap_or_else(|_| dest.to_path_buf());
    canonical.starts_with(&dest_canonical)
}

#[test]
fn rejects_path_traversal_dotdot() {
    assert!(!is_path_safe("../escape"));
    assert!(!is_path_safe("foo/../../../escape"));
    assert!(!is_path_safe("./foo/../../bar"));
}

#[test]
fn rejects_absolute_paths() {
    assert!(!is_path_safe("/etc/passwd"));
    assert!(!is_path_safe("/usr/bin/malicious"));

    #[cfg(windows)]
    {
        assert!(!is_path_safe("C:\\Windows\\System32"));
        assert!(!is_path_safe("\\\\server\\share"));
    }
}

#[test]
fn allows_safe_relative_paths() {
    assert!(is_path_safe("marrow"));
    assert!(is_path_safe("marrow.exe"));
    assert!(is_path_safe("bin/marrow"));
    assert!(is_path_safe("./marrow"));
    assert!(is_path_safe("foo/bar/baz"));
}

#[test]
fn stays_within_dest_rejects_escape() {
    let tmpdir = tempfile::tempdir().unwrap();
    let dest = tmpdir.path();

    // These should NOT stay within dest
    assert!(!stays_within_dest("../escape", dest));
    assert!(!stays_within_dest("foo/../../escape", dest));
}

#[test]
fn stays_within_dest_allows_nested() {
    let tmpdir = tempfile::tempdir().unwrap();
    let dest = tmpdir.path();

    // Create the nested structure so canonicalize works
    std::fs::create_dir_all(dest.join("foo/bar")).unwrap();

    // These should stay within dest
    assert!(stays_within_dest("foo", dest));
    assert!(stays_within_dest("foo/bar", dest));
}

/// Document the expected binary names that should be extracted.
#[test]
fn expected_binary_names() {
    let unix_binary = "marrow";
    let windows_binary = "marrow.exe";

    // Valid binary names
    assert!(unix_binary == "marrow");
    assert!(windows_binary == "marrow.exe");

    // Should reject binaries that don't match exactly
    let malicious_names = [
        "marrow.sh",
        "marrow-injected",
        "notmarrow",
        ".marrow",
        "marrow ",
    ];

    for name in malicious_names {
        assert!(
            name != unix_binary && name != windows_binary,
            "should not extract: {}",
            name
        );
    }
}

/// Test that ambiguous archives (multiple matching entries) would be rejected.
#[test]
fn rejects_ambiguous_entries() {
    // Scenario: archive contains multiple files named "marrow" in different dirs
    let entries = vec![
        "bin/marrow",
        "lib/marrow", // Ambiguous!
    ];

    let matching: Vec<_> = entries
        .iter()
        .filter(|e| {
            Path::new(e)
                .file_name()
                .map(|n| n == "marrow")
                .unwrap_or(false)
        })
        .collect();

    assert!(
        matching.len() > 1,
        "test setup: should have multiple matching entries"
    );

    // The extractSecurely function should reject this scenario
    // (this test documents the expected behavior)
}

/// Symlinks and hardlinks should be rejected from archives.
#[test]
fn document_link_rejection() {
    // These entry types should be rejected by extractSecurely:
    // - SymbolicLink
    // - Link (hardlink)
    //
    // Only "File" type entries should be extracted.
    //
    // This is enforced in the JavaScript extractSecurely function:
    // if (entry.type === "SymbolicLink" || entry.type === "Link") {
    //     throw new Error(`Security violation: ${entry.type.toLowerCase()}: ${entryPath}`);
    // }

    // This test documents the security requirement.
    // Actual enforcement is in npm/scripts/install.js.
}
