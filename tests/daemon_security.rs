//! Security tests for daemon watch path validation concepts.
//!
//! These tests validate the security principles enforced by the daemon's
//! watch path validation. The actual DaemonState tests are in src/daemon/routes.rs.

use std::path::Path;

/// Validates that a canonical path stays within an approved root.
/// This mirrors the logic in DaemonState::is_path_approved.
fn is_path_within_root(path: &Path, root: &Path) -> bool {
    let canonical = match path.canonicalize() {
        Ok(p) => p,
        Err(_) => return false,
    };
    let root_canonical = match root.canonicalize() {
        Ok(r) => r,
        Err(_) => return false,
    };
    canonical.starts_with(&root_canonical)
}

#[test]
fn path_validation_rejects_outside_root() {
    let tmpdir = tempfile::tempdir().unwrap();
    let approved = tmpdir.path().join("workspace");
    std::fs::create_dir_all(&approved).unwrap();

    let outside = tmpdir.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();

    assert!(
        is_path_within_root(&approved, &approved),
        "path inside approved root should be valid"
    );

    assert!(
        !is_path_within_root(&outside, &approved),
        "path outside approved root should be rejected"
    );
}

#[test]
fn path_validation_rejects_nonexistent_path() {
    let tmpdir = tempfile::tempdir().unwrap();
    let approved = tmpdir.path().join("workspace");
    std::fs::create_dir_all(&approved).unwrap();

    let nonexistent = tmpdir.path().join("does_not_exist");
    assert!(
        !is_path_within_root(&nonexistent, &approved),
        "nonexistent path should be rejected"
    );
}

#[cfg(unix)]
#[test]
fn path_validation_rejects_symlink_escaping() {
    let tmpdir = tempfile::tempdir().unwrap();
    let workspace = tmpdir.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();

    let outside = tmpdir.path().join("outside");
    std::fs::create_dir_all(&outside).unwrap();

    // Create symlink inside workspace pointing outside
    let symlink_path = workspace.join("escape");
    std::os::unix::fs::symlink(&outside, &symlink_path).unwrap();

    // The symlink resolves to outside the workspace, so it should be rejected
    assert!(
        !is_path_within_root(&symlink_path, &workspace),
        "symlink resolving outside workspace should be rejected"
    );
}

#[test]
fn path_validation_allows_nested_paths() {
    let tmpdir = tempfile::tempdir().unwrap();
    let workspace = tmpdir.path().join("workspace");
    let nested = workspace.join("src").join("lib");
    std::fs::create_dir_all(&nested).unwrap();

    assert!(
        is_path_within_root(&nested, &workspace),
        "nested path inside workspace should be allowed"
    );
}

#[cfg(unix)]
#[test]
fn path_validation_handles_symlink_within_workspace() {
    let tmpdir = tempfile::tempdir().unwrap();
    let workspace = tmpdir.path().join("workspace");
    let target = workspace.join("target");
    std::fs::create_dir_all(&target).unwrap();

    // Create symlink inside workspace pointing to another location inside workspace
    let symlink_path = workspace.join("link");
    std::os::unix::fs::symlink(&target, &symlink_path).unwrap();

    // Symlink resolves within workspace, should be allowed
    assert!(
        is_path_within_root(&symlink_path, &workspace),
        "symlink resolving inside workspace should be allowed"
    );
}
