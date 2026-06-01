use std::{
    path::{Path, PathBuf},
    process::Command,
};

fn marrow_bin() -> &'static str {
    env!("CARGO_BIN_EXE_marrow")
}

fn repo_file(path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn read_repo_file(path: &str) -> String {
    // Normalize EOLs: these assertions check logical file content (and several
    // match on `\n`-delimited substrings), so a CRLF checkout on Windows
    // (`core.autocrlf`) must not change the result.
    std::fs::read_to_string(repo_file(path))
        .unwrap_or_else(|err| panic!("read {path}: {err}"))
        .replace("\r\n", "\n")
}

fn run_marrow(args: &[&str], envs: &[(&str, &Path)]) -> std::process::Output {
    let mut command = Command::new(marrow_bin());
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    command
        .output()
        .unwrap_or_else(|err| panic!("run {:?}: {err}", args))
}

fn run_marrow_with_path(args: &[&str], home: &Path, path: &Path) -> std::process::Output {
    Command::new(marrow_bin())
        .args(args)
        .env("HOME", home)
        .env("PATH", path)
        .output()
        .unwrap_or_else(|err| panic!("run {:?}: {err}", args))
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[cfg(target_os = "macos")]
#[test]
fn sprint2_daemon_commands_are_opt_in_idempotent_and_runtime_scoped() {
    let temp_home = tempfile::tempdir().unwrap();
    let stage_root = tempfile::tempdir().unwrap();
    let plist_path = temp_home
        .path()
        .join("Library/LaunchAgents/dev.marrow.daemon.plist");

    let ui_enable = run_marrow(
        &["ui-app", "enable"],
        &[
            ("HOME", temp_home.path()),
            ("MARROW_UI_APP_STAGE_ROOT", stage_root.path()),
        ],
    );
    assert!(
        ui_enable.status.success(),
        "ui-app enable should succeed: {}",
        stderr(&ui_enable)
    );
    assert!(
        !plist_path.exists(),
        "ui-app enable must not create daemon autostart artifacts"
    );

    let initial_status = run_marrow(&["daemon", "status"], &[("HOME", temp_home.path())]);
    assert!(
        initial_status.status.success(),
        "daemon status should succeed before install: {}",
        stderr(&initial_status)
    );
    let initial_stdout = stdout(&initial_status);
    assert!(initial_stdout.contains("Autostart: not configured"));
    assert!(initial_stdout.contains("Runtime:"));
    assert!(initial_stdout.contains(plist_path.to_string_lossy().as_ref()));

    let first_install = run_marrow(&["daemon", "install"], &[("HOME", temp_home.path())]);
    assert!(
        first_install.status.success(),
        "daemon install should succeed: {}",
        stderr(&first_install)
    );
    let first_stdout = stdout(&first_install);
    assert!(first_stdout.contains("autostart configured:"));
    assert!(first_stdout.contains(plist_path.to_string_lossy().as_ref()));
    assert!(
        plist_path.exists(),
        "daemon install must create the launch agent"
    );

    let plist = std::fs::read_to_string(&plist_path).unwrap();
    assert!(plist.contains(marrow_bin()));
    assert!(plist.contains("<string>daemon</string>"));

    let second_install = run_marrow(&["daemon", "install"], &[("HOME", temp_home.path())]);
    assert!(
        second_install.status.success(),
        "second daemon install should succeed: {}",
        stderr(&second_install)
    );
    assert!(
        second_install.status.success() && plist_path.exists(),
        "idempotent install must keep exactly one canonical launch agent"
    );

    let daemon_status = run_marrow(&["daemon", "status"], &[("HOME", temp_home.path())]);
    assert!(
        daemon_status.status.success(),
        "daemon status should succeed after install: {}",
        stderr(&daemon_status)
    );
    let daemon_stdout = stdout(&daemon_status);
    assert!(daemon_stdout.contains("Autostart: configured"));
    assert!(daemon_stdout.contains("Runtime:"));
    assert!(daemon_stdout.contains("Artifact:"));
    assert!(daemon_stdout.contains(plist_path.to_string_lossy().as_ref()));

    let top_level_status = run_marrow(&["status"], &[("HOME", temp_home.path())]);
    assert!(
        top_level_status.status.success(),
        "top-level status should succeed: {}",
        stderr(&top_level_status)
    );
    let top_level_stdout = stdout(&top_level_status);
    assert!(
        top_level_stdout.contains("daemon is") || top_level_stdout.contains("status check error"),
        "top-level status should remain runtime-only: {top_level_stdout}"
    );
    assert!(!top_level_stdout.contains("Autostart:"));
    assert!(!top_level_stdout.contains("Artifact:"));

    let first_uninstall = run_marrow(&["daemon", "uninstall"], &[("HOME", temp_home.path())]);
    assert!(
        first_uninstall.status.success(),
        "daemon uninstall should succeed: {}",
        stderr(&first_uninstall)
    );
    assert!(
        !plist_path.exists(),
        "daemon uninstall must remove the configured launch agent"
    );

    let second_uninstall = run_marrow(&["daemon", "uninstall"], &[("HOME", temp_home.path())]);
    assert!(
        second_uninstall.status.success(),
        "daemon uninstall must stay idempotent: {}",
        stderr(&second_uninstall)
    );
}

#[cfg(target_os = "macos")]
#[test]
fn sprint2_daemon_install_is_nonfatal_when_launchctl_is_missing() {
    let temp_home = tempfile::tempdir().unwrap();
    let empty_path = tempfile::tempdir().unwrap();
    let plist_path = temp_home
        .path()
        .join("Library/LaunchAgents/dev.marrow.daemon.plist");

    let install = run_marrow_with_path(&["daemon", "install"], temp_home.path(), empty_path.path());
    assert!(
        install.status.success(),
        "daemon install should succeed without launchctl: {}",
        stderr(&install)
    );
    assert!(stdout(&install).contains(plist_path.to_string_lossy().as_ref()));
    assert!(stderr(&install).contains("launchctl not found"));
    assert!(plist_path.exists());

    let uninstall = run_marrow_with_path(
        &["daemon", "uninstall"],
        temp_home.path(),
        empty_path.path(),
    );
    assert!(
        uninstall.status.success(),
        "daemon uninstall should succeed without launchctl: {}",
        stderr(&uninstall)
    );
    assert!(stderr(&uninstall).contains("launchctl not found"));
}

#[cfg(target_os = "macos")]
#[test]
fn sprint2_service_install_alias_matches_daemon_install() {
    let temp_home = tempfile::tempdir().unwrap();
    let plist_path = temp_home
        .path()
        .join("Library/LaunchAgents/dev.marrow.daemon.plist");

    let alias_install = run_marrow(&["service", "install"], &[("HOME", temp_home.path())]);
    assert!(
        alias_install.status.success(),
        "service install should succeed: {}",
        stderr(&alias_install)
    );
    let alias_stdout = stdout(&alias_install);
    assert!(alias_stdout.contains(plist_path.to_string_lossy().as_ref()));
    assert!(plist_path.exists());

    let uninstall = run_marrow(&["daemon", "uninstall"], &[("HOME", temp_home.path())]);
    assert!(
        uninstall.status.success(),
        "daemon uninstall after alias should succeed: {}",
        stderr(&uninstall)
    );

    let daemon_install = run_marrow(&["daemon", "install"], &[("HOME", temp_home.path())]);
    assert!(
        daemon_install.status.success(),
        "daemon install should succeed: {}",
        stderr(&daemon_install)
    );
    let daemon_stdout = stdout(&daemon_install);
    assert_eq!(
        alias_stdout.trim(),
        daemon_stdout.trim(),
        "service install alias must produce the same reported path as daemon install"
    );
}

#[test]
fn sprint2_packaging_metadata_and_release_workflow_cover_native_artifacts() {
    let cargo_toml = read_repo_file("Cargo.toml");
    let release = read_repo_file(".github/workflows/release.yml");
    let ci = read_repo_file(".github/workflows/ci.yml");

    assert!(cargo_toml.contains("license = \"MIT\""));
    assert!(cargo_toml.contains("maintainer-scripts = \"scripts\""));
    assert!(cargo_toml.contains("/usr/bin/marrow"));
    assert!(cargo_toml.contains("/usr/share/applications/marrow.desktop"));
    assert!(cargo_toml.contains("/usr/share/icons/hicolor/256x256/apps/marrow.png"));
    assert!(cargo_toml.contains("[package.metadata.wix]"));
    assert!(cargo_toml.contains("product-name = \"Marrow\""));
    assert!(cargo_toml.contains("manufacturer = \"NDMyers\""));

    assert!(release.contains("package-macos:"));
    assert!(release.contains("aarch64-apple-darwin"));
    assert!(release.contains("x86_64-apple-darwin"));
    assert!(release.contains("dist/*.dmg"));
    assert!(release.contains("dist/*.deb dist/*.AppImage"));
    assert!(release.contains("dist/*.msi"));
    assert!(release.contains("cargo deb --target x86_64-unknown-linux-gnu --no-build"));
    assert!(release.contains("cargo wix --no-build --target x86_64-pc-windows-msvc"));
    assert!(release.contains("Marrow-${{ github.ref_name }}-x86_64-pc-windows-msvc.msi"));
    assert!(release.contains("--pattern \"*.dmg\""));
    assert!(release.contains("--pattern \"*.deb\""));
    assert!(release.contains("--pattern \"*.AppImage\""));
    assert!(release.contains("--pattern \"*.msi\""));
    assert!(release.contains("shasum -a 256 * > checksums.sha256"));
    assert!(release.contains("permissions:\n  contents: read"));
    assert!(release.contains("contents: write"));
    assert!(release.contains("publish-npm:"));
    assert!(release.contains("id-token: write"));
    assert!(release.contains("npm audit --omit=dev --audit-level=moderate"));
    assert!(release.contains("npm pack --dry-run --json"));
    assert!(release.contains("npm publish --access public --tag alpha --provenance"));
    assert!(release.contains("marrow-x86_64-unknown-linux-gnu.tar.gz"));
    assert!(release.contains("marrow-x86_64-apple-darwin.tar.gz"));
    assert!(release.contains("marrow-aarch64-apple-darwin.tar.gz"));
    assert!(release.contains("marrow-x86_64-pc-windows-msvc.tar.gz"));
    assert!(release.contains("appimagetool/releases/download/1.9.1/appimagetool-x86_64.AppImage"));
    assert!(release.contains("ed4ce84f0d9caff66f50bcca6ff6f35aae54ce8135408b3fa33abfc3cb384eb0"));

    assert!(ci.contains("permissions:\n  contents: read"));
    assert!(ci.contains("Validate packaging metadata and scripts"));
    assert!(ci.contains("bash -n scripts/postinst scripts/deb-postinst.sh scripts/stage-linux-package-assets.sh scripts/package-linux-appimage.sh scripts/package-macos-dmg.sh"));
}

#[test]
fn sprint2_npm_package_metadata_is_release_ready_and_lean() {
    let package_json = read_repo_file("npm/package.json");
    let package_lock = read_repo_file("npm/package-lock.json");

    for text in [&package_json, &package_lock] {
        assert!(text.contains("\"repository\""));
        assert!(text.contains("\"homepage\""));
        assert!(text.contains("\"bugs\""));
        assert!(text.contains("\"author\""));
        assert!(text.contains("\"publishConfig\""));
        assert!(text.contains("\"access\": \"public\""));
    }

    assert!(package_json.contains("\"license\": \"MIT\""));
    assert!(package_json.contains("\"README.md\""));
    assert!(package_json.contains("\"LICENSE\""));
    assert!(!package_json.contains("\"docs"));
    assert!(!package_json.contains("\"target"));
    assert!(!package_json.contains("\"node_modules"));
    assert!(!package_json.contains("\"dist"));
    assert!(!package_json.contains("10x-squad-artifacts"));
}

#[test]
fn sprint2_scripts_use_staging_helpers_and_avoid_daemon_side_effects() {
    let macos_dmg = read_repo_file("scripts/package-macos-dmg.sh");
    let appimage = read_repo_file("scripts/package-linux-appimage.sh");
    let stage_linux = read_repo_file("scripts/stage-linux-package-assets.sh");
    let postinst = read_repo_file("scripts/postinst");
    let deb_postinst = read_repo_file("scripts/deb-postinst.sh");
    let install_js = read_repo_file("npm/scripts/install.js");

    assert!(macos_dmg.contains("MARROW_UI_APP_STAGE_ROOT"));
    assert!(macos_dmg.contains("\"$BIN_PATH\" ui-app enable"));
    assert!(macos_dmg.contains("ln -s /Applications \"$DMG_ROOT/Applications\""));
    assert!(macos_dmg.contains("DMG_NAME=\"Marrow-${VERSION}-${ARCH_LABEL}.dmg\""));
    assert!(macos_dmg.contains("hdiutil is required to build a DMG"));

    assert!(stage_linux.contains(
        "MARROW_UI_APP_STAGE_ROOT=\"$STAGE_ROOT\" MARROW_DESKTOP_EXEC=\"$DESKTOP_EXEC\""
    ));
    assert!(stage_linux.contains("\"$BIN_PATH\" ui-app enable >/dev/null"));
    assert!(stage_linux.contains(
        "cp \"$STAGE_ROOT/.local/share/applications/marrow.desktop\" \"$OUT_DIR/marrow.desktop\""
    ));
    assert!(stage_linux.contains(
        "cp \"$STAGE_ROOT/.local/share/icons/hicolor/256x256/apps/marrow.png\" \"$OUT_DIR/marrow.png\""
    ));

    assert!(appimage.contains("x86_64-unknown-linux-gnu only"));
    assert!(appimage.contains("appimagetool is required to build an AppImage"));
    assert!(appimage.contains("scripts/stage-linux-package-assets.sh"));
    assert!(appimage.contains("OUTPUT_PATH=\"$OUT_DIR/Marrow-${VERSION}-${ARCH_LABEL}.AppImage\""));

    assert!(postinst.contains("update-desktop-database /usr/share/applications || true"));
    assert!(deb_postinst.contains("update-desktop-database /usr/share/applications || true"));

    assert!(!install_js.contains("daemon install"));
    assert!(!install_js.contains("service install"));
    assert!(!install_js.contains("spawnSync(binaryPath, ['ui-app', 'enable']"));
    assert!(install_js.contains("Run `marrow ui-app enable`"));
}

#[test]
fn sprint2_docs_describe_explicit_desktop_registration() {
    let readme = read_repo_file("README.md");
    let security = read_repo_file("SECURITY.md");
    let release = read_repo_file("RELEASE.md");

    assert!(readme.contains("npm install"));
    assert!(readme.contains("marrow ui-app enable"));
    assert!(security.contains("release workflow"));
    assert!(security.contains("checksums.sha256"));
    assert!(security.contains("AppImage"));
    assert!(release.contains("Public Release Checklist"));
}
