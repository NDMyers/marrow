//! OS-level service installation for the Marrow daemon.
//!
//! macOS:  generates ~/Library/LaunchAgents/dev.marrow.daemon.plist
//! Linux:  generates ~/.config/systemd/user/marrow.service

use anyhow::{Context as _, Result};
use std::path::Path;

pub fn install() -> Result<()> {
    let exe = std::env::current_exe().context("resolving binary path")?;

    #[cfg(target_os = "macos")]
    install_macos(&exe)?;

    #[cfg(target_os = "linux")]
    install_linux(&exe)?;

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    eprintln!("[marrow] service install is only supported on macOS and Linux");

    Ok(())
}

// ── macOS ─────────────────────────────────────────────────────────────────────

#[cfg(target_os = "macos")]
fn install_macos(exe: &Path) -> Result<()> {
    let plist = generate_plist(exe);
    let agents_dir = dirs::home_dir()
        .context("cannot resolve home dir")?
        .join("Library")
        .join("LaunchAgents");
    std::fs::create_dir_all(&agents_dir)?;

    let plist_path = agents_dir.join("dev.marrow.daemon.plist");
    std::fs::write(&plist_path, &plist)?;
    eprintln!("[marrow] wrote {}", plist_path.display());

    let status = std::process::Command::new("launchctl")
        .args(["load", "-w", plist_path.to_str().unwrap_or_default()])
        .status()
        .context("running launchctl load")?;

    if status.success() {
        println!("[marrow] daemon service loaded via launchctl.");
    } else {
        eprintln!("[marrow] launchctl exited with {status}; check the plist manually.");
    }
    Ok(())
}

pub fn generate_plist(exe: &Path) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>dev.marrow.daemon</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe}</string>
        <string>daemon</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardErrorPath</key>
    <string>{home}/.marrow/daemon.log</string>
    <key>StandardOutPath</key>
    <string>{home}/.marrow/daemon.log</string>
</dict>
</plist>"#,
        exe = exe.display(),
        home = dirs::home_dir().unwrap_or_default().display(),
    )
}

// ── Linux ─────────────────────────────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn install_linux(exe: &Path) -> Result<()> {
    let unit = generate_systemd_unit(exe);
    let unit_dir = dirs::home_dir()
        .context("cannot resolve home dir")?
        .join(".config")
        .join("systemd")
        .join("user");
    std::fs::create_dir_all(&unit_dir)?;

    let unit_path = unit_dir.join("marrow.service");
    std::fs::write(&unit_path, &unit)?;
    eprintln!("[marrow] wrote {}", unit_path.display());

    let status = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", "marrow"])
        .status()
        .context("running systemctl")?;

    if status.success() {
        println!("[marrow] daemon service enabled via systemctl.");
    } else {
        eprintln!("[marrow] systemctl exited with {status}; check the unit file manually.");
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
pub fn generate_systemd_unit(exe: &Path) -> String {
    format!(
        r#"[Unit]
Description=Marrow AST Context Engine Daemon
After=network.target

[Service]
ExecStart={exe} daemon
Restart=on-failure
StandardOutput=append:{home}/.marrow/daemon.log
StandardError=append:{home}/.marrow/daemon.log

[Install]
WantedBy=default.target
"#,
        exe = exe.display(),
        home = dirs::home_dir().unwrap_or_default().display(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn plist_content_contains_binary_path() {
        let bin = PathBuf::from("/usr/local/bin/marrow");
        let plist = generate_plist(&bin);
        assert!(plist.contains("/usr/local/bin/marrow"), "plist missing binary: {plist}");
        assert!(plist.contains("daemon"), "plist must pass 'daemon' arg: {plist}");
    }

    #[test]
    fn systemd_unit_contains_binary_path() {
        let bin = PathBuf::from("/usr/local/bin/marrow");
        let unit = generate_systemd_unit(&bin);
        assert!(unit.contains("/usr/local/bin/marrow daemon"), "unit missing exec: {unit}");
    }
}
