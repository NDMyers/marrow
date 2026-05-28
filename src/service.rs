use anyhow::{Context as _, Result};
use std::{
    path::{Path, PathBuf},
    process::Command,
};

const MACOS_LABEL: &str = "dev.marrow.daemon";
#[cfg(target_os = "linux")]
const LINUX_UNIT_NAME: &str = "marrow-daemon.service";
#[cfg(target_os = "linux")]
const LINUX_LEGACY_UNIT_NAME: &str = "marrow.service";
#[cfg(target_os = "windows")]
const WINDOWS_TASK_NAME: &str = "MarrowDaemon";
#[cfg(any(target_os = "windows", test))]
const WINDOWS_TASK_XML_ENCODING: &str = "UTF-8";

pub struct ServiceStatus {
    pub configured: bool,
    pub running: bool,
    pub artifact: String,
}

#[cfg(any(target_os = "linux", test))]
enum LinuxEnablementProbe {
    Enabled,
    Disabled,
    Unavailable,
}

pub fn install() -> Result<()> {
    let exe = std::env::current_exe().context("resolving binary path")?;

    #[cfg(target_os = "macos")]
    install_macos(&exe)?;

    #[cfg(target_os = "linux")]
    install_linux(&exe)?;

    #[cfg(target_os = "windows")]
    install_windows(&exe)?;

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    eprintln!("[marrow] daemon install is not supported on this platform.");

    Ok(())
}

pub fn uninstall() -> Result<()> {
    #[cfg(target_os = "macos")]
    uninstall_macos()?;

    #[cfg(target_os = "linux")]
    uninstall_linux()?;

    #[cfg(target_os = "windows")]
    uninstall_windows()?;

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    eprintln!("[marrow] daemon uninstall is not supported on this platform.");

    Ok(())
}

pub async fn status() -> Result<()> {
    let runtime_running = crate::ipc::default_client()
        .health_check()
        .await
        .unwrap_or(false);
    let status = status_report(runtime_running)?;

    println!(
        "Autostart: {}",
        if status.configured {
            "configured"
        } else {
            "not configured"
        }
    );
    println!(
        "Runtime:   {}",
        if status.running {
            "running"
        } else {
            "not running"
        }
    );
    println!("Artifact:  {}", status.artifact);
    Ok(())
}

pub fn status_report(runtime_running: bool) -> Result<ServiceStatus> {
    #[cfg(target_os = "macos")]
    {
        let path = macos_plist_path()?;
        Ok(ServiceStatus {
            configured: path.exists(),
            running: runtime_running,
            artifact: path.display().to_string(),
        })
    }

    #[cfg(target_os = "linux")]
    {
        let canonical = linux_unit_path(LINUX_UNIT_NAME)?;
        let legacy = linux_unit_path(LINUX_LEGACY_UNIT_NAME)?;
        let canonical_exists = canonical.exists();
        let legacy_exists = legacy.exists();
        let artifact = if canonical_exists {
            canonical
        } else if legacy_exists {
            legacy
        } else {
            canonical
        };

        let configured = match linux_enabled_probe(LINUX_UNIT_NAME)? {
            LinuxEnablementProbe::Enabled => true,
            LinuxEnablementProbe::Disabled => match linux_enabled_probe(LINUX_LEGACY_UNIT_NAME)? {
                LinuxEnablementProbe::Enabled => true,
                LinuxEnablementProbe::Disabled => false,
                LinuxEnablementProbe::Unavailable => {
                    eprintln!(
                        "[marrow] Warning: systemctl unavailable; falling back to unit file presence for status."
                    );
                    canonical_exists || legacy_exists
                }
            },
            LinuxEnablementProbe::Unavailable => {
                eprintln!(
                    "[marrow] Warning: systemctl unavailable; falling back to unit file presence for status."
                );
                canonical_exists || legacy_exists
            }
        };

        Ok(ServiceStatus {
            configured,
            running: runtime_running,
            artifact: artifact.display().to_string(),
        })
    }

    #[cfg(target_os = "windows")]
    {
        Ok(ServiceStatus {
            configured: windows_task_exists().unwrap_or(false),
            running: runtime_running,
            artifact: format!("Task Scheduler/{}", WINDOWS_TASK_NAME),
        })
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        Ok(ServiceStatus {
            configured: false,
            running: runtime_running,
            artifact: "unsupported platform".to_string(),
        })
    }
}

#[cfg(target_os = "macos")]
fn install_macos(exe: &Path) -> Result<()> {
    let plist_path = macos_plist_path()?;
    let agents_dir = plist_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("launch agent path has no parent directory"))?;

    std::fs::create_dir_all(agents_dir)?;
    std::fs::write(&plist_path, generate_plist(exe))?;
    println!("[marrow] autostart configured: {}", plist_path.display());

    let plist_arg = plist_path.display().to_string();
    run_optional_command(
        "launchctl",
        &["load", "-w", &plist_arg],
        Some(format!("launch agent remains at {}", plist_path.display())),
    );
    Ok(())
}

#[cfg(target_os = "macos")]
fn uninstall_macos() -> Result<()> {
    let plist_path = macos_plist_path()?;

    if plist_path.exists() {
        let plist_arg = plist_path.display().to_string();
        run_optional_command(
            "launchctl",
            &["unload", "-w", &plist_arg],
            Some(format!(
                "launch agent remains at {} until removed",
                plist_path.display()
            )),
        );
        std::fs::remove_file(&plist_path)?;
    }

    println!("[marrow] autostart removed: {}", plist_path.display());
    Ok(())
}

pub fn generate_plist(exe: &Path) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
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
        label = MACOS_LABEL,
        exe = exe.display(),
        home = dirs::home_dir().unwrap_or_default().display(),
    )
}

#[cfg(target_os = "macos")]
fn macos_plist_path() -> Result<PathBuf> {
    Ok(home_dir()?
        .join("Library")
        .join("LaunchAgents")
        .join("dev.marrow.daemon.plist"))
}

#[cfg(target_os = "linux")]
fn install_linux(exe: &Path) -> Result<()> {
    let unit_path = linux_unit_path(LINUX_UNIT_NAME)?;
    if let Some(parent) = unit_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&unit_path, generate_systemd_unit(exe))?;

    let legacy = linux_unit_path(LINUX_LEGACY_UNIT_NAME)?;
    remove_if_exists(&legacy)?;

    run_optional_command(
        "systemctl",
        &["--user", "daemon-reload"],
        Some(format!("systemd unit remains at {}", unit_path.display())),
    );
    run_optional_command(
        "systemctl",
        &["--user", "disable", "--now", LINUX_LEGACY_UNIT_NAME],
        Some("legacy unit cleanup may require manual intervention".to_string()),
    );
    run_optional_command(
        "systemctl",
        &["--user", "enable", "--now", LINUX_UNIT_NAME],
        Some(format!("systemd unit remains at {}", unit_path.display())),
    );

    println!("[marrow] autostart configured: {}", unit_path.display());
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

#[cfg(target_os = "linux")]
fn uninstall_linux() -> Result<()> {
    let canonical = linux_unit_path(LINUX_UNIT_NAME)?;
    let legacy = linux_unit_path(LINUX_LEGACY_UNIT_NAME)?;

    run_optional_command(
        "systemctl",
        &["--user", "disable", "--now", LINUX_UNIT_NAME],
        Some(format!("remove {} manually if needed", canonical.display())),
    );
    run_optional_command(
        "systemctl",
        &["--user", "disable", "--now", LINUX_LEGACY_UNIT_NAME],
        Some(format!("remove {} manually if needed", legacy.display())),
    );

    remove_if_exists(&canonical)?;
    remove_if_exists(&legacy)?;

    run_optional_command(
        "systemctl",
        &["--user", "daemon-reload"],
        Some("systemd user state may still need daemon-reload".to_string()),
    );

    println!("[marrow] autostart removed: {}", canonical.display());
    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_unit_path(name: &str) -> Result<PathBuf> {
    Ok(home_dir()?
        .join(".config")
        .join("systemd")
        .join("user")
        .join(name))
}

#[cfg(target_os = "windows")]
fn install_windows(exe: &Path) -> Result<()> {
    let xml = generate_windows_task_xml(exe);
    let xml_file = write_windows_task_xml(&xml)?;
    let xml_arg = xml_file.display().to_string();

    run_optional_command(
        "schtasks",
        &["/Create", "/TN", WINDOWS_TASK_NAME, "/XML", &xml_arg, "/F"],
        Some(format!(
            "scheduled task definition remains at {}",
            xml_file.display()
        )),
    );

    println!(
        "[marrow] autostart configured: Task Scheduler/{}",
        WINDOWS_TASK_NAME
    );
    Ok(())
}

#[cfg(target_os = "windows")]
fn uninstall_windows() -> Result<()> {
    run_optional_command(
        "schtasks",
        &["/Delete", "/TN", WINDOWS_TASK_NAME, "/F"],
        Some(format!("scheduled task name is {}", WINDOWS_TASK_NAME)),
    );

    println!(
        "[marrow] autostart removed: Task Scheduler/{}",
        WINDOWS_TASK_NAME
    );
    Ok(())
}

#[cfg(target_os = "windows")]
fn windows_task_exists() -> Result<bool> {
    match Command::new("schtasks")
        .args(["/Query", "/TN", WINDOWS_TASK_NAME])
        .status()
    {
        Ok(status) => Ok(status.success()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "[marrow] Warning: schtasks not found; scheduled task state could not be queried."
            );
            Ok(false)
        }
        Err(err) => {
            eprintln!("[marrow] Warning: schtasks query failed: {err}");
            Ok(false)
        }
    }
}

#[cfg(target_os = "windows")]
fn generate_windows_task_xml(exe: &Path) -> String {
    let escaped = exe.display().to_string().replace('&', "&amp;");
    format!(
        r#"<?xml version="1.0" encoding="{encoding}"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{escaped}</Command>
      <Arguments>daemon</Arguments>
    </Exec>
  </Actions>
</Task>"#,
        encoding = WINDOWS_TASK_XML_ENCODING,
    )
}

#[cfg(target_os = "windows")]
fn write_windows_task_xml(contents: &str) -> Result<PathBuf> {
    let dir = std::env::temp_dir().join("marrow");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("marrow-daemon-task.xml");
    std::fs::write(&path, contents.as_bytes())?;
    Ok(path)
}

#[cfg(target_os = "linux")]
fn linux_enabled_probe(unit_name: &str) -> Result<LinuxEnablementProbe> {
    match Command::new("systemctl")
        .args(["--user", "is-enabled", unit_name])
        .output()
    {
        Ok(output) => Ok(parse_systemctl_is_enabled(
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            Ok(LinuxEnablementProbe::Unavailable)
        }
        Err(err) => {
            eprintln!("[marrow] Warning: systemctl status probe failed: {err}");
            Ok(LinuxEnablementProbe::Unavailable)
        }
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_systemctl_is_enabled(success: bool, stdout: &str, stderr: &str) -> LinuxEnablementProbe {
    if success {
        return LinuxEnablementProbe::Enabled;
    }

    if systemctl_user_manager_unavailable(stdout, stderr) {
        return LinuxEnablementProbe::Unavailable;
    }

    if matches!(
        stdout,
        "disabled"
            | "disabled-runtime"
            | "masked"
            | "masked-runtime"
            | "static"
            | "indirect"
            | "generated"
            | "transient"
            | "bad"
            | "not-found"
    ) {
        LinuxEnablementProbe::Disabled
    } else {
        LinuxEnablementProbe::Unavailable
    }
}

#[cfg(any(target_os = "linux", test))]
fn systemctl_user_manager_unavailable(stdout: &str, stderr: &str) -> bool {
    let combined = format!("{stdout}\n{stderr}").to_ascii_lowercase();
    combined.contains("failed to connect to bus")
        || combined.contains("failed to connect to user scope bus")
        || combined.contains("transport endpoint is not connected")
        || combined.contains("user manager")
        || combined.contains("no medium found")
}

fn run_optional_command(binary: &str, args: &[&str], warning: Option<String>) {
    match Command::new(binary).args(args).status() {
        Ok(status) if !status.success() => {
            if let Some(warning) = warning {
                eprintln!(
                    "[marrow] Warning: {} exited with {}; {}.",
                    binary, status, warning
                );
            } else {
                eprintln!("[marrow] Warning: {} exited with {}.", binary, status);
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            if let Some(warning) = warning {
                eprintln!("[marrow] Warning: {} not found; {}.", binary, warning);
            } else {
                eprintln!("[marrow] Warning: {} not found.", binary);
            }
        }
        Err(err) => {
            if let Some(warning) = warning {
                eprintln!("[marrow] Warning: {} failed: {}; {}.", binary, err, warning);
            } else {
                eprintln!("[marrow] Warning: {} failed: {}.", binary, err);
            }
        }
        Ok(_) => {}
    }
}

fn home_dir() -> Result<PathBuf> {
    dirs::home_dir().ok_or_else(|| anyhow::anyhow!("cannot resolve home directory"))
}

#[cfg(target_os = "linux")]
fn remove_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn parse_systemctl_requires_enablement() {
        assert!(matches!(
            parse_systemctl_is_enabled(true, "enabled", ""),
            LinuxEnablementProbe::Enabled
        ));
        assert!(matches!(
            parse_systemctl_is_enabled(false, "disabled", ""),
            LinuxEnablementProbe::Disabled
        ));
        assert!(matches!(
            parse_systemctl_is_enabled(false, "not-found", ""),
            LinuxEnablementProbe::Disabled
        ));
        assert!(matches!(
            parse_systemctl_is_enabled(false, "Failed to connect to bus", ""),
            LinuxEnablementProbe::Unavailable
        ));
    }

    #[test]
    fn parse_systemctl_uses_stderr_for_transport_failures() {
        assert!(matches!(
            parse_systemctl_is_enabled(false, "", "Failed to connect to bus: No medium found"),
            LinuxEnablementProbe::Unavailable
        ));
        assert!(matches!(
            parse_systemctl_is_enabled(
                false,
                "",
                "Failed to connect to user scope bus via local transport: No such file or directory"
            ),
            LinuxEnablementProbe::Unavailable
        ));
    }

    #[test]
    fn plist_content_contains_binary_path() {
        let bin = PathBuf::from("/usr/local/bin/marrow");
        let plist = generate_plist(&bin);
        assert!(
            plist.contains("/usr/local/bin/marrow"),
            "plist missing binary: {plist}"
        );
        assert!(
            plist.contains("daemon"),
            "plist must pass 'daemon' arg: {plist}"
        );
    }

    #[test]
    fn systemd_unit_contains_binary_path() {
        let bin = PathBuf::from("/usr/local/bin/marrow");
        let unit = generate_systemd_unit(&bin);
        assert!(
            unit.contains("/usr/local/bin/marrow daemon"),
            "unit missing exec: {unit}"
        );
    }

    #[test]
    fn systemd_unit_uses_sprint2_name() {
        let bin = PathBuf::from("/usr/local/bin/marrow");
        let unit = generate_systemd_unit(&bin);
        assert!(
            unit.contains("Description=Marrow AST Context Engine Daemon"),
            "unit description should remain stable: {unit}"
        );
        assert!(
            !unit.contains("marrow.service"),
            "unit contents should not hardcode the legacy marrow.service name: {unit}"
        );
    }

    #[test]
    fn windows_task_xml_declares_utf8() {
        assert_eq!(WINDOWS_TASK_XML_ENCODING, "UTF-8");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn uninstall_removes_legacy_and_canonical_linux_unit_when_present() {
        let temp_home = tempfile::tempdir().unwrap();
        let original_home = std::env::var_os("HOME");
        std::env::set_var("HOME", temp_home.path());

        let unit_dir = temp_home.path().join(".config/systemd/user");
        std::fs::create_dir_all(&unit_dir).unwrap();
        std::fs::write(unit_dir.join("marrow.service"), "legacy").unwrap();
        std::fs::write(unit_dir.join("marrow-daemon.service"), "canonical").unwrap();

        let result = uninstall();

        match original_home {
            Some(home) => std::env::set_var("HOME", home),
            None => std::env::remove_var("HOME"),
        }

        assert!(result.is_ok(), "uninstall should succeed: {result:?}");
        assert!(
            !unit_dir.join("marrow.service").exists(),
            "legacy unit should be removed"
        );
        assert!(
            !unit_dir.join("marrow-daemon.service").exists(),
            "canonical unit should be removed"
        );
    }
}
