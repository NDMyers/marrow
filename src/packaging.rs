#[cfg(feature = "desktop")]
use anyhow::Result;
#[cfg(feature = "desktop")]
use std::path::{Path, PathBuf};

#[cfg(all(feature = "desktop", target_os = "linux"))]
const LINUX_UI_APP_OPEN_ARGS: &str = "ui-app open";

#[cfg(all(feature = "desktop", target_os = "linux"))]
pub struct LinuxDesktopAssets {
    pub desktop_path: PathBuf,
    pub icon_path: PathBuf,
}

#[cfg(feature = "desktop")]
pub fn staging_root_override() -> Option<PathBuf> {
    std::env::var_os("MARROW_UI_APP_STAGE_ROOT").map(PathBuf::from)
}

#[cfg(all(feature = "desktop", target_os = "linux"))]
fn linux_desktop_exec_override() -> Option<String> {
    std::env::var("MARROW_DESKTOP_EXEC").ok()
}

#[cfg(all(feature = "desktop", target_os = "macos"))]
pub fn stage_macos_bundle(base_root: &Path, exe_path: &Path) -> Result<PathBuf> {
    let app_path = base_root.join("Applications/Marrow.app");
    let app_dir = app_path.join("Contents/MacOS");
    let resources_dir = app_path.join("Contents/Resources");
    std::fs::create_dir_all(&app_dir)?;
    std::fs::create_dir_all(&resources_dir)?;

    let png_data = include_bytes!("../assets/icon_256.png").to_vec();

    let block_len = 8u32 + png_data.len() as u32;
    let total_len = 8u32 + block_len;
    let mut icns_data: Vec<u8> = Vec::new();
    icns_data.extend_from_slice(b"icns");
    icns_data.extend_from_slice(&total_len.to_be_bytes());
    icns_data.extend_from_slice(b"ic08");
    icns_data.extend_from_slice(&block_len.to_be_bytes());
    icns_data.extend_from_slice(&png_data);
    std::fs::write(resources_dir.join("Marrow.icns"), icns_data)?;

    let plist_content = r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleExecutable</key>
    <string>marrow-launcher</string>
    <key>CFBundleIdentifier</key>
    <string>dev.marrow.app</string>
    <key>CFBundleName</key>
    <string>Marrow</string>
    <key>CFBundleVersion</key>
    <string>0.1.0</string>
    <key>CFBundleShortVersionString</key>
    <string>0.1.0</string>
    <key>CFBundleIconFile</key>
    <string>Marrow</string>
    <key>LSMinimumSystemVersion</key>
    <string>11.0</string>
</dict>
</plist>"#;
    std::fs::write(app_path.join("Contents/Info.plist"), plist_content)?;

    let bundled_binary_path = app_dir.join("marrow");
    std::fs::copy(exe_path, &bundled_binary_path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bundled_binary_path, std::fs::Permissions::from_mode(0o755))?;
    }

    let launcher_path = app_dir.join("marrow-launcher");
    let launcher_content = "#!/bin/sh\nHERE=\"$(CDPATH= cd -- \"$(dirname \"$0\")\" && pwd)\"\nexec \"$HERE/marrow\" ui-app open\n";
    std::fs::write(&launcher_path, launcher_content)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&launcher_path, std::fs::Permissions::from_mode(0o755))?;
    }

    Ok(app_path)
}

#[cfg(all(feature = "desktop", target_os = "linux"))]
pub fn linux_desktop_entry(exec_command: &str) -> String {
    format!(
        "[Desktop Entry]\n\
         Version=1.0\n\
         Type=Application\n\
         Name=Marrow\n\
         Comment=AST Context Engine Dashboard\n\
         Exec={}\n\
         Icon=marrow\n\
         Terminal=false\n\
         Categories=Development;\n",
        exec_command
    )
}

#[cfg(all(feature = "desktop", target_os = "linux"))]
pub fn stage_linux_desktop_assets(base_root: &Path, exe_path: &Path) -> Result<LinuxDesktopAssets> {
    let exec_command = linux_desktop_exec_override()
        .unwrap_or_else(|| format!("\"{}\" {}", exe_path.display(), LINUX_UI_APP_OPEN_ARGS));
    stage_linux_desktop_assets_with_exec(base_root, &exec_command)
}

#[cfg(all(feature = "desktop", target_os = "linux"))]
pub fn stage_linux_desktop_assets_with_exec(
    base_root: &Path,
    exec_command: &str,
) -> Result<LinuxDesktopAssets> {
    let desktop_path = base_root.join(".local/share/applications/marrow.desktop");
    let icon_path = base_root.join(".local/share/icons/hicolor/256x256/apps/marrow.png");
    if let Some(parent) = desktop_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if let Some(parent) = icon_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(&icon_path, include_bytes!("../assets/icon_256.png"))?;
    std::fs::write(&desktop_path, linux_desktop_entry(exec_command))?;

    Ok(LinuxDesktopAssets {
        desktop_path,
        icon_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "desktop", target_os = "macos"))]
    #[test]
    fn macos_bundle_stages_outside_home() {
        let tmpdir = tempfile::tempdir().unwrap();
        let exe = tmpdir.path().join("marrow-source");
        std::fs::write(&exe, b"#!/bin/sh\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        let app_path = stage_macos_bundle(tmpdir.path(), &exe).unwrap();
        assert!(app_path.ends_with("Applications/Marrow.app"));
        assert!(app_path.join("Contents/Info.plist").exists());
        assert!(app_path.join("Contents/MacOS/marrow-launcher").exists());
        assert!(app_path.join("Contents/MacOS/marrow").exists());
        assert!(app_path.join("Contents/Resources/Marrow.icns").exists());

        let launcher =
            std::fs::read_to_string(app_path.join("Contents/MacOS/marrow-launcher")).unwrap();
        assert!(launcher.contains("$HERE/marrow"));
        assert!(!launcher.contains(exe.to_string_lossy().as_ref()));
    }

    #[cfg(all(feature = "desktop", target_os = "linux"))]
    #[test]
    fn linux_assets_stage_outside_home() {
        let tmpdir = tempfile::tempdir().unwrap();
        let assets =
            stage_linux_desktop_assets(tmpdir.path(), Path::new("/usr/local/bin/marrow")).unwrap();
        assert!(assets
            .desktop_path
            .ends_with(".local/share/applications/marrow.desktop"));
        assert!(assets
            .icon_path
            .ends_with(".local/share/icons/hicolor/256x256/apps/marrow.png"));
        assert!(std::fs::read_to_string(&assets.desktop_path)
            .unwrap()
            .contains("ui-app open"));
        assert!(assets.icon_path.exists());
    }

    #[cfg(all(feature = "desktop", target_os = "linux"))]
    #[test]
    fn linux_assets_support_package_specific_exec_targets() {
        let tmpdir = tempfile::tempdir().unwrap();
        let assets =
            stage_linux_desktop_assets_with_exec(tmpdir.path(), "/usr/bin/marrow ui-app open")
                .unwrap();
        let desktop = std::fs::read_to_string(&assets.desktop_path).unwrap();
        assert!(desktop.contains("Exec=/usr/bin/marrow ui-app open"));

        let appimage_assets =
            stage_linux_desktop_assets_with_exec(tmpdir.path(), "AppRun").unwrap();
        let appimage_desktop = std::fs::read_to_string(&appimage_assets.desktop_path).unwrap();
        assert!(appimage_desktop.contains("Exec=AppRun"));
        assert!(!appimage_desktop.contains("ui-app open"));
    }
}
