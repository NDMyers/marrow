//! Desktop application mode — native webview window backed by the daemon.
//!
//! Gated behind the `desktop` Cargo feature. When the feature is disabled,
//! all public functions print an informational message and exit.

use anyhow::Result;

/// The dashboard URL served by the daemon.
#[cfg(feature = "desktop")]
const DASHBOARD_URL: &str = "http://127.0.0.1:8765";

// ── Feature-gated implementation ──────────────────────────────────────────────

#[cfg(not(feature = "desktop"))]
pub fn open_app() -> Result<()> {
    eprintln!("[marrow] Desktop support is not compiled in.");
    eprintln!("[marrow] Rebuild with: cargo build --features desktop");
    std::process::exit(1);
}

#[cfg(not(feature = "desktop"))]
pub fn enable() -> Result<()> {
    eprintln!("[marrow] Desktop support is not compiled in.");
    eprintln!("[marrow] Rebuild with: cargo build --features desktop");
    std::process::exit(1);
}

#[cfg(not(feature = "desktop"))]
pub fn disable() -> Result<()> {
    eprintln!("[marrow] Desktop support is not compiled in.");
    eprintln!("[marrow] Rebuild with: cargo build --features desktop");
    std::process::exit(1);
}

#[cfg(not(feature = "desktop"))]
pub fn status() -> Result<()> {
    eprintln!("[marrow] Desktop support is not compiled in.");
    eprintln!("[marrow] Rebuild with: cargo build --features desktop");
    std::process::exit(1);
}

// ── Desktop feature enabled ───────────────────────────────────────────────────

#[cfg(feature = "desktop")]
pub fn open_app() -> Result<()> {
    // Ensure the daemon is running before launching the webview.
    // Use a small current-thread runtime for this single async call.
    // This is safe because open_app() is called from the process main thread
    // BEFORE any multithreaded Tokio runtime is constructed.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {e}"))?;
    if let Err(e) = rt.block_on(crate::ipc::ensure_daemon_running()) {
        anyhow::bail!(
            "Could not start the Marrow daemon: {e}\n\
             The desktop app requires the daemon to be running."
        );
    }
    drop(rt);

    // Check single-instance via lockfile.
    let lock_path = dirs::home_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
        .join(".marrow")
        .join("ui-app.lock");
    let _ = std::fs::create_dir_all(lock_path.parent().unwrap_or(std::path::Path::new("/tmp")));

    if lock_path.exists() {
        // Check if the PID in the lockfile is still running.
        if let Ok(contents) = std::fs::read_to_string(&lock_path) {
            if let Ok(pid) = contents.trim().parse::<u32>() {
                if is_process_running(pid) {
                    eprintln!("[marrow] Desktop app is already running (PID {pid}). Focusing existing window.");
                    return Ok(());
                }
            }
        }
        // Stale lockfile — remove it.
        let _ = std::fs::remove_file(&lock_path);
    }

    // Write our PID to the lockfile.
    std::fs::write(&lock_path, std::process::id().to_string())?;

    // Run the webview — this blocks until the app quits.
    let result = run_webview();

    // Clean up lockfile on exit.
    let _ = std::fs::remove_file(&lock_path);

    result
}

#[cfg(feature = "desktop")]
fn is_process_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // signal 0 checks if process exists without sending a signal
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false // On non-Unix, assume stale
    }
}

#[cfg(feature = "desktop")]
fn run_webview() -> Result<()> {
    use tao::event_loop::EventLoop;
    use tray_icon::{
        menu::{Menu, MenuEvent, MenuItem},
        TrayIconBuilder,
    };
    use wry::WebViewBuilder;

    #[cfg(target_os = "linux")]
    {
        // Check for WebKitGTK availability on Linux.
        if std::process::Command::new("pkg-config")
            .args(["--exists", "webkit2gtk-4.1"])
            .status()
            .map(|s| !s.success())
            .unwrap_or(true)
        {
            eprintln!(
                "[marrow] Error: WebKitGTK runtime libraries not found.\n\
                 Please install the required packages:\n\
                 \n\
                   Ubuntu/Debian: sudo apt install libwebkit2gtk-4.1-dev\n\
                   Fedora:        sudo dnf install webkit2gtk4.1-devel\n\
                   Arch:          sudo pacman -S webkit2gtk-4.1\n"
            );
            std::process::exit(1);
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Check for WebView2 runtime on Windows.
        let webview2_key =
            "SOFTWARE\\Microsoft\\EdgeUpdate\\Clients\\{F3017226-FE2A-4295-8BEF-AE91B6C6C5CF}";
        if winreg::RegKey::predef(winreg::enums::HKEY_LOCAL_MACHINE)
            .open_subkey(webview2_key)
            .is_err()
            && winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER)
                .open_subkey(webview2_key)
                .is_err()
        {
            eprintln!(
                "[marrow] Error: Microsoft WebView2 Runtime not found.\n\
                 Please install it from:\n\
                 https://developer.microsoft.com/en-us/microsoft-edge/webview2/\n\
                 \n\
                 Download the \"Evergreen Bootstrapper\" or \"Evergreen Standalone Installer\"."
            );
            std::process::exit(1);
        }
    }

    // Build tray menu
    let menu = Menu::new();
    let show_item = MenuItem::new("Show", true, None);
    let hide_item = MenuItem::new("Hide", true, None);
    let quit_item = MenuItem::new("Quit", true, None);
    let show_id = show_item.id().clone();
    let hide_id = hide_item.id().clone();
    let quit_id = quit_item.id().clone();
    menu.append(&show_item)?;
    menu.append(&hide_item)?;
    menu.append(&quit_item)?;

    // Use a simple 16x16 RGBA icon (a filled square as a minimal icon).
    let icon_rgba = [0x4A, 0xB5, 0x6F, 0xFF].repeat(16 * 16); // green square
    let icon = tray_icon::Icon::from_rgba(icon_rgba, 16, 16)?;

    // Create the event loop FIRST — on macOS this initializes NSApplication
    // and establishes the window server connection, which is required before
    // any CoreGraphics/tray operations.
    let event_loop = EventLoop::new();

    let _tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("Marrow")
        .with_icon(icon)
        .build()?;

    let window = tao::window::WindowBuilder::new()
        .with_title("Marrow Dashboard")
        .with_inner_size(tao::dpi::LogicalSize::new(1200.0, 800.0))
        .build(&event_loop)?;

    let _webview = WebViewBuilder::new()
        .with_url(DASHBOARD_URL)
        .build(&window)?;

    // Run the event loop (diverges — never returns).
    event_loop.run(move |event, _, control_flow| {
        *control_flow = tao::event_loop::ControlFlow::Wait;

        // Handle tray menu events.
        if let Ok(event) = MenuEvent::receiver().try_recv() {
            if event.id == show_id {
                window.set_visible(true);
            } else if event.id == hide_id {
                window.set_visible(false);
            } else if event.id == quit_id {
                *control_flow = tao::event_loop::ControlFlow::Exit;
            }
        }

        if let tao::event::Event::WindowEvent {
            event: tao::event::WindowEvent::CloseRequested,
            ..
        } = event
        {
            // Hide to tray instead of quitting.
            window.set_visible(false);
        }
    });
}

// ── OS Registration ───────────────────────────────────────────────────────────

#[cfg(feature = "desktop")]
pub fn enable() -> Result<()> {
    let exe_path = std::env::current_exe()?;

    // Warn if the binary is in a temporary/cache location.
    let exe_str = exe_path.to_string_lossy();
    if exe_str.contains("_npx") || exe_str.contains("npx-cache") || exe_str.contains(".npm/_") {
        eprintln!(
            "[marrow] Warning: The binary appears to be in an npm cache directory.\n\
             Registration may break if the cache is cleared.\n\
             Consider installing globally: npm install -g marrow"
        );
    }

    #[cfg(target_os = "macos")]
    {
        macos_enable(&exe_path)?;
    }
    #[cfg(target_os = "linux")]
    {
        linux_enable(&exe_path)?;
    }
    #[cfg(target_os = "windows")]
    {
        windows_enable(&exe_path)?;
    }

    eprintln!("[marrow] Desktop app registered. You can launch it from your OS application menu.");
    Ok(())
}

#[cfg(feature = "desktop")]
pub fn disable() -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        macos_disable()?;
    }
    #[cfg(target_os = "linux")]
    {
        linux_disable()?;
    }
    #[cfg(target_os = "windows")]
    {
        windows_disable()?;
    }

    eprintln!("[marrow] Desktop app registration removed.");
    Ok(())
}

#[cfg(feature = "desktop")]
pub fn status() -> Result<()> {
    let registered = is_registered();
    let app_running = is_app_running();

    println!(
        "Registration: {}",
        if registered { "enabled" } else { "disabled" }
    );
    println!(
        "App process:  {}",
        if app_running {
            "running"
        } else {
            "not running"
        }
    );
    Ok(())
}

#[cfg(feature = "desktop")]
fn is_registered() -> bool {
    #[cfg(target_os = "macos")]
    {
        let app_path = dirs::home_dir()
            .map(|h| h.join("Applications/Marrow.app"))
            .unwrap_or_default();
        app_path.exists()
    }
    #[cfg(target_os = "linux")]
    {
        let desktop_path = dirs::home_dir()
            .map(|h| h.join(".local/share/applications/marrow.desktop"))
            .unwrap_or_default();
        desktop_path.exists()
    }
    #[cfg(target_os = "windows")]
    {
        let start_menu = dirs::data_dir()
            .map(|d| d.join("Microsoft\\Windows\\Start Menu\\Programs\\Marrow.lnk"))
            .unwrap_or_default();
        start_menu.exists()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        false
    }
}

#[cfg(feature = "desktop")]
fn is_app_running() -> bool {
    let lock_path = dirs::home_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp"))
        .join(".marrow")
        .join("ui-app.lock");

    if let Ok(contents) = std::fs::read_to_string(&lock_path) {
        if let Ok(pid) = contents.trim().parse::<u32>() {
            return is_process_running(pid);
        }
    }
    false
}

// ── macOS registration ────────────────────────────────────────────────────────

#[cfg(all(feature = "desktop", target_os = "macos"))]
fn macos_enable(exe_path: &std::path::Path) -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let app_dir = home.join("Applications/Marrow.app/Contents/MacOS");
    std::fs::create_dir_all(&app_dir)?;

    // Create Info.plist
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
    <key>LSMinimumSystemVersion</key>
    <string>11.0</string>
</dict>
</plist>"#;
    let plist_path = home.join("Applications/Marrow.app/Contents/Info.plist");
    std::fs::write(&plist_path, plist_content)?;

    // Create a launcher script that calls the actual binary.
    let launcher_path = app_dir.join("marrow-launcher");
    let launcher_content = format!("#!/bin/sh\nexec \"{}\" ui-app open\n", exe_path.display());
    std::fs::write(&launcher_path, launcher_content)?;

    // Make it executable.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&launcher_path, std::fs::Permissions::from_mode(0o755))?;
    }

    Ok(())
}

#[cfg(all(feature = "desktop", target_os = "macos"))]
fn macos_disable() -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let app_path = home.join("Applications/Marrow.app");
    if app_path.exists() {
        std::fs::remove_dir_all(&app_path)?;
    } else {
        eprintln!("[marrow] No registration found (nothing to remove).");
    }
    Ok(())
}

// ── Linux registration ────────────────────────────────────────────────────────

#[cfg(all(feature = "desktop", target_os = "linux"))]
fn linux_enable(exe_path: &std::path::Path) -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let apps_dir = home.join(".local/share/applications");
    std::fs::create_dir_all(&apps_dir)?;

    let desktop_entry = format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Name=Marrow\n\
         Comment=AST Context Engine Dashboard\n\
         Exec=\"{}\" ui-app open\n\
         Terminal=false\n\
         Categories=Development;\n",
        exe_path.display()
    );

    std::fs::write(apps_dir.join("marrow.desktop"), desktop_entry)?;
    Ok(())
}

#[cfg(all(feature = "desktop", target_os = "linux"))]
fn linux_disable() -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot determine home directory"))?;
    let desktop_path = home.join(".local/share/applications/marrow.desktop");
    if desktop_path.exists() {
        std::fs::remove_file(&desktop_path)?;
    } else {
        eprintln!("[marrow] No registration found (nothing to remove).");
    }
    Ok(())
}

// ── Windows registration ──────────────────────────────────────────────────────

#[cfg(all(feature = "desktop", target_os = "windows"))]
fn windows_enable(exe_path: &std::path::Path) -> Result<()> {
    // Create a .bat launcher in the Start Menu Programs folder.
    let start_menu = dirs::data_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot determine data directory"))?
        .join("Microsoft\\Windows\\Start Menu\\Programs");
    std::fs::create_dir_all(&start_menu)?;

    // Write a .bat file that launches the binary (simpler than COM shell link)
    let bat_content = format!("@echo off\r\n\"{}\" ui-app open\r\n", exe_path.display());
    std::fs::write(start_menu.join("Marrow.bat"), bat_content)?;

    // Also write a marker .lnk placeholder for status detection
    std::fs::write(start_menu.join("Marrow.lnk"), "placeholder")?;
    Ok(())
}

#[cfg(all(feature = "desktop", target_os = "windows"))]
fn windows_disable() -> Result<()> {
    let start_menu = dirs::data_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot determine data directory"))?
        .join("Microsoft\\Windows\\Start Menu\\Programs");

    let bat_path = start_menu.join("Marrow.bat");
    let lnk_path = start_menu.join("Marrow.lnk");

    let found = bat_path.exists() || lnk_path.exists();
    let _ = std::fs::remove_file(&bat_path);
    let _ = std::fs::remove_file(&lnk_path);

    if !found {
        eprintln!("[marrow] No registration found (nothing to remove).");
    }
    Ok(())
}
