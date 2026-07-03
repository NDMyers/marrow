//! Desktop application mode — native webview window backed by the daemon.
//!
//! Gated behind the `desktop` Cargo feature. When the feature is disabled,
//! all public functions print an informational message and exit.

use anyhow::Result;

#[cfg(all(feature = "desktop", any(target_os = "macos", target_os = "linux")))]
use crate::packaging;

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
        // Detect the WebView2 runtime via the official loader API
        // (GetAvailableCoreWebView2BrowserVersionString), which covers
        // Evergreen per-machine, per-user, and fixed-version installs.
        // Do not probe the registry instead: the runtime's keys sit in the
        // 32-bit (WOW6432Node) view on 64-bit Windows and their layout is
        // undocumented, which previously caused false negatives here.
        if let Err(e) = wry::webview_version() {
            eprintln!(
                "[marrow] Error: Microsoft WebView2 Runtime not found ({e}).\n\
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

    // Decode embedded tray icon PNG → RGBA for the system tray.
    let tray_png = include_bytes!("../assets/tray_32.png");
    let tray_img = image::load_from_memory(tray_png)
        .expect("embedded tray icon is valid PNG")
        .into_rgba8();
    let (tw, th) = tray_img.dimensions();
    let icon = tray_icon::Icon::from_rgba(tray_img.into_raw(), tw, th)?;

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
             Consider installing globally: npm install -g @nickm-swe/marrow"
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

    #[cfg(target_os = "macos")]
    {
        let app_path = dirs::home_dir()
            .map(|h| h.join("Applications/Marrow.app"))
            .unwrap_or_default();
        println!(
            "App path:     {} [{}]",
            app_path.display(),
            if app_path.exists() {
                "exists"
            } else {
                "missing"
            }
        );

        // Launcher target: extract exec path from the launcher shell script.
        let launcher_path = dirs::home_dir()
            .map(|h| h.join("Applications/Marrow.app/Contents/MacOS/marrow-launcher"))
            .unwrap_or_default();
        let launcher_target_printed = if let Ok(content) = std::fs::read_to_string(&launcher_path) {
            let mut printed = false;
            for line in content.lines() {
                if let Some(rest) = line.strip_prefix("exec ") {
                    let target = rest.trim().trim_matches('"');
                    let target_path = std::path::PathBuf::from(target);
                    println!(
                        "Launcher target: {} [{}]",
                        target,
                        if target_path.exists() {
                            "exists"
                        } else {
                            "missing"
                        }
                    );
                    printed = true;
                    break;
                }
            }
            printed
        } else {
            false
        };
        if !launcher_target_printed {
            println!("Launcher target: {} [missing]", launcher_path.display());
        }

        // Gatekeeper check (non-fatal).
        let xattr_result = std::process::Command::new("xattr")
            .args([
                "-p",
                "com.apple.quarantine",
                app_path.to_str().unwrap_or(""),
            ])
            .output();
        match xattr_result {
            Ok(output) if !output.stdout.is_empty() => {
                println!("Gatekeeper: quarantined");
                eprintln!(
                    "[marrow] Advisory: run `xattr -dr com.apple.quarantine {}` to clear.",
                    app_path.display()
                );
            }
            Ok(_) => {
                println!("Gatekeeper: clear");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // xattr not available; omit the line.
            }
            Err(_) => {
                // Other error; omit the line.
            }
        }
    }

    #[cfg(target_os = "linux")]
    {
        let desktop_path = dirs::home_dir()
            .map(|h| h.join(".local/share/applications/marrow.desktop"))
            .unwrap_or_default();
        println!(
            "App path:     {} [{}]",
            desktop_path.display(),
            if desktop_path.exists() {
                "exists"
            } else {
                "missing"
            }
        );

        // Launcher target: extract Exec= line from .desktop file.
        let launcher_target_printed = if let Ok(content) = std::fs::read_to_string(&desktop_path) {
            let mut printed = false;
            for line in content.lines() {
                if let Some(rest) = line.strip_prefix("Exec=") {
                    // The Exec= value may be quoted: `"<path>" ui-app open`.
                    // Extract the binary path (first token, unquoted).
                    let exe = rest.trim_start_matches('"');
                    let exe = exe.split_whitespace().next().unwrap_or(rest);
                    let exe = exe.trim_matches('"');
                    let target_path = std::path::PathBuf::from(exe);
                    println!(
                        "Launcher target: {} [{}]",
                        exe,
                        if target_path.exists() {
                            "exists"
                        } else {
                            "missing"
                        }
                    );
                    printed = true;
                    break;
                }
            }
            printed
        } else {
            false
        };
        if !launcher_target_printed {
            println!("Launcher target: {} [missing]", desktop_path.display());
        }
    }

    #[cfg(target_os = "windows")]
    {
        let lnk_path = dirs::data_dir()
            .map(|d| d.join("Microsoft\\Windows\\Start Menu\\Programs\\Marrow.lnk"))
            .unwrap_or_default();
        println!(
            "App path:     {} [{}]",
            lnk_path.display(),
            if lnk_path.exists() {
                "exists"
            } else {
                "missing"
            }
        );
        println!("Launcher target: (run 'marrow ui-app enable' to configure)");
    }

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
        if !start_menu.exists() {
            return false;
        }
        // Validate the Shell Link magic bytes [0x4C, 0x00, 0x00, 0x00].
        match std::fs::read(&start_menu) {
            Ok(bytes) if bytes.len() >= 4 => bytes[..4] == [0x4C, 0x00, 0x00, 0x00],
            _ => false,
        }
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
    let staging_override = packaging::staging_root_override();
    let root = staging_override
        .clone()
        .or_else(dirs::home_dir)
        .ok_or_else(|| anyhow::anyhow!("Cannot determine staging root or home directory"))?;
    let app_path = packaging::stage_macos_bundle(&root, exe_path)?;

    // Register with Launch Services (non-fatal).
    if staging_override.is_none() {
        match std::process::Command::new(
            "/System/Library/Frameworks/CoreServices.framework/Versions/A/Frameworks/\
             LaunchServices.framework/Versions/A/Support/lsregister",
        )
        .args(["-R", "-f", app_path.to_str().unwrap_or("")])
        .status()
        {
            Ok(status) if !status.success() => {
                eprintln!("[marrow] Warning: lsregister returned non-zero status.");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("[marrow] Warning: lsregister not found; Launch Services not updated.");
            }
            Err(e) => {
                eprintln!("[marrow] Warning: lsregister failed: {e}");
            }
            Ok(_) => {}
        }
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
    // Warn if running as root — registration will land in /root/.local/ and
    // be invisible to other users.
    if unsafe { libc::getuid() } == 0 {
        eprintln!(
            "[marrow] Warning: Running as root. Desktop registration will be scoped to \
/root/.local/. Use a non-root user or install system-wide manually."
        );
    }

    let staging_override = packaging::staging_root_override();
    let root = staging_override
        .clone()
        .or_else(dirs::home_dir)
        .ok_or_else(|| anyhow::anyhow!("Cannot determine staging root or home directory"))?;
    let assets = packaging::stage_linux_desktop_assets(&root, exe_path)?;

    // Update the desktop database (non-fatal).
    if staging_override.is_none() {
        match std::process::Command::new("update-desktop-database")
            .arg(
                assets
                    .desktop_path
                    .parent()
                    .and_then(|path| path.to_str())
                    .unwrap_or(""),
            )
            .status()
        {
            Ok(status) if !status.success() => {
                eprintln!("[marrow] Warning: update-desktop-database returned non-zero status.");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!(
                    "[marrow] Warning: update-desktop-database not found; desktop database not updated."
                );
            }
            Err(e) => {
                eprintln!("[marrow] Warning: update-desktop-database failed: {e}");
            }
            Ok(_) => {}
        }
    }

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
    // Creates a Shell Link (.lnk) in the Start Menu Programs folder using the
    // `mslnk` crate. The .lnk file is indexed by Windows Search and appears in
    // the Start Menu correctly. The icon is pulled from the exe at index 0.
    //
    // Fallback note: if `mslnk` is found to be unmaintained or non-functional,
    // embed a minimal `marrow-launcher.exe` binary that calls the Rust binary
    // with `ui-app open` arguments and place it in the Start Menu folder.
    let start_menu = dirs::data_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot determine data directory"))?
        .join("Microsoft\\Windows\\Start Menu\\Programs");
    std::fs::create_dir_all(&start_menu)?;

    let lnk_path = start_menu.join("Marrow.lnk");
    let mut lnk = mslnk::ShellLink::new(exe_path)?;
    lnk.set_name(Some("Marrow".to_string()));
    lnk.set_arguments(Some("ui-app open".to_string()));
    lnk.create_lnk(&lnk_path)?;
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
