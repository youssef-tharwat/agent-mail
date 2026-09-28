use anyhow::{Context, Result, ensure};
use std::{
    fs::OpenOptions,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Command,
};

const LABEL: &str = "io.github.youssef-tharwat.agent-mail";

fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn plist(binary: &Path, state: &Path) -> Result<String> {
    let binary = xml(binary.to_str().context("binary path is not UTF-8")?);
    let state = xml(state.to_str().context("state path is not UTF-8")?);
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>{LABEL}</string>
<key>ProgramArguments</key><array><string>{binary}</string><string>--state-dir</string><string>{state}</string><string>service</string><string>run</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>ThrottleInterval</key><integer>10</integer>
<key>ProcessType</key><string>Background</string>
<key>StandardOutPath</key><string>/dev/null</string>
<key>StandardErrorPath</key><string>/dev/null</string>
</dict></plist>
"#
    ))
}

fn target() -> Result<(String, PathBuf)> {
    ensure!(
        cfg!(target_os = "macos"),
        "automatic service installation is macOS-only; use service run under your supervisor on other Unix systems"
    );
    let output = Command::new("id").arg("-u").output()?;
    ensure!(output.status.success(), "cannot determine user ID");
    let uid = String::from_utf8(output.stdout)?.trim().to_owned();
    ensure!(
        uid.bytes().all(|c| c.is_ascii_digit()) && !uid.is_empty(),
        "invalid user ID"
    );
    let path = dirs::home_dir()
        .context("home directory unavailable")?
        .join("Library/LaunchAgents")
        .join(format!("{LABEL}.plist"));
    Ok((format!("gui/{uid}"), path))
}

fn launch(args: &[&str]) -> Result<()> {
    let output = Command::new("launchctl").args(args).output()?;
    ensure!(
        output.status.success(),
        "launchctl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

pub fn install(root: &Path) -> Result<()> {
    let (domain, path) = target()?;
    let root = root.canonicalize()?;
    let bin = root.join("bin/agent-mail");
    let body = plist(&bin, &root)?;
    if path.exists() {
        ensure!(
            std::fs::read_to_string(&path)? == body,
            "existing service uses another state directory; uninstall it explicitly first"
        );
    }
    ensure!(
        !crate::service::running(&root),
        "stop the service before installing/updating its executable"
    );
    std::fs::create_dir_all(bin.parent().context("missing binary parent")?)?;
    let temp = bin.with_extension("new");
    std::fs::copy(std::env::current_exe()?, &temp)?;
    std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o700))?;
    std::fs::rename(temp, &bin)?;
    std::fs::create_dir_all(path.parent().context("missing LaunchAgents directory")?)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(body.as_bytes())?;
    file.sync_all()?;
    let service_target = format!("{domain}/{LABEL}");
    let loaded = Command::new("launchctl")
        .args(["print", &service_target])
        .output()?
        .status
        .success();
    if !loaded {
        launch(&[
            "bootstrap",
            &domain,
            path.to_str().context("invalid plist path")?,
        ])?;
    }
    launch(&["kickstart", &service_target])
}

pub fn restore(root: &Path) -> Result<bool> {
    let (domain, path) = target()?;
    if !path.exists() {
        return Ok(false);
    }
    let root = root.canonicalize()?;
    ensure!(
        std::fs::read_to_string(&path)? == plist(&root.join("bin/agent-mail"), &root)?,
        "service belongs to another state directory"
    );
    let service_target = format!("{domain}/{LABEL}");
    if !Command::new("launchctl")
        .args(["print", &service_target])
        .output()?
        .status
        .success()
    {
        launch(&[
            "bootstrap",
            &domain,
            path.to_str().context("invalid plist path")?,
        ])?;
    }
    launch(&["kickstart", &service_target])?;
    Ok(true)
}

pub fn uninstall(root: &Path) -> Result<()> {
    let (domain, path) = target()?;
    if !path.exists() {
        return Ok(());
    }
    let root = root.canonicalize()?;
    ensure!(
        std::fs::read_to_string(&path)? == plist(&root.join("bin/agent-mail"), &root)?,
        "service belongs to another state directory"
    );
    let service_target = format!("{domain}/{LABEL}");
    if Command::new("launchctl")
        .args(["print", &service_target])
        .output()?
        .status
        .success()
    {
        launch(&["bootout", &service_target])?;
    }
    std::fs::remove_file(path)?;
    Ok(())
}

pub fn locator() -> Result<PathBuf> {
    Ok(dirs::config_dir()
        .context("config directory unavailable")?
        .join("agent-mail/state-path"))
}

pub fn state_root(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(root) = explicit {
        return Ok(root);
    }
    if std::env::var("HERDR_PLUGIN_ID").as_deref() == Ok(crate::PLUGIN_ID) {
        return std::env::var_os("HERDR_PLUGIN_STATE_DIR")
            .map(PathBuf::from)
            .context("missing plugin state directory");
    }
    let locator = locator()?;
    if locator.exists() {
        return Ok(PathBuf::from(std::fs::read_to_string(locator)?));
    }
    Ok(dirs::data_local_dir()
        .context("data directory unavailable")?
        .join("agent-mail"))
}

pub fn save_locator(root: &Path) -> Result<()> {
    let path = locator()?;
    std::fs::create_dir_all(path.parent().context("config parent missing")?)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(
        root.canonicalize()?
            .to_str()
            .context("state path is not UTF-8")?
            .as_bytes(),
    )?;
    file.sync_all()?;
    Ok(())
}
