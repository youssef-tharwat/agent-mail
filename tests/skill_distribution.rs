//! The distributed loader reads the full guide from the installed binary.
use anyhow::Result;
use std::process::Command;

#[test]
fn loader_and_state_free_cli_use_the_same_installed_guide() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let binary = env!("CARGO_BIN_EXE_agent-mail");
    let loader = include_str!("../skills/agent-mail/SKILL.md");
    assert!(loader.contains("agent-mail --skill"));
    assert!(!loader.contains("task update api"));
    let guide = Command::new(binary)
        .arg("--skill")
        .arg("--state-dir")
        .arg(temp.path().join("absent"))
        .output()?;
    assert!(guide.status.success());
    assert_eq!(
        String::from_utf8(guide.stdout)?,
        include_str!("../docs/agent-guide.md")
    );
    assert!(!temp.path().join("absent").exists());
    Ok(())
}
