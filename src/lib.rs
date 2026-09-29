pub mod herdr;
pub mod identity;
pub mod relay;
pub mod service;
pub mod store;
pub mod supervision;
pub mod work;

pub const PLUGIN_ID: &str = "youssef-tharwat.agent-mail";
pub const BODY_LIMIT: usize = 8 * 1024;
pub const SUMMARY_LIMIT: usize = 240;

pub fn now() -> anyhow::Result<i64> {
    Ok(i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?)
}

pub fn name(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty()
            && value.len() <= 48
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
        "names must be 1–48 ASCII letters, digits, dots, dashes or underscores"
    );
    Ok(())
}

pub fn bounded(value: &str, limit: usize, label: &str) -> anyhow::Result<()> {
    anyhow::ensure!(value.len() <= limit, "{label} exceeds {limit} UTF-8 bytes");
    anyhow::ensure!(!value.contains('\0'), "{label} contains a NUL byte");
    Ok(())
}

pub mod events;

pub mod hooks;
pub mod recovery;

pub mod codex;

pub mod attention;

pub mod stream;

pub mod doctor;
