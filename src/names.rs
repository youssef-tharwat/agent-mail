//! Validated identities used at attention boundaries.
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

macro_rules! name_type {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl $name {
            /// Validate an ASCII coordination name.
            /// # Errors
            /// The value is empty, exceeds 48 bytes, or contains unsupported characters.
            pub fn new(value: &str) -> Result<Self> {
                crate::name(value)?;
                Ok(Self(value.into()))
            }
            /// The validated name as text.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
        impl TryFrom<String> for $name {
            type Error = anyhow::Error;
            fn try_from(value: String) -> Result<Self> {
                crate::name(&value)?;
                Ok(Self(value))
            }
        }
        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
        impl FromStr for $name {
            type Err = anyhow::Error;
            fn from_str(value: &str) -> Result<Self> {
                Self::new(value)
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}
name_type!(GroupName, "Validated coordination group name.");
name_type!(TaskId, "Validated task identifier within a group.");
name_type!(
    ParticipantName,
    "Validated participant name within a group."
);

/// Validated runtime or lifecycle-hook session identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SessionId(String);
impl SessionId {
    /// Validate a bounded session identity.
    /// # Errors
    /// The identity is empty, exceeds 160 bytes, or contains whitespace or controls.
    pub fn new(value: &str) -> Result<Self> {
        ensure!(
            !value.is_empty()
                && value.len() <= 160
                && !value.chars().any(|c| c.is_whitespace() || c.is_control()),
            "invalid session identity"
        );
        Ok(Self(value.into()))
    }
    /// The validated identity as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl TryFrom<String> for SessionId {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        Self::new(&value)
    }
}
impl From<SessionId> for String {
    fn from(value: SessionId) -> Self {
        value.0
    }
}

/// A supported delivery consumer; arbitrary strings cannot claim attention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum DeliveryConsumer {
    /// Native runtime worker.
    Native,
    /// Herdr runtime worker.
    Herdr,
    /// A lifecycle hook in one authenticated session.
    Hook(SessionId),
    /// Explicit model-facing watch integration.
    Watch(ParticipantName),
}
impl fmt::Display for DeliveryConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native => f.write_str("native"),
            Self::Herdr => f.write_str("herdr"),
            Self::Hook(session) => write!(f, "hook:{}", session.as_str()),
            Self::Watch(name) => write!(f, "watch:{name}"),
        }
    }
}
impl FromStr for DeliveryConsumer {
    type Err = anyhow::Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "native" => Ok(Self::Native),
            "herdr" => Ok(Self::Herdr),
            _ if value.starts_with("hook:") => Ok(Self::Hook(SessionId::new(&value[5..])?)),
            _ if value.starts_with("watch:") => Ok(Self::Watch(value[6..].parse()?)),
            _ => anyhow::bail!("unsupported attention consumer"),
        }
    }
}
impl TryFrom<String> for DeliveryConsumer {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}
impl From<DeliveryConsumer> for String {
    fn from(value: DeliveryConsumer) -> Self {
        value.to_string()
    }
}
