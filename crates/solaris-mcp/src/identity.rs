use std::fs::OpenOptions;
use std::io::{ErrorKind, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

const MINIMUM_KEY_BYTES: usize = 32;

#[derive(Clone)]
pub struct McpIdentityKey {
    version: String,
    key: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct StoredIdentityKey {
    version: String,
    key: String,
}

impl McpIdentityKey {
    pub fn new(version: impl Into<String>, key: impl Into<Vec<u8>>) -> Result<Self, String> {
        let version = version.into();
        let key = key.into();
        if version.trim().is_empty() {
            return Err("MCP identity key version cannot be empty".to_owned());
        }
        if key.len() < MINIMUM_KEY_BYTES {
            return Err(format!(
                "MCP identity key must contain at least {MINIMUM_KEY_BYTES} bytes"
            ));
        }
        Ok(Self { version, key })
    }

    pub fn load_or_create(path: &Path) -> std::io::Result<Self> {
        match Self::load(path) {
            Ok(key) => return Ok(key),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let stored = StoredIdentityKey {
            version: format!("host-key-{}", uuid::Uuid::now_v7()),
            key: (0..4).map(|_| uuid::Uuid::now_v7().to_string()).collect(),
        };
        let encoded = serde_json::to_vec(&stored).map_err(std::io::Error::other)?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(mut file) => {
                file.write_all(&encoded)?;
                file.sync_all()?;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => return Self::load(path),
            Err(error) => return Err(error),
        }
        Self::from_stored(stored)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    fn load(path: &Path) -> std::io::Result<Self> {
        let stored: StoredIdentityKey = serde_json::from_slice(&std::fs::read(path)?)
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))?;
        Self::from_stored(stored)
    }

    fn from_stored(stored: StoredIdentityKey) -> std::io::Result<Self> {
        Self::new(stored.version, stored.key.into_bytes())
            .map_err(|error| std::io::Error::new(ErrorKind::InvalidData, error))
    }
}

#[cfg(test)]
#[path = "identity_test.rs"]
mod identity_test;
