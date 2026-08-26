use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutableIdentity {
    pub(crate) canonical_path: PathBuf,
    pub(crate) path_digest: String,
    pub(crate) content_digest: String,
}

impl ExecutableIdentity {
    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }

    pub fn path_digest(&self) -> &str {
        &self.path_digest
    }

    pub fn content_digest(&self) -> &str {
        &self.content_digest
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutableError {
    #[error("{category} for executable identity {identity_digest}")]
    Rejected {
        category: &'static str,
        identity_digest: String,
    },
    #[error("{category} for executable identity {identity_digest}")]
    Io {
        category: &'static str,
        identity_digest: String,
        #[source]
        source: std::io::Error,
    },
}

impl ExecutableError {
    pub(crate) fn new(category: &'static str, identity_digest: String, source: Option<std::io::Error>) -> Self {
        match source {
            Some(source) => Self::Io {
                category,
                identity_digest,
                source,
            },
            None => Self::Rejected {
                category,
                identity_digest,
            },
        }
    }

    pub fn category(&self) -> &'static str {
        match self {
            Self::Rejected { category, .. } | Self::Io { category, .. } => category,
        }
    }

    pub fn identity_digest(&self) -> &str {
        match self {
            Self::Rejected { identity_digest, .. } | Self::Io { identity_digest, .. } => identity_digest,
        }
    }
}

pub fn executable_path_identity(path: &Path) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"solaris.process/executable-path/v1\0");
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;

        let bytes = path.as_os_str().as_bytes();
        hasher.update(b"unix/os-str-bytes\0");
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;

        let wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
        hasher.update(b"windows/utf16le\0");
        hasher.update((wide.len() as u64).to_be_bytes());
        for code_unit in wide {
            hasher.update(code_unit.to_le_bytes());
        }
    }
    format!("sha256:{:x}", hasher.finalize())
}
