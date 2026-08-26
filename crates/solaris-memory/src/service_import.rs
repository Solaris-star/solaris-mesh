use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use solaris_config::file_identity::OpenedFileIdentity;

use super::{MemoryMutation, MemoryScope, MemoryService, MemoryServiceError, apply_mutation, database_error};
use crate::paths::ENTRYPOINT_NAME;
use crate::store::parse_frontmatter;
use crate::types::MemoryType;

const MAX_LEGACY_ENTRIES: usize = 10_000;
const MAX_LEGACY_DEPTH: usize = 64;
const MAX_LEGACY_FILE_BYTES: u64 = 1024 * 1024 + 64 * 1024;

struct LegacySource {
    canonical_path: PathBuf,
    identity: OpenedFileIdentity,
    raw: String,
}

impl MemoryService {
    /// Import legacy Markdown records once without changing or deleting their sources.
    pub fn import_legacy_directory(&self, directory: &Path) -> Result<usize, MemoryServiceError> {
        let sources = collect_legacy_sources(directory)?;
        let mut imported = 0usize;
        let mut skipped = 0usize;
        for source in sources {
            match read_legacy_source(&source) {
                Ok(Some(source)) => {
                    if self.import_legacy_source(&source)? {
                        imported += 1;
                    }
                }
                Ok(None) => skipped += 1,
                Err(_) => skipped += 1,
            }
        }
        if skipped > 0 {
            tracing::warn!(target: "solaris_memory", skipped, "legacy memory files were skipped during bounded import");
        }
        Ok(imported)
    }

    fn import_legacy_source(&self, source: &LegacySource) -> Result<bool, MemoryServiceError> {
        let path_digest = Sha256::digest(source.canonical_path.to_string_lossy().as_bytes()).to_vec();
        let content_digest = Sha256::digest(source.raw.as_bytes()).to_vec();
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| database_error("begin legacy memory import", source))?;
        let completed = transaction
            .query_row(
                "SELECT completed FROM memory_import_sources WHERE path_digest = ?1",
                params![path_digest],
                |row| row.get::<_, i64>(0),
            )
            .optional()
            .map_err(|source| database_error("check legacy memory import", source))?;
        if completed == Some(1) {
            return Ok(false);
        }

        let (frontmatter, content) = parse_frontmatter(&source.raw, None);
        let fallback_name = source
            .canonical_path
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("legacy-memory");
        let name = bounded_text(frontmatter.name.as_deref().unwrap_or(fallback_name), 256);
        let description = bounded_text(frontmatter.description.as_deref().unwrap_or(""), 4 * 1024);
        let memory_type = frontmatter.memory_type.unwrap_or(MemoryType::Reference);
        let scope = match memory_type {
            MemoryType::User | MemoryType::Feedback => MemoryScope::User,
            MemoryType::Project | MemoryType::Reference => MemoryScope::Memory,
        };
        let mutation = MemoryMutation::Create {
            scope,
            memory_type,
            name,
            description,
            content,
        };
        super::validate_mutation(&mutation)?;
        let record = apply_mutation(&transaction, &mutation)?;
        let current = File::open(&source.canonical_path).map_err(|source| MemoryServiceError::Io {
            operation: "reopen legacy memory source",
            source,
        })?;
        let current = OpenedFileIdentity::from_owned_file(current).map_err(|source| MemoryServiceError::Io {
            operation: "identify legacy memory source",
            source,
        })?;
        if !source.identity.same_object(&current) {
            return Ok(false);
        }
        transaction
            .execute(
                "INSERT INTO memory_import_sources
                    (path_digest, content_digest, record_id, completed) VALUES (?1, ?2, ?3, 1)",
                params![path_digest, content_digest, record.id],
            )
            .map_err(|source| database_error("record legacy memory import", source))?;
        transaction
            .commit()
            .map_err(|source| database_error("commit legacy memory import", source))?;
        Ok(true)
    }
}

fn collect_legacy_sources(directory: &Path) -> Result<Vec<PathBuf>, MemoryServiceError> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut stack = vec![(directory.to_path_buf(), 0usize)];
    let mut sources = Vec::new();
    let mut visited = 0usize;
    while let Some((current, depth)) = stack.pop() {
        let entries = std::fs::read_dir(&current).map_err(|source| MemoryServiceError::Io {
            operation: "scan legacy memory directory",
            source,
        })?;
        for entry in entries {
            visited = visited.checked_add(1).ok_or(MemoryServiceError::SnapshotTooLarge)?;
            if visited > MAX_LEGACY_ENTRIES {
                return Err(MemoryServiceError::SnapshotTooLarge);
            }
            let entry = entry.map_err(|source| MemoryServiceError::Io {
                operation: "read legacy memory directory entry",
                source,
            })?;
            let file_type = entry.file_type().map_err(|source| MemoryServiceError::Io {
                operation: "inspect legacy memory entry",
                source,
            })?;
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                if depth < MAX_LEGACY_DEPTH {
                    stack.push((entry.path(), depth + 1));
                }
                continue;
            }
            let path = entry.path();
            if file_type.is_file()
                && path.extension().and_then(|value| value.to_str()) == Some("md")
                && path.file_name().and_then(|value| value.to_str()) != Some(ENTRYPOINT_NAME)
            {
                sources.push(path);
            }
        }
    }
    sources.sort();
    Ok(sources)
}

fn read_legacy_source(path: &Path) -> Result<Option<LegacySource>, MemoryServiceError> {
    let canonical_path = path.canonicalize().map_err(|source| MemoryServiceError::Io {
        operation: "resolve legacy memory source",
        source,
    })?;
    let file = File::open(&canonical_path).map_err(|source| MemoryServiceError::Io {
        operation: "open legacy memory source",
        source,
    })?;
    let before = file.metadata().map_err(|source| MemoryServiceError::Io {
        operation: "inspect legacy memory source",
        source,
    })?;
    if !before.is_file() || before.len() > MAX_LEGACY_FILE_BYTES {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    (&file)
        .take(MAX_LEGACY_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| MemoryServiceError::Io {
            operation: "read legacy memory source",
            source,
        })?;
    if bytes.len() as u64 > MAX_LEGACY_FILE_BYTES {
        return Ok(None);
    }
    let after = file.metadata().map_err(|source| MemoryServiceError::Io {
        operation: "reinspect legacy memory source",
        source,
    })?;
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return Ok(None);
    }
    let Some(raw) = String::from_utf8(bytes).ok() else {
        return Ok(None);
    };
    let identity = OpenedFileIdentity::from_owned_file(file).map_err(|source| MemoryServiceError::Io {
        operation: "identify legacy memory source",
        source,
    })?;
    Ok(Some(LegacySource {
        canonical_path,
        identity,
        raw,
    }))
}

fn bounded_text(value: &str, max_bytes: usize) -> String {
    let mut end = value.len().min(max_bytes);
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].trim().to_owned()
}

#[cfg(test)]
#[path = "service_import_test.rs"]
mod service_import_test;
