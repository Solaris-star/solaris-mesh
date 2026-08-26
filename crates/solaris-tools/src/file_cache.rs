use std::num::NonZeroUsize;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use lru::LruCache;
use sha2::{Digest, Sha256};

use solaris_config::file_cache::FileCacheConfig;
use solaris_config::file_identity::OpenedFileIdentity;
use solaris_types::file_state::FileState;

struct CachedFileState {
    state: FileState,
    identity: Option<Arc<OpenedFileIdentity>>,
    content_digest: Option<[u8; 32]>,
}

/// LRU cache for file states seen by the model.
///
/// Provides dual eviction: entry-count limit (via LRU) and byte-size limit
/// (manually tracked). All path keys are normalized before access so that
/// `"/a/../b"` and `"/b"` map to the same cache slot.
///
/// Thread safety: wrap in `Arc<std::sync::RwLock<FileStateCache>>` when
/// sharing across tools. Cache operations are brief (hash lookup + insert),
/// so `std::sync::RwLock` is preferred over `tokio::sync::RwLock`.
pub struct FileStateCache {
    entries: LruCache<PathBuf, CachedFileState>,
    max_size_bytes: usize,
    current_size_bytes: usize,
}

impl FileStateCache {
    /// Create a new cache from configuration.
    ///
    /// If `max_entries` is 0, defaults to 100.
    pub fn new(config: &FileCacheConfig) -> Self {
        let cap = NonZeroUsize::new(config.max_entries).unwrap_or(NonZeroUsize::new(100).expect("100 is non-zero"));
        Self {
            entries: LruCache::new(cap),
            max_size_bytes: config.max_size_bytes,
            current_size_bytes: 0,
        }
    }

    /// Look up a file state, promoting it to most-recently-used.
    pub fn get(&mut self, path: &Path) -> Option<&FileState> {
        let normalized = normalize_path(path);
        self.entries.get(&normalized).map(|entry| &entry.state)
    }

    /// Insert or update a file state entry.
    ///
    /// Evicts least-recently-used entries when the byte-size limit or
    /// entry-count limit would be exceeded.
    pub fn insert(&mut self, path: PathBuf, state: FileState) {
        self.insert_entry(
            path,
            CachedFileState {
                state,
                identity: None,
                content_digest: None,
            },
        );
    }

    pub(crate) fn insert_opened(
        &mut self,
        path: PathBuf,
        state: FileState,
        identity: Arc<OpenedFileIdentity>,
        content_digest: [u8; 32],
    ) {
        self.insert_entry(
            path,
            CachedFileState {
                state,
                identity: Some(identity),
                content_digest: Some(content_digest),
            },
        );
    }

    /// Returns `None` when no cached read exists, or whether the cached read
    /// refers to the same opened object, range, and content.
    pub(crate) fn matches_opened(
        &mut self,
        path: &Path,
        identity: &OpenedFileIdentity,
        content_digest: &[u8; 32],
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Option<bool> {
        let normalized = normalize_path(path);
        let entry = self.entries.get(&normalized)?;
        Some(
            entry.state.offset == offset
                && entry.state.limit == limit
                && entry
                    .identity
                    .as_deref()
                    .is_some_and(|cached| cached.same_object(identity))
                && entry.content_digest.as_ref() == Some(content_digest),
        )
    }

    fn insert_entry(&mut self, path: PathBuf, entry: CachedFileState) {
        let normalized = normalize_path(&path);
        let new_size = entry.state.content_bytes();

        // Remove existing entry for this key first (simplifies size accounting).
        if let Some(old) = self.entries.pop(&normalized) {
            self.current_size_bytes = self.current_size_bytes.saturating_sub(old.state.content_bytes());
        }

        // An entry that cannot fit by itself must not evict the rest of the
        // cache or leave a stale value for the same path behind.
        if new_size > self.max_size_bytes {
            return;
        }

        // Evict LRU entries until byte-size budget is available.
        while self.current_size_bytes.saturating_add(new_size) > self.max_size_bytes && !self.entries.is_empty() {
            if let Some((_k, v)) = self.entries.pop_lru() {
                self.current_size_bytes = self.current_size_bytes.saturating_sub(v.state.content_bytes());
            }
        }

        // push() returns evicted (key, value) if entry-count capacity is reached.
        if let Some((_evicted_key, evicted_val)) = self.entries.push(normalized, entry) {
            self.current_size_bytes = self
                .current_size_bytes
                .saturating_sub(evicted_val.state.content_bytes());
        }
        self.current_size_bytes += new_size;
    }

    /// Remove a specific entry by path.
    pub fn remove(&mut self, path: &Path) -> Option<FileState> {
        let normalized = normalize_path(path);
        let removed = self.entries.pop(&normalized);
        if let Some(ref v) = removed {
            self.current_size_bytes = self.current_size_bytes.saturating_sub(v.state.content_bytes());
        }
        removed.map(|entry| entry.state)
    }

    /// Remove all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.current_size_bytes = 0;
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Current total byte size of all cached content.
    pub fn current_size_bytes(&self) -> usize {
        self.current_size_bytes
    }
}

/// Update the cache after a successful file write (Edit or Write).
///
/// Opens the written object, retains its identity, and stores a digest of the
/// line-numbered content. The legacy millisecond timestamp remains in the
/// public projection but is not used to authorize Edit or deduplicate Read.
pub fn update_cache_after_write(cache_arc: &Arc<std::sync::RwLock<FileStateCache>>, path: &Path, content: &str) {
    let opened = std::fs::File::open(path).and_then(|file| {
        let metadata = file.metadata()?;
        let identity = OpenedFileIdentity::from_owned_file(file)?;
        Ok((Arc::new(identity), metadata.modified()?))
    });
    let Ok((identity, modified)) = opened else {
        if let Ok(mut cache) = cache_arc.write() {
            cache.remove(path);
        }
        return;
    };
    update_cache_after_verified_write(cache_arc, path, content, identity, modified);
}

pub(crate) fn update_cache_after_verified_write(
    cache_arc: &Arc<std::sync::RwLock<FileStateCache>>,
    path: &Path,
    content: &str,
    identity: Arc<OpenedFileIdentity>,
    modified: std::time::SystemTime,
) {
    let Ok(mut cache) = cache_arc.write() else {
        return;
    };
    let mtime_ms = modified
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or(0);
    let numbered = numbered_content(content);
    cache.insert_opened(
        path.to_path_buf(),
        FileState {
            content: numbered.clone(),
            mtime_ms,
            offset: None,
            limit: None,
        },
        identity,
        content_digest(numbered.as_bytes()),
    );
}

pub(crate) fn content_digest(content: &[u8]) -> [u8; 32] {
    Sha256::digest(content).into()
}

pub(crate) fn numbered_content(content: &str) -> String {
    content
        .lines()
        .enumerate()
        .map(|(index, line)| format!("{:>6}\t{}", index + 1, line))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Get file modification time as milliseconds since UNIX epoch.
///
/// Returns `None` if the file does not exist or metadata is unavailable.
pub fn file_mtime_ms(path: &Path) -> Option<u64> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(duration.as_millis() as u64)
}

/// Normalize a path by resolving `.` and `..` components without filesystem access.
///
/// Unlike `std::fs::canonicalize`, this does not require the path to exist on disk,
/// which is important because cache lookups can happen before the file is created.
///
/// Examples:
/// - `/a/../b/file` -> `/b/file`
/// - `a/./b/../c`   -> `a/c`
/// - `/../b`        -> `/b` (can't go above root)
fn normalize_path(path: &Path) -> PathBuf {
    let mut components: Vec<Component> = Vec::new();
    for component in path.components() {
        match component {
            Component::ParentDir => match components.last() {
                Some(Component::Normal(_)) => {
                    components.pop();
                }
                Some(Component::RootDir) => {
                    // Can't go above filesystem root; ignore the `..`
                }
                _ => {
                    // Preserve leading `..` in relative paths (e.g. `../../foo`)
                    components.push(component);
                }
            },
            Component::CurDir => {} // skip `.`
            other => components.push(other),
        }
    }
    let mut result = PathBuf::new();
    for c in &components {
        result.push(c);
    }
    result
}

#[cfg(test)]
#[path = "file_cache_test.rs"]
mod file_cache_test;
