use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use lru::LruCache;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use solaris_config::file_identity::{OpenedFileIdentity, OpenedFileState};
use solaris_process::ProtectedObjectIdentity;
use solaris_types::permission::PermissionMode;
use solaris_types::tool::{ClassifiedToolResult, ToolResultStatus};

use crate::tool::ReadOnlyEvidenceScope;

const DEFAULT_MAX_ENTRIES: usize = 256;
const DEFAULT_MAX_SIZE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct EvidenceKey {
    tool_name: String,
    input_digest: String,
    permission_mode: u8,
    authorization_digest: String,
    environment_digest: String,
}

#[derive(Clone, Debug)]
struct EvidenceObject {
    label: String,
    identity: ProtectedObjectIdentity,
    state: OpenedFileState,
    // Keeping the original handle alive prevents Windows file IDs from being
    // recycled while this evidence entry remains reusable.
    _retained_handle: Arc<OpenedFileIdentity>,
}

impl PartialEq for EvidenceObject {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label && self.identity == other.identity && self.state == other.state
    }
}

impl Eq for EvidenceObject {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EvidenceValidation {
    traversal_digest: String,
    objects: Vec<EvidenceObject>,
}

impl EvidenceValidation {
    pub(crate) fn from_opened_object(label: impl Into<String>, identity: Arc<OpenedFileIdentity>) -> Option<Self> {
        Self::from_opened_objects([(label.into(), identity)], "single-file".to_owned())
    }

    pub(crate) fn from_opened_objects(
        objects: impl IntoIterator<Item = (String, Arc<OpenedFileIdentity>)>,
        traversal_digest: String,
    ) -> Option<Self> {
        let mut objects = objects
            .into_iter()
            .map(|(label, identity)| {
                Some(EvidenceObject {
                    label,
                    identity: identity.protected_object_identity()?,
                    state: identity.current_state().ok()?,
                    _retained_handle: identity,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        objects.sort_by(|left, right| left.label.cmp(&right.label));
        Some(Self {
            traversal_digest,
            objects,
        })
    }
}

#[derive(Clone)]
struct EvidenceEntry {
    validation: EvidenceValidation,
    result_digest: String,
    content: String,
}

struct EvidenceStore {
    entries: LruCache<EvidenceKey, EvidenceEntry>,
    current_size_bytes: usize,
}

/// Run-local index for verified Read, Glob, and Grep results.
///
/// Entries are reusable only after the caller has reopened and revalidated the
/// same filesystem objects. Validation uses the OS object identity and change
/// timestamp, so a cache lookup does not need to read file contents first.
pub struct ReadOnlyEvidenceIndex {
    store: Mutex<EvidenceStore>,
    max_size_bytes: usize,
}

impl Default for ReadOnlyEvidenceIndex {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MAX_ENTRIES)
    }
}

impl ReadOnlyEvidenceIndex {
    pub fn with_capacity(max_entries: usize) -> Self {
        Self::with_limits(max_entries, DEFAULT_MAX_SIZE_BYTES)
    }

    pub(crate) fn with_limits(max_entries: usize, max_size_bytes: usize) -> Self {
        let capacity = NonZeroUsize::new(max_entries.max(1)).expect("evidence index capacity is non-zero");
        Self {
            store: Mutex::new(EvidenceStore {
                entries: LruCache::new(capacity),
                current_size_bytes: 0,
            }),
            max_size_bytes,
        }
    }

    pub(crate) fn lookup(
        &self,
        tool_name: &str,
        input: &Value,
        permission_mode: PermissionMode,
        scope: &ReadOnlyEvidenceScope,
        validation: &EvidenceValidation,
    ) -> Option<ClassifiedToolResult> {
        let key = evidence_key(tool_name, input, permission_mode, scope);
        let mut store = self.store.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(entry) = store.entries.get(&key)
            && entry.validation == *validation
            && entry.result_digest == bytes_digest(entry.content.as_bytes())
        {
            return Some(ClassifiedToolResult::new(
                entry.content.clone(),
                ToolResultStatus::CacheHit,
            ));
        }
        None
    }

    pub(crate) fn insert(
        &self,
        tool_name: &str,
        input: &Value,
        permission_mode: PermissionMode,
        scope: &ReadOnlyEvidenceScope,
        validation: EvidenceValidation,
        result: ClassifiedToolResult,
    ) -> ClassifiedToolResult {
        if result.status != ToolResultStatus::Executed {
            return result;
        }
        let new_size = result.content.len();
        if new_size > self.max_size_bytes {
            return result;
        }
        let key = evidence_key(tool_name, input, permission_mode, scope);
        let result_digest = bytes_digest(result.content.as_bytes());
        let mut store = self.store.lock().unwrap_or_else(|error| error.into_inner());

        if let Some(previous) = store.entries.pop(&key) {
            store.current_size_bytes = store.current_size_bytes.saturating_sub(previous.content.len());
        }
        while store.current_size_bytes.saturating_add(new_size) > self.max_size_bytes && !store.entries.is_empty() {
            if let Some((_key, entry)) = store.entries.pop_lru() {
                store.current_size_bytes = store.current_size_bytes.saturating_sub(entry.content.len());
            }
        }
        if let Some((_key, entry)) = store.entries.push(
            key,
            EvidenceEntry {
                validation,
                result_digest,
                content: result.content.clone(),
            },
        ) {
            store.current_size_bytes = store.current_size_bytes.saturating_sub(entry.content.len());
        }
        store.current_size_bytes += new_size;
        result
    }
}

fn evidence_key(
    tool_name: &str,
    input: &Value,
    permission_mode: PermissionMode,
    scope: &ReadOnlyEvidenceScope,
) -> EvidenceKey {
    EvidenceKey {
        tool_name: tool_name.to_owned(),
        input_digest: canonical_json_digest(&normalized_tool_input(tool_name, input)),
        permission_mode: match permission_mode {
            PermissionMode::Plan => 0,
            PermissionMode::Auto => 1,
            PermissionMode::Bypass => 2,
        },
        authorization_digest: scope.authorization_digest.clone(),
        environment_digest: scope.environment_digest.clone(),
    }
}

fn normalized_tool_input(tool_name: &str, input: &Value) -> Value {
    let mut normalized = Map::new();
    match tool_name {
        "Read" => {
            copy_string(input, &mut normalized, "file_path");
            normalized.insert(
                "offset".to_owned(),
                Value::from(input.get("offset").and_then(Value::as_u64).unwrap_or(0)),
            );
            normalized.insert(
                "limit".to_owned(),
                input
                    .get("limit")
                    .and_then(Value::as_u64)
                    .map(Value::from)
                    .unwrap_or(Value::Null),
            );
            normalized.insert(
                "force".to_owned(),
                Value::Bool(input.get("force").and_then(Value::as_bool).unwrap_or(false)),
            );
        }
        "Glob" => {
            copy_string(input, &mut normalized, "pattern");
            normalized.insert(
                "path".to_owned(),
                Value::String(input.get("path").and_then(Value::as_str).unwrap_or(".").to_owned()),
            );
        }
        "Grep" => {
            copy_string(input, &mut normalized, "pattern");
            normalized.insert(
                "path".to_owned(),
                Value::String(input.get("path").and_then(Value::as_str).unwrap_or(".").to_owned()),
            );
            normalized.insert(
                "glob".to_owned(),
                input
                    .get("glob")
                    .and_then(Value::as_str)
                    .map(|value| Value::String(value.to_owned()))
                    .unwrap_or(Value::Null),
            );
            normalized.insert(
                "case_insensitive".to_owned(),
                Value::Bool(input.get("case_insensitive").and_then(Value::as_bool).unwrap_or(false)),
            );
        }
        _ => return canonical_json(input),
    }
    Value::Object(normalized)
}

fn copy_string(input: &Value, output: &mut Map<String, Value>, field: &str) {
    output.insert(
        field.to_owned(),
        input
            .get(field)
            .and_then(Value::as_str)
            .map(|value| Value::String(value.to_owned()))
            .unwrap_or(Value::Null),
    );
}

pub(crate) fn evidence_digest(parts: impl IntoIterator<Item = impl AsRef<[u8]>>) -> String {
    let mut digest = Sha256::new();
    for part in parts {
        let bytes = part.as_ref();
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(bytes);
    }
    format!("{:x}", digest.finalize())
}

fn canonical_json_digest(value: &Value) -> String {
    let canonical = canonical_json(value);
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    bytes_digest(&bytes)
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json).collect()),
        Value::Object(values) => {
            let sorted: BTreeMap<_, _> = values
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        value => value.clone(),
    }
}

fn bytes_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
