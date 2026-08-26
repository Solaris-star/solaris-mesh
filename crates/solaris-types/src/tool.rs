use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::sandbox::SandboxReport;

/// Schema for a tool parameter, in JSON Schema format
pub type JsonSchema = Value;

/// Maximum chars kept from a deferred tool's description.
const DEFERRED_DESC_MAX_CHARS: usize = 200;

/// Truncate a description for a deferred tool stub.
///
/// Keeps up to the first blank line or `DEFERRED_DESC_MAX_CHARS` characters
/// (whichever is shorter). If the text was trimmed, an ellipsis is appended.
pub fn truncate_deferred_description(desc: &str) -> String {
    // Find first blank line (double newline)
    let end_at_blank = desc.find("\n\n").unwrap_or(desc.len());
    let limit = end_at_blank.min(DEFERRED_DESC_MAX_CHARS);

    if limit >= desc.len() {
        return desc.to_string();
    }

    // Avoid cutting in the middle of a UTF-8 char boundary
    let safe_end = desc
        .char_indices()
        .take_while(|(i, _)| *i < limit)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0);

    format!("{}…", &desc[..safe_end])
}

/// Definition of a tool for the API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolDef {
    pub name: String,
    pub description: String,
    pub input_schema: JsonSchema,
    /// Whether this tool's full schema is deferred (only name + stub sent to LLM).
    pub deferred: bool,
}

/// Result from executing a tool
#[derive(Debug, Clone, Deserialize)]
pub struct ToolResult {
    pub content: String,
    pub is_error: bool,
}

impl Serialize for ToolResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ToolResult", 3)?;
        state.serialize_field("content", &self.content)?;
        state.serialize_field("is_error", &self.is_error)?;
        state.serialize_field("status", &self.inferred_status())?;
        state.end()
    }
}

impl ToolResult {
    /// Classify a legacy binary result without losing its content.
    pub fn classified(self, status: ToolResultStatus) -> ClassifiedToolResult {
        ClassifiedToolResult::new(self.content, status)
    }

    /// Infer the only status available in the legacy binary result shape.
    pub fn inferred_status(&self) -> ToolResultStatus {
        ToolResultStatus::from_legacy_is_error(self.is_error)
    }
}

/// Terminal status of one model-visible tool call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    #[serde(alias = "success")]
    Executed,
    CacheHit,
    Noop,
    Denied,
    #[serde(alias = "error")]
    Failed,
    Aborted,
    Timeout,
    OutcomeUnknown,
}

/// Structured, non-sensitive diagnostics attached to one terminal tool result.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolResultMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_report: Option<SandboxReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub collaboration_summary: Option<crate::workflow::CollaborationRunSummary>,
}

impl ToolResultMetadata {
    pub const fn sandbox_report(report: SandboxReport) -> Self {
        Self {
            sandbox_report: Some(report),
            collaboration_summary: None,
        }
    }

    pub fn collaboration_summary(summary: crate::workflow::CollaborationRunSummary) -> Self {
        Self {
            sandbox_report: None,
            collaboration_summary: Some(summary),
        }
    }

    pub const fn is_empty(&self) -> bool {
        self.sandbox_report.is_none() && self.collaboration_summary.is_none()
    }
}

impl ToolResultStatus {
    /// Legacy source-compatible spelling. New code should use `Executed`.
    #[allow(non_upper_case_globals)]
    pub const Success: Self = Self::Executed;

    /// Legacy source-compatible spelling. New code should use `Failed`.
    #[allow(non_upper_case_globals)]
    pub const Error: Self = Self::Failed;

    pub const fn from_legacy_is_error(is_error: bool) -> Self {
        if is_error { Self::Failed } else { Self::Executed }
    }

    /// Whether providers should receive this result as an error.
    pub const fn is_error(self) -> bool {
        matches!(
            self,
            Self::Denied | Self::Failed | Self::Aborted | Self::Timeout | Self::OutcomeUnknown
        )
    }

    /// A useful call either performed the operation or returned a valid cache result.
    ///
    /// A cache hit remains distinct from execution, while no-ops and all
    /// unsuccessful terminal states are excluded.
    pub const fn is_useful_call(self) -> bool {
        matches!(self, Self::Executed | Self::CacheHit)
    }
}

/// Source-compatible migration wrapper for tool results with an explicit status.
///
/// `is_error` remains on the wire for old readers, but is always derived from
/// `status`. New execution paths should carry this type instead of inferring a
/// multi-state outcome from the legacy boolean.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ClassifiedToolResult {
    pub content: String,
    pub is_error: bool,
    pub status: ToolResultStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ToolResultMetadata>,
}

impl ClassifiedToolResult {
    pub fn new(content: impl Into<String>, status: ToolResultStatus) -> Self {
        Self {
            content: content.into(),
            is_error: status.is_error(),
            status,
            metadata: None,
        }
    }

    /// Attaches structured, non-sensitive execution diagnostics for Hosts.
    pub fn with_metadata(mut self, metadata: ToolResultMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn into_legacy(self) -> ToolResult {
        ToolResult {
            content: self.content,
            is_error: self.is_error,
        }
    }
}

impl From<ToolResult> for ClassifiedToolResult {
    fn from(result: ToolResult) -> Self {
        let status = result.inferred_status();
        Self::new(result.content, status)
    }
}

impl From<ClassifiedToolResult> for ToolResult {
    fn from(result: ClassifiedToolResult) -> Self {
        result.into_legacy()
    }
}

impl<'de> Deserialize<'de> for ClassifiedToolResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct WireResult {
            content: String,
            #[serde(default)]
            is_error: Option<bool>,
            #[serde(default)]
            status: Option<ToolResultStatus>,
            #[serde(default)]
            metadata: Option<ToolResultMetadata>,
        }

        let wire = WireResult::deserialize(deserializer)?;
        let status = wire
            .status
            .or_else(|| wire.is_error.map(ToolResultStatus::from_legacy_is_error))
            .ok_or_else(|| serde::de::Error::missing_field("status"))?;
        let mut result = Self::new(wire.content, status);
        result.metadata = wire.metadata;
        Ok(result)
    }
}

/// Fraction of all observed tool calls that executed or returned a valid cache result.
///
/// The denominator includes every terminal status. An empty sample returns
/// `None` rather than reporting a misleading zero-percent rate.
pub fn useful_call_rate(statuses: &[ToolResultStatus]) -> Option<f64> {
    if statuses.is_empty() {
        return None;
    }
    let useful = statuses.iter().filter(|status| status.is_useful_call()).count();
    Some(useful as f64 / statuses.len() as f64)
}

/// One terminal tool call observed within a statistics scope.
///
/// The fingerprint identifies the logical call (stable tool name plus
/// canonicalized JSON input plus the required task/environment scope) and is
/// independent of object key order. The status is the terminal outcome used
/// to classify the call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCallStat {
    pub fingerprint: String,
    pub status: ToolResultStatus,
}

impl ToolCallStat {
    /// Build one stat by fingerprinting the call within a statistics scope.
    pub fn new(scope: &str, tool_name: &str, input: &Value, status: ToolResultStatus) -> Self {
        Self {
            fingerprint: tool_call_fingerprint(scope, tool_name, input),
            status,
        }
    }
}

/// Fraction of observed tool calls whose fingerprint repeats an earlier call
/// in the same statistics scope.
///
/// The denominator includes every terminal status (`Executed`, `CacheHit`,
/// `Noop`, `Denied`, `Failed`, `Aborted`, `Timeout`, `OutcomeUnknown`); a
/// malformed call is recorded as `Failed` and participates like any other
/// failed call. The first occurrence of a fingerprint is never a duplicate;
/// every later occurrence of the same fingerprint is. An empty sample returns
/// `None` rather than reporting a misleading zero-percent rate.
pub fn duplicate_call_rate(stats: &[ToolCallStat]) -> Option<f64> {
    if stats.is_empty() {
        return None;
    }
    let mut seen = std::collections::HashSet::new();
    let mut duplicates = 0usize;
    for stat in stats {
        if !seen.insert(stat.fingerprint.as_str()) {
            duplicates += 1;
        }
    }
    Some(duplicates as f64 / stats.len() as f64)
}

/// Stable fingerprint for one tool call within a statistics scope.
///
/// Combines the statistics scope (task/environment), the stable tool name, and
/// the canonicalized JSON input. Object key order in the input does not affect
/// the fingerprint, so two calls that differ only in key ordering produce the
/// same fingerprint and are treated as the same logical call.
pub fn tool_call_fingerprint(scope: &str, tool_name: &str, input: &Value) -> String {
    let canonical = canonicalize_json_value(input);
    let mut hasher = Sha256::new();
    hasher.update(b"solaris/tool-call-fingerprint/v1\0");
    hasher.update(scope.as_bytes());
    hasher.update(b"\0");
    hasher.update(tool_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(serde_json::to_vec(&canonical).unwrap_or_default());
    format!("{:x}", hasher.finalize())
}

/// Canonicalize object insertion order before hashing.
///
/// `serde_json` can be built with its `preserve_order` feature. That feature
/// is useful for display, but it must not change durable identities: a value
/// loaded from a provider response and the same value reconstructed from a
/// ledger record may otherwise hash differently solely because their object
/// keys were inserted in a different order.
fn canonicalize_json_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort_unstable();
            let mut canonical = serde_json::Map::with_capacity(object.len());
            for key in keys {
                canonical.insert(key.clone(), canonicalize_json_value(&object[key]));
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json_value).collect()),
        _ => value.clone(),
    }
}

#[cfg(test)]
#[path = "tool_test.rs"]
mod tool_test;
