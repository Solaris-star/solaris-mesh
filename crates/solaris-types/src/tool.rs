use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

#[cfg(test)]
#[path = "tool_test.rs"]
mod tool_test;
