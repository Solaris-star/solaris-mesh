use serde::{Deserialize, Serialize};

use solaris_types::skill_types::ContextModifier;
use solaris_types::tool::{ToolResultMetadata, ToolResultStatus};

const TOOL_OUTCOME_SCHEMA_V1: &str = "solaris/durable-tool-outcome/v1";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DurableToolOutcome {
    schema: String,
    pub(super) content: String,
    pub(super) is_error: bool,
    pub(super) status: ToolResultStatus,
    pub(super) modifier: Option<ContextModifier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) metadata: Option<ToolResultMetadata>,
    #[serde(skip)]
    legacy: bool,
}

impl DurableToolOutcome {
    pub(super) fn new(
        content: String,
        is_error: bool,
        status: ToolResultStatus,
        modifier: Option<ContextModifier>,
        metadata: Option<ToolResultMetadata>,
    ) -> Self {
        Self {
            schema: TOOL_OUTCOME_SCHEMA_V1.to_owned(),
            content,
            is_error,
            status,
            modifier,
            metadata,
            legacy: false,
        }
    }

    pub(super) fn encode(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub(super) fn decode(output: String, legacy_is_error: bool) -> Result<Self, String> {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&output) else {
            return Ok(Self::legacy(output, legacy_is_error));
        };
        if value.get("schema").and_then(serde_json::Value::as_str) != Some(TOOL_OUTCOME_SCHEMA_V1) {
            return Ok(Self::legacy(output, legacy_is_error));
        }
        let outcome: Self =
            serde_json::from_value(value).map_err(|error| format!("durable tool outcome is invalid: {error}"))?;
        if outcome.is_error != legacy_is_error || outcome.status.is_error() != outcome.is_error {
            return Err("durable tool outcome status conflicts with its effect record".to_owned());
        }
        Ok(outcome)
    }

    fn legacy(content: String, is_error: bool) -> Self {
        Self {
            schema: TOOL_OUTCOME_SCHEMA_V1.to_owned(),
            content,
            is_error,
            status: ToolResultStatus::from_legacy_is_error(is_error),
            modifier: None,
            metadata: None,
            legacy: true,
        }
    }

    pub(super) fn is_legacy(&self) -> bool {
        self.legacy
    }

    pub(super) fn replay_status(&self) -> ToolResultStatus {
        if self.is_error {
            self.status
        } else {
            ToolResultStatus::CacheHit
        }
    }
}

#[cfg(test)]
#[path = "durable_tool_outcome_test.rs"]
mod durable_tool_outcome_test;
