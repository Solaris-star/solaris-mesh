use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::workflow::{CollaborationSelection, MultiAgentPolicy, WorkflowRequirement};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Intensity {
    Low,
    Medium,
    #[default]
    High,
    XHigh,
    Extra,
    Ultracode,
}

impl Intensity {
    pub const USER_LEVELS: [&'static str; 6] = ["low", "medium", "high", "xhigh", "extra", "ultracode"];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
            Self::Extra => "extra",
            Self::Ultracode => "ultracode",
        }
    }
}

impl fmt::Display for Intensity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Intensity {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
            "low" => Ok(Self::Low),
            "medium" | "med" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            "xhigh" | "x_high" | "extra_high" => Ok(Self::XHigh),
            "extra" | "max" => Ok(Self::Extra),
            "ultracode" | "ultra_code" | "ultra" => Ok(Self::Ultracode),
            other => Err(format!(
                "unknown intensity '{other}'; expected one of {}",
                Self::USER_LEVELS.join(", ")
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPreset {
    pub intensity: Intensity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    pub workflow_requirement: WorkflowRequirement,
    pub multi_agent_policy: MultiAgentPolicy,
    pub collaboration: CollaborationSelection,
}
