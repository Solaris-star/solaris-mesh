use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;
use solaris_types::config::RuntimeConfigUpdate;
use solaris_types::message::Role;
use solaris_types::run_preset::Intensity;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HistoryMessage {
    pub role: Role,
    pub content: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct DeliveryAcknowledgement {
    pub session_id: String,
    pub run_epoch: u64,
    pub delivery_id: String,
    pub digest: String,
}

pub use solaris_types::permission::PermissionMode as SessionMode;

/// Commands sent from the client to the agent (Client -> Agent)
#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum ProtocolCommand {
    Message {
        msg_id: String,
        content: String,
        #[serde(default)]
        files: Vec<String>,
    },
    Cancel {
        #[serde(default)]
        request_id: Option<String>,
        msg_id: String,
    },
    CancelWorkflow {
        #[serde(default)]
        request_id: Option<String>,
        run_id: String,
    },
    Stop,
    ToolApprove {
        #[serde(default)]
        request_id: Option<String>,
        call_id: String,
        #[serde(default)]
        scope: ApprovalScope,
    },
    ToolDeny {
        #[serde(default)]
        request_id: Option<String>,
        call_id: String,
        #[serde(default)]
        reason: String,
    },
    InitHistory {
        #[serde(default)]
        messages: Vec<HistoryMessage>,
        #[serde(default)]
        text: Option<String>,
    },
    SetMode {
        #[serde(default)]
        request_id: Option<String>,
        mode: SessionMode,
    },
    SetIntensity {
        #[serde(default)]
        request_id: Option<String>,
        intensity: Intensity,
    },
    SetConfig {
        #[serde(default)]
        request_id: Option<String>,
        #[serde(flatten)]
        update: RuntimeConfigUpdate,
    },
    AddMcpServer {
        name: String,
        transport: String,
        #[serde(default)]
        command: Option<String>,
        #[serde(default)]
        args: Option<Vec<String>>,
        #[serde(default)]
        env: Option<HashMap<String, String>>,
        #[serde(default)]
        url: Option<String>,
        #[serde(default)]
        headers: Option<HashMap<String, String>>,
        #[serde(default)]
        network: solaris_types::permission::ProcessNetworkConfig,
    },
    HostContextReady,
    InstallPlugin {
        request_id: String,
        manifest_path: String,
    },
    ActivatePlugin {
        request_id: String,
        plugin_id: String,
    },
    DeactivatePlugin {
        request_id: String,
        plugin_id: String,
    },
    GetRuntimeJournal {
        request_id: String,
        #[serde(default)]
        run_id: Option<String>,
        #[serde(default)]
        after_sequence: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    GetRuntimeSnapshot {
        request_id: String,
    },
    GetPlanArtifacts {
        request_id: String,
        #[serde(default)]
        run_id: Option<String>,
    },
    RunWorkflow {
        request_id: String,
        workflow: String,
        #[serde(default)]
        parameters: Value,
    },
    AcknowledgeDelivery(DeliveryAcknowledgement),
    Ping,
}

#[derive(Debug, Clone, Copy, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalScope {
    #[default]
    Once,
    Always,
}

#[cfg(test)]
#[path = "commands_test.rs"]
mod commands_test;
