use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::identity::RunId;

/// A durable, revisioned plan submitted by an Agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanArtifact {
    pub id: String,
    pub revision: u64,
    pub markdown: String,
    pub digest: String,
    pub run_id: RunId,
    pub msg_id: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// A compact reference suitable for runtime snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanArtifactReference {
    pub id: String,
    pub revision: u64,
    pub digest: String,
    pub run_id: RunId,
    pub msg_id: String,
    pub updated_at_unix_ms: i64,
}

impl PlanArtifact {
    pub fn markdown_digest(markdown: &str) -> String {
        format!("sha256:{:x}", Sha256::digest(markdown.as_bytes()))
    }

    pub fn reference(&self) -> PlanArtifactReference {
        PlanArtifactReference {
            id: self.id.clone(),
            revision: self.revision,
            digest: self.digest.clone(),
            run_id: self.run_id.clone(),
            msg_id: self.msg_id.clone(),
            updated_at_unix_ms: self.updated_at_unix_ms,
        }
    }
}

#[cfg(test)]
#[path = "plan_test.rs"]
mod plan_test;
