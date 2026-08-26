use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::{LedgerRecord, RuntimeLedger};

#[derive(Default)]
pub struct RunMutationCoordinator {
    lines: Mutex<HashMap<RunId, Arc<Mutex<()>>>>,
}

impl RunMutationCoordinator {
    pub fn line_for(&self, run_id: &RunId) -> Arc<Mutex<()>> {
        let mutation_root = run_id
            .as_str()
            .split_once(":workflow:")
            .map(|(root, _)| RunId::from(root))
            .unwrap_or_else(|| run_id.clone());
        self.lines
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(mutation_root)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    pub fn append_serialized(
        &self,
        ledger: &dyn RuntimeLedger,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        let line = self.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        ledger.append(run_id, durability, record_type, payload)
    }

    pub fn compare_and_append_serialized(
        &self,
        ledger: &dyn RuntimeLedger,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        identity_fields: &[&str],
        payload: Value,
    ) -> std::io::Result<LedgerRecord> {
        let line = self.line_for(run_id);
        let _guard = line.lock().unwrap_or_else(|error| error.into_inner());
        ledger.compare_and_append(run_id, durability, record_type, identity_fields, payload)
    }
}
