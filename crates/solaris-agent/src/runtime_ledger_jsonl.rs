use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde_json::Value;
use solaris_types::effect::DurabilityClass;
use solaris_types::identity::RunId;

use super::{
    LEDGER_SCHEMA_VERSION, LedgerRecord, LogicalAppendCapability, RuntimeLedger, WorkflowMutationLease,
    WorkflowRestoreCommit,
};

pub struct JsonlRuntimeLedger {
    pub(super) path: PathBuf,
    pub(super) state: Mutex<JsonlLedgerState>,
}

pub(super) struct JsonlLedgerState {
    pub(super) writer: Box<dyn JsonlWriter>,
    pub(super) sequences: HashMap<RunId, u64>,
    pub(super) next_sequence: u64,
    pub(super) poisoned: bool,
}

pub(super) trait JsonlWriter: Write + Send {
    fn sync_data(&mut self) -> io::Result<()>;
}

impl JsonlWriter for BufWriter<File> {
    fn sync_data(&mut self) -> io::Result<()> {
        self.get_ref().sync_data()
    }
}

impl JsonlRuntimeLedger {
    pub fn open(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let (sequences, next_sequence) = load_sequences(&path)?;
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            state: Mutex::new(JsonlLedgerState {
                writer: Box::new(BufWriter::new(file)),
                sequences,
                next_sequence,
                poisoned: false,
            }),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl RuntimeLedger for JsonlRuntimeLedger {
    fn logical_append_capability(&self) -> LogicalAppendCapability {
        LogicalAppendCapability::Unsupported
    }

    fn acquire_workflow_mutation_lease(
        &self,
        _run_id: &RunId,
        _owner_id: &str,
        _now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease> {
        Err(unsupported_workflow_mutation_lease())
    }

    fn renew_workflow_mutation_lease(
        &self,
        _lease: &WorkflowMutationLease,
        _now_unix_ms: i64,
    ) -> io::Result<WorkflowMutationLease> {
        Err(unsupported_workflow_mutation_lease())
    }

    fn commit_workflow_restore(
        &self,
        _lease: &WorkflowMutationLease,
        _expected_sequence: u64,
        _now_unix_ms: i64,
    ) -> io::Result<WorkflowRestoreCommit> {
        Err(unsupported_workflow_mutation_lease())
    }

    fn release_workflow_mutation_lease(&self, _lease: &WorkflowMutationLease) -> io::Result<()> {
        Err(unsupported_workflow_mutation_lease())
    }

    fn append(
        &self,
        run_id: &RunId,
        durability: DurabilityClass,
        record_type: &str,
        payload: Value,
    ) -> io::Result<LedgerRecord> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.poisoned {
            return Err(io::Error::other(
                "runtime ledger writer is unavailable after a partial write",
            ));
        }
        let seq = state.next_sequence.saturating_add(1);
        let record = LedgerRecord {
            schema_version: LEDGER_SCHEMA_VERSION,
            seq,
            run_id: run_id.clone(),
            timestamp_unix_ms: chrono::Utc::now().timestamp_millis(),
            durability,
            record_type: record_type.to_owned(),
            payload,
        };
        if durability == DurabilityClass::Ephemeral {
            return Ok(record);
        }
        let encoded = serde_json::to_vec(&record).map_err(io::Error::other)?;
        state.next_sequence = seq;
        let write_result = state
            .writer
            .write_all(&encoded)
            .and_then(|()| state.writer.write_all(b"\n"))
            .and_then(|()| state.writer.flush());
        if let Err(error) = write_result {
            state.poisoned = true;
            return Err(error);
        }
        state.sequences.insert(run_id.clone(), seq);
        if durability == DurabilityClass::SyncCritical {
            state.writer.sync_data()?;
        }
        Ok(record)
    }

    fn append_under_workflow_lease(
        &self,
        _lease: &WorkflowMutationLease,
        _now_unix_ms: i64,
        _durability: DurabilityClass,
        _record_type: &str,
        _payload: Value,
    ) -> io::Result<LedgerRecord> {
        Err(unsupported_workflow_mutation_lease())
    }

    fn compare_and_append(
        &self,
        _run_id: &RunId,
        _durability: DurabilityClass,
        _record_type: &str,
        _identity_fields: &[&str],
        _payload: Value,
    ) -> io::Result<LedgerRecord> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "JSONL runtime ledger does not support atomic logical record append",
        ))
    }

    fn compare_and_append_under_workflow_lease(
        &self,
        _lease: &WorkflowMutationLease,
        _now_unix_ms: i64,
        _durability: DurabilityClass,
        _record_type: &str,
        _identity_fields: &[&str],
        _payload: Value,
    ) -> io::Result<LedgerRecord> {
        Err(unsupported_workflow_mutation_lease())
    }

    fn run_ids(&self) -> io::Result<Vec<RunId>> {
        let mut ids: Vec<_> = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sequences
            .keys()
            .cloned()
            .collect();
        ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Ok(ids)
    }

    fn records_for_run(&self, run_id: &RunId) -> io::Result<Vec<LedgerRecord>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let content = std::fs::read_to_string(&self.path)?;
        content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str::<LedgerRecord>(line).map_err(io::Error::other))
            .filter_map(|result| match result {
                Ok(record) if &record.run_id == run_id => Some(Ok(record)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect()
    }

    fn last_sequence(&self, run_id: &RunId) -> io::Result<u64> {
        Ok(*self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sequences
            .get(run_id)
            .unwrap_or(&0))
    }

    fn protected_state_paths(&self) -> Vec<PathBuf> {
        vec![self.path.clone()]
    }

    fn delete_run_exact(&self, _run_id: &RunId) -> io::Result<usize> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "JSONL runtime ledger does not support Run deletion",
        ))
    }

    fn effect_output_root(&self) -> Option<PathBuf> {
        self.path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(|parent| parent.join("effect-outcomes"))
    }
}

fn unsupported_workflow_mutation_lease() -> io::Error {
    io::Error::new(
        ErrorKind::Unsupported,
        "runtime ledger does not support durable Workflow mutation leases",
    )
}

fn load_sequences(path: &Path) -> io::Result<(HashMap<RunId, u64>, u64)> {
    if !path.exists() {
        return Ok((HashMap::new(), 0));
    }
    let content = std::fs::read(path)?;
    let mut sequences = HashMap::new();
    let mut next_sequence = 0;
    let mut line_start = 0;
    for (index, byte) in content.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        let line = &content[line_start..index];
        line_start = index.saturating_add(1);
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        let record: LedgerRecord = serde_json::from_slice(line).map_err(io::Error::other)?;
        next_sequence = next_sequence.max(record.seq);
        sequences
            .entry(record.run_id)
            .and_modify(|seq: &mut u64| *seq = (*seq).max(record.seq))
            .or_insert(record.seq);
    }
    if line_start != content.len() {
        OpenOptions::new().write(true).open(path)?.set_len(line_start as u64)?;
    }
    Ok((sequences, next_sequence))
}
