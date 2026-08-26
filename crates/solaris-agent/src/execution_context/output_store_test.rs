use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use solaris_types::identity::RunId;

use super::*;
use crate::runtime_ledger::{LedgerRecord, RuntimeLedger, SqliteRuntimeLedger};

struct OutputRootLedger(PathBuf);

impl RuntimeLedger for OutputRootLedger {
    crate::runtime_ledger::unsupported_compare_and_append!();

    fn append(
        &self,
        _: &RunId,
        _: solaris_types::effect::DurabilityClass,
        _: &str,
        _: serde_json::Value,
    ) -> std::io::Result<LedgerRecord> {
        unreachable!("output-root test ledger does not append")
    }

    fn run_ids(&self) -> std::io::Result<Vec<RunId>> {
        Ok(Vec::new())
    }

    fn records_for_run(&self, _: &RunId) -> std::io::Result<Vec<LedgerRecord>> {
        Ok(Vec::new())
    }

    fn effect_output_root(&self) -> Option<PathBuf> {
        Some(self.0.clone())
    }
}

fn test_store(root: PathBuf, legacy_root: Option<PathBuf>) -> EffectOutputStore {
    EffectOutputStore {
        root: Some(root),
        legacy_root,
    }
}

#[test]
fn new_write_requires_a_ledger_local_root() {
    let store = EffectOutputStore {
        root: None,
        legacy_root: None,
    };

    let error = store.write_named("effect", "must-not-be-global").unwrap_err();

    assert_eq!(error.kind(), ErrorKind::Unsupported);
}

#[test]
fn new_write_rejects_a_ledger_that_points_at_the_legacy_global_root() {
    let run_id = RunId::from(format!("legacy-write-rejected-{}", uuid::Uuid::now_v7()));
    let run_root = legacy_run_root(&run_id);
    let ledger = OutputRootLedger(effect_output_state_root());
    let store = EffectOutputStore::for_run_with_ledger(&run_id, &ledger);

    let error = store.write_named("effect", "must-stay-local").unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(!run_root.exists());
}

#[cfg(any(windows, target_os = "macos"))]
#[test]
fn missing_directory_aliases_are_compared_case_insensitively() {
    let directory = tempfile::tempdir().unwrap();
    let suffix = format!("missing-legacy-alias-{}", uuid::Uuid::now_v7());
    let upper = directory.path().join(suffix.to_uppercase());
    let lower = directory.path().join(suffix.to_lowercase());

    assert!(!upper.exists());
    assert!(!lower.exists());
    assert!(secure_directory::paths_refer_to_same_directory(&upper, &lower).unwrap());
}

#[test]
fn output_reference_is_the_content_sha256_name() {
    let directory = tempfile::tempdir().unwrap();
    let store = test_store(directory.path().join("run"), None);
    let digest = stable_digest_bytes(b"shared-output");

    let first = store.write_named("first-effect", "shared-output").unwrap();
    let second = store.write_named("second-effect", "shared-output").unwrap();

    assert_eq!(first, format!("sha256-{digest}.blob"));
    assert_eq!(second, first);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("run").join(first)).unwrap(),
        "shared-output"
    );
}

#[test]
fn successful_new_write_syncs_file_then_renames_then_syncs_parent() {
    let directory = tempfile::tempdir().unwrap();
    let store = test_store(directory.path().join("run"), None);
    let steps = Mutex::new(Vec::new());

    store
        .write_named_with_observer("durable-output", |step| steps.lock().unwrap().push(step))
        .unwrap();

    assert_eq!(
        *steps.lock().unwrap(),
        vec![
            secure_directory::WriteStep::TemporarySynced,
            secure_directory::WriteStep::Renamed,
            secure_directory::WriteStep::ParentSynced,
        ]
    );
}

#[test]
fn identical_retry_resyncs_the_existing_file_and_parent() {
    let directory = tempfile::tempdir().unwrap();
    let store = test_store(directory.path().join("run"), None);
    store.write_named("first", "durable-output").unwrap();
    let steps = Mutex::new(Vec::new());

    store
        .write_named_with_observer("durable-output", |step| steps.lock().unwrap().push(step))
        .unwrap();

    assert_eq!(*steps.lock().unwrap(), vec![secure_directory::WriteStep::ParentSynced]);
}

#[test]
fn missing_local_blob_can_be_read_from_the_legacy_root_without_writing_there() {
    let directory = tempfile::tempdir().unwrap();
    let legacy_root = directory.path().join("legacy");
    std::fs::create_dir(&legacy_root).unwrap();
    let output = "legacy-output";
    let digest = stable_digest_bytes(output.as_bytes());
    let reference = format!("{}-{digest}.blob", stable_digest_bytes(b"legacy-effect"));
    std::fs::write(legacy_root.join(&reference), output).unwrap();
    let store = EffectOutputStore {
        root: None,
        legacy_root: Some(legacy_root),
    };

    assert_eq!(store.read(&reference).unwrap(), output);
    assert_eq!(
        store.write_named("new-effect", "new-output").unwrap_err().kind(),
        ErrorKind::Unsupported
    );
}

#[test]
fn read_rejects_content_that_does_not_match_the_reference_digest() {
    let directory = tempfile::tempdir().unwrap();
    let run_root = directory.path().join("run");
    std::fs::create_dir(&run_root).unwrap();
    let reference = format!("sha256-{}.blob", stable_digest_bytes(b"expected"));
    std::fs::write(run_root.join(&reference), "tampered").unwrap();
    let store = test_store(run_root, None);

    let error = store.read(&reference).unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidData);
}

#[test]
fn existing_hardlink_is_rejected_without_changing_the_other_name() {
    let directory = tempfile::tempdir().unwrap();
    let run_root = directory.path().join("run");
    std::fs::create_dir(&run_root).unwrap();
    let output = "protected-output";
    let reference = format!("sha256-{}.blob", stable_digest_bytes(output.as_bytes()));
    let outside = directory.path().join("outside.blob");
    std::fs::write(&outside, output).unwrap();
    std::fs::hard_link(&outside, run_root.join(&reference)).unwrap();
    let store = test_store(run_root, None);

    assert!(store.write_named("effect", output).is_err());
    assert_eq!(std::fs::read_to_string(outside).unwrap(), output);
}

#[test]
fn read_rejects_a_blob_symlink() {
    let directory = tempfile::tempdir().unwrap();
    let run_root = directory.path().join("run");
    std::fs::create_dir(&run_root).unwrap();
    let output = "outside-output";
    let reference = format!("sha256-{}.blob", stable_digest_bytes(output.as_bytes()));
    let outside = directory.path().join("outside.blob");
    std::fs::write(&outside, output).unwrap();
    if let Err(error) = create_file_symlink(&outside, &run_root.join(&reference)) {
        #[cfg(windows)]
        if error.kind() == ErrorKind::PermissionDenied || error.raw_os_error() == Some(1314) {
            return;
        }
        panic!("create file symlink: {error}");
    }
    let store = test_store(run_root, None);

    assert!(store.read(&reference).is_err());
}

#[test]
fn write_rejects_a_redirected_run_directory() {
    let directory = tempfile::tempdir().unwrap();
    let outside = directory.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    let run_root = directory.path().join("run");
    if let Err(error) = create_directory_symlink(&outside, &run_root) {
        #[cfg(windows)]
        if error.kind() == ErrorKind::PermissionDenied || error.raw_os_error() == Some(1314) {
            return;
        }
        panic!("create directory symlink: {error}");
    }
    let store = test_store(run_root, None);

    assert!(store.write_named("effect", "must-not-escape").is_err());
    assert!(std::fs::read_dir(outside).unwrap().next().is_none());
}

#[test]
fn local_run_blob_deletion_is_exact_idempotent_and_never_touches_legacy() {
    let directory = tempfile::tempdir().unwrap();
    let runtime_root = directory.path().join("runtime");
    let ledger = SqliteRuntimeLedger::open(runtime_root.join("ledger.sqlite3")).unwrap();
    let target = RunId::from("delete-target");
    let similar = RunId::from("delete-target-extra");
    let target_store = EffectOutputStore::for_run_with_ledger(&target, &ledger);
    let similar_store = EffectOutputStore::for_run_with_ledger(&similar, &ledger);
    let target_reference = target_store.write_named("target", "target-output").unwrap();
    let similar_reference = similar_store.write_named("similar", "similar-output").unwrap();
    let legacy_store = EffectOutputStore::for_legacy_run(&target);
    let legacy_reference = legacy_store.write_legacy_fixture("legacy-output").unwrap();

    delete_local_run_outputs(&target, &ledger).unwrap();
    delete_local_run_outputs(&target, &ledger).unwrap();

    assert_eq!(similar_store.read(&similar_reference).unwrap(), "similar-output");
    assert_eq!(legacy_store.read(&legacy_reference).unwrap(), "legacy-output");
    assert!(target_store.read(&target_reference).is_err());
    std::fs::remove_dir_all(legacy_run_root(&target)).unwrap();
}

#[test]
fn local_run_blob_deletion_rejects_a_hardlinked_blob() {
    let directory = tempfile::tempdir().unwrap();
    let runtime_root = directory.path().join("runtime");
    let ledger = SqliteRuntimeLedger::open(runtime_root.join("ledger.sqlite3")).unwrap();
    let run_id = RunId::from("hardlink-delete");
    let store = EffectOutputStore::for_run_with_ledger(&run_id, &ledger);
    let reference = store.write_named("effect", "protected-output").unwrap();
    let run_root = ledger
        .effect_output_root()
        .unwrap()
        .join(stable_digest_bytes(run_id.as_str().as_bytes()));
    let outside = directory.path().join("outside.blob");
    std::fs::hard_link(run_root.join(&reference), &outside).unwrap();

    assert!(delete_local_run_outputs(&run_id, &ledger).is_err());
    assert_eq!(std::fs::read_to_string(outside).unwrap(), "protected-output");
    assert!(run_root.join(reference).exists());
}

#[test]
fn local_run_blob_deletion_rejects_a_redirected_run_directory() {
    let directory = tempfile::tempdir().unwrap();
    let runtime_root = directory.path().join("runtime");
    let ledger = SqliteRuntimeLedger::open(runtime_root.join("ledger.sqlite3")).unwrap();
    let run_id = RunId::from("redirected-delete");
    let output_root = ledger.effect_output_root().unwrap();
    std::fs::create_dir(&output_root).unwrap();
    let outside = directory.path().join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::fs::write(outside.join("protected.blob"), "keep").unwrap();
    let run_root = output_root.join(stable_digest_bytes(run_id.as_str().as_bytes()));
    if let Err(error) = create_directory_symlink(&outside, &run_root) {
        #[cfg(windows)]
        if error.kind() == ErrorKind::PermissionDenied || error.raw_os_error() == Some(1314) {
            return;
        }
        panic!("create directory symlink: {error}");
    }

    assert!(delete_local_run_outputs(&run_id, &ledger).is_err());
    assert_eq!(std::fs::read_to_string(outside.join("protected.blob")).unwrap(), "keep");
}

#[test]
fn local_run_blob_deletion_never_accepts_the_legacy_global_root() {
    let run_id = RunId::from("legacy-delete-rejected");
    let legacy_store = EffectOutputStore::for_legacy_run(&run_id);
    let reference = legacy_store.write_legacy_fixture("keep-legacy").unwrap();
    let ledger = OutputRootLedger(effect_output_state_root());

    assert!(delete_local_run_outputs(&run_id, &ledger).is_err());
    assert_eq!(legacy_store.read(&reference).unwrap(), "keep-legacy");
    std::fs::remove_dir_all(legacy_run_root(&run_id)).unwrap();
}

#[cfg(unix)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}
