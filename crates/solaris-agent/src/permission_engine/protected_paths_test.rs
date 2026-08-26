use super::*;
use solaris_process::ProtectedObjectIdentity;

#[test]
fn retained_state_identity_history_is_bounded_and_fails_closed() {
    const EXPECTED_MAX_RETAINED_IDENTITIES: usize = 64;

    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    std::fs::create_dir_all(&runtime).expect("runtime");
    let mut policy = ProtectedPathPolicy::default();

    let mut saturation_error = None;
    for generation in 0..=(EXPECTED_MAX_RETAINED_IDENTITIES + 1) {
        let sidecar = runtime.join(format!("ledger.sqlite3-sidecar-{generation}"));
        std::fs::write(&sidecar, format!("state-{generation}")).expect("create sidecar generation");
        if let Err(error) = policy.register(&runtime, vec![sidecar]) {
            saturation_error = Some(error);
            break;
        }
    }

    assert_eq!(policy.file_identities.len(), EXPECTED_MAX_RETAINED_IDENTITIES);
    assert!(
        saturation_error
            .as_deref()
            .is_some_and(|error| error.contains("retained identity capacity")),
        "identity churn must fail closed once the bounded history is full"
    );
    assert!(policy.refresh_file_identities().is_err());
    assert!(policy.protects(&runtime.join("unrelated-file")));
    assert!(
        policy
            .fingerprint_material()
            .contains(&"retained-identities:exhausted".to_owned())
    );
}

#[cfg(not(windows))]
#[test]
fn one_recreated_sidecar_cannot_grow_the_identity_history_past_the_limit() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    let wal = runtime.join("ledger.sqlite3-wal");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(&wal, b"state-0").expect("initial sidecar");
    let mut policy = ProtectedPathPolicy::default();
    policy.register(&runtime, vec![wal.clone()]).expect("register sidecar");

    for generation in 1..MAX_RETAINED_IDENTITIES {
        std::fs::remove_file(&wal).expect("remove preceding sidecar generation");
        std::fs::write(&wal, format!("state-{generation}")).expect("recreate sidecar");
        policy.refresh_file_identities().expect("retain identity within limit");
    }
    std::fs::remove_file(&wal).expect("remove final retained generation");
    std::fs::write(&wal, b"overflow").expect("recreate overflow generation");

    let error = policy
        .refresh_file_identities()
        .expect_err("identity churn beyond the limit must fail closed");

    assert!(error.contains("retained identity capacity"));
    assert_eq!(policy.file_identities.len(), MAX_RETAINED_IDENTITIES);
}

#[test]
fn registering_the_same_live_objects_does_not_consume_identity_capacity() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    let database = runtime.join("ledger.sqlite3");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(&database, b"state").expect("database");
    let mut policy = ProtectedPathPolicy::default();

    for _ in 0..(MAX_RETAINED_IDENTITIES * 2) {
        policy
            .register(&runtime, vec![database.clone()])
            .expect("same protected objects must be deduplicated");
    }

    assert_eq!(policy.roots.len(), 1);
    assert_eq!(policy.files.len(), 1);
    assert_eq!(policy.slots.len(), 1);
    assert_eq!(policy.file_identities.len(), 1);
    assert!(!policy.retention_exhausted);
}

#[test]
fn process_snapshot_keeps_the_identity_of_a_rotated_protected_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = directory.path().join("runtime");
    let database = runtime.join("session.sqlite3");
    let old_alias = directory.path().join("old-session.sqlite3");
    std::fs::create_dir_all(&runtime).expect("runtime");
    std::fs::write(&database, b"old").expect("old database");
    std::fs::hard_link(&database, &old_alias).expect("old alias");
    let old_identity = ProtectedObjectIdentity::from_file(&std::fs::File::open(&old_alias).unwrap()).unwrap();
    let mut policy = ProtectedPathPolicy::default();
    policy
        .register(&runtime, vec![database.clone()])
        .expect("register database");
    std::fs::remove_file(&database).expect("rotate old database");
    std::fs::write(&database, b"new").expect("new database");

    let (_, identities) = policy.process_snapshot().expect("process snapshot");

    assert!(identities.contains(&old_identity));
    assert!(identities.contains(&ProtectedObjectIdentity::from_file(&std::fs::File::open(database).unwrap()).unwrap()));
}
