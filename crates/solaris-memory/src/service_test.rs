use tempfile::tempdir;

use super::{MemoryMutation, MemoryProposalDecision, MemoryScope, MemoryService, MemoryServiceError, MemorySnapshot};
use crate::types::MemoryType;

fn create_memory(name: &str, content: &str) -> MemoryMutation {
    MemoryMutation::Create {
        scope: MemoryScope::Memory,
        memory_type: MemoryType::Project,
        name: name.to_owned(),
        description: format!("description for {name}"),
        content: content.to_owned(),
    }
}

#[test]
fn create_edit_delete_are_versioned_and_survive_reopen() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("memory.sqlite3");
    let service = MemoryService::open(&database).unwrap();

    let created = service.apply(create_memory("release", "first release rule")).unwrap();
    assert_eq!(created.version, 1);

    let edited = service
        .apply(MemoryMutation::Edit {
            id: created.id.clone(),
            expected_version: 1,
            name: "release".to_owned(),
            description: "updated release rule".to_owned(),
            content: "second release rule".to_owned(),
        })
        .unwrap();
    assert_eq!(edited.version, 2);
    drop(service);

    let reopened = MemoryService::open(&database).unwrap();
    assert_eq!(
        reopened.get(&created.id).unwrap().unwrap().content,
        "second release rule"
    );
    reopened
        .apply(MemoryMutation::Delete {
            id: created.id.clone(),
            expected_version: 2,
        })
        .unwrap();
    assert!(reopened.get(&created.id).unwrap().is_none());
    assert_eq!(reopened.versions(&created.id).unwrap().len(), 3);
}

#[test]
fn persisted_snapshot_records_restore_without_reading_current_service_state() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    service
        .apply(create_memory("frozen", "visible in restored snapshot"))
        .unwrap();
    let frozen = service.snapshot().unwrap();
    let restored = MemorySnapshot::restore(frozen.captured_at_ms(), frozen.records().to_vec()).unwrap();

    service
        .apply(create_memory("later", "not in restored snapshot"))
        .unwrap();

    assert_eq!(restored.search("frozen").unwrap().len(), 1);
    assert!(restored.search("later").unwrap().is_empty());
}

#[test]
fn persisted_snapshot_restore_rejects_duplicate_record_ids() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    service.apply(create_memory("duplicate", "record")).unwrap();
    let frozen = service.snapshot().unwrap();
    let mut records = frozen.records().to_vec();
    records.push(records[0].clone());

    assert!(matches!(
        MemorySnapshot::restore(frozen.captured_at_ms(), records),
        Err(MemoryServiceError::CorruptRecord)
    ));
}

#[test]
fn stale_edit_is_rejected_without_changing_the_record() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    let created = service.apply(create_memory("stable", "original")).unwrap();

    let error = service
        .apply(MemoryMutation::Edit {
            id: created.id.clone(),
            expected_version: 9,
            name: "stable".to_owned(),
            description: "wrong".to_owned(),
            content: "must not persist".to_owned(),
        })
        .unwrap_err();

    assert!(matches!(error, MemoryServiceError::VersionConflict { .. }));
    assert_eq!(service.get(&created.id).unwrap().unwrap().content, "original");
}

#[test]
fn proposals_change_memory_only_after_approval() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();

    let approved = service.submit_proposal(create_memory("approved", "keep this")).unwrap();
    let rejected = service.submit_proposal(create_memory("rejected", "drop this")).unwrap();
    assert!(service.list().unwrap().is_empty());
    assert_eq!(service.pending_proposals().unwrap().len(), 2);

    let applied = service
        .review_proposal(&approved.id, MemoryProposalDecision::Approve)
        .unwrap()
        .unwrap();
    assert_eq!(applied.name, "approved");
    assert!(
        service
            .review_proposal(&rejected.id, MemoryProposalDecision::Reject)
            .unwrap()
            .is_none()
    );
    assert_eq!(service.list().unwrap().len(), 1);
    assert!(service.pending_proposals().unwrap().is_empty());
}

#[test]
fn fts_search_is_bounded_and_excludes_deleted_records() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    let mut first = None;
    for index in 0..12 {
        let record = service
            .apply(create_memory(
                &format!("entry-{index}"),
                &format!("needle {} {index}", "x".repeat(6_000)),
            ))
            .unwrap();
        first.get_or_insert(record);
    }
    let first = first.unwrap();
    service
        .apply(MemoryMutation::Delete {
            id: first.id,
            expected_version: first.version,
        })
        .unwrap();

    let results = service.search("needle").unwrap();
    assert!(results.len() <= 8);
    assert!(results.iter().map(|record| record.content.len()).sum::<usize>() <= 32 * 1024);
    assert!(results.iter().all(|record| record.name != "entry-0"));
}

#[test]
fn session_snapshot_does_not_observe_later_writes() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    let original = service.apply(create_memory("frozen", "old value")).unwrap();
    let snapshot = service.snapshot().unwrap();

    service
        .apply(MemoryMutation::Edit {
            id: original.id,
            expected_version: original.version,
            name: "frozen".to_owned(),
            description: "new description".to_owned(),
            content: "new value".to_owned(),
        })
        .unwrap();
    service.apply(create_memory("later", "later value")).unwrap();

    let frozen = snapshot.search("value").unwrap();
    assert_eq!(frozen.len(), 1);
    assert_eq!(frozen[0].content, "old value");
    assert_eq!(service.search("value").unwrap().len(), 2);
}

#[test]
fn session_snapshot_search_uses_fts_tokens_instead_of_substring_matching() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    service.apply(create_memory("observer", "service observer")).unwrap();

    let snapshot = service.snapshot().unwrap();

    assert!(snapshot.search("observer").unwrap().len() == 1);
    assert!(snapshot.search("serve").unwrap().is_empty());
}

#[test]
fn empty_or_oversized_inputs_fail_without_persisting_payloads() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();

    let empty = service.apply(create_memory("", "content")).unwrap_err();
    assert!(matches!(empty, MemoryServiceError::InvalidInput { .. }));

    let oversized = service
        .apply(create_memory("large", &"x".repeat(1024 * 1024 + 1)))
        .unwrap_err();
    assert!(matches!(oversized, MemoryServiceError::InvalidInput { .. }));
    assert!(service.list().unwrap().is_empty());
}

#[test]
fn a_reviewed_proposal_cannot_be_reviewed_again() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    let proposal = service.submit_proposal(create_memory("once", "review once")).unwrap();

    service
        .review_proposal(&proposal.id, MemoryProposalDecision::Approve)
        .unwrap();
    let error = service
        .review_proposal(&proposal.id, MemoryProposalDecision::Reject)
        .unwrap_err();

    assert!(matches!(error, MemoryServiceError::ProposalNotPending));
    assert_eq!(service.list().unwrap().len(), 1);
}

#[test]
fn a_stale_proposal_stays_pending_until_it_is_reviewed() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    let original = service.apply(create_memory("stable", "version one")).unwrap();
    let proposal = service
        .submit_proposal(MemoryMutation::Edit {
            id: original.id.clone(),
            expected_version: original.version,
            name: "stable".to_owned(),
            description: "proposed edit".to_owned(),
            content: "stale proposal".to_owned(),
        })
        .unwrap();
    service
        .apply(MemoryMutation::Edit {
            id: original.id.clone(),
            expected_version: original.version,
            name: "stable".to_owned(),
            description: "newer edit".to_owned(),
            content: "version two".to_owned(),
        })
        .unwrap();

    let error = service
        .review_proposal(&proposal.id, MemoryProposalDecision::Approve)
        .unwrap_err();
    assert!(matches!(error, MemoryServiceError::VersionConflict { .. }));
    assert!(
        service
            .review_proposal(&proposal.id, MemoryProposalDecision::Reject)
            .unwrap()
            .is_none()
    );
    assert_eq!(service.get(&original.id).unwrap().unwrap().content, "version two");
}

#[test]
fn search_handles_punctuation_and_truncates_at_a_utf8_boundary() {
    let temp = tempdir().unwrap();
    let service = MemoryService::open(temp.path().join("memory.sqlite3")).unwrap();
    service
        .apply(create_memory(
            "unicode",
            &format!("release.note {}", "界".repeat(20_000)),
        ))
        .unwrap();

    let results = service.search("release.note").unwrap();

    assert_eq!(results.len(), 1);
    assert!(results[0].content_truncated);
    assert!(results[0].content.len() <= 32 * 1024);
    assert!(std::str::from_utf8(results[0].content.as_bytes()).is_ok());
}

#[test]
fn user_scope_is_persisted_without_becoming_project_memory() {
    let temp = tempdir().unwrap();
    let database = temp.path().join("memory.sqlite3");
    let service = MemoryService::open(&database).unwrap();
    let created = service
        .apply(MemoryMutation::Create {
            scope: MemoryScope::User,
            memory_type: MemoryType::Feedback,
            name: "preference".to_owned(),
            description: "user preference".to_owned(),
            content: "prefer concise answers".to_owned(),
        })
        .unwrap();
    drop(service);

    let reopened = MemoryService::open(database).unwrap();
    assert_eq!(reopened.get(&created.id).unwrap().unwrap().scope, MemoryScope::User);
}
