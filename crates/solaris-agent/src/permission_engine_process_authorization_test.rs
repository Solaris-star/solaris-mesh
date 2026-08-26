use super::*;

use std::sync::mpsc;
use std::time::Duration;

#[test]
fn permission_mutation_waits_until_guarded_spawn_section_finishes() {
    let permissions = PermissionContext::new(PermissionMode::Auto, PermissionCeiling::unrestricted());
    let guarded_permissions = permissions.clone();
    let mutating_permissions = permissions.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (started_tx, started_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();

    let guarded = std::thread::spawn(move || {
        guarded_permissions.with_process_spawn_gate(|| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
    });
    entered_rx.recv().unwrap();
    let mutation = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        mutating_permissions.set_mode(PermissionMode::Plan);
        completed_tx.send(()).unwrap();
    });
    started_rx.recv().unwrap();

    assert!(completed_rx.recv_timeout(Duration::from_millis(100)).is_err());
    release_tx.send(()).unwrap();
    completed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    guarded.join().unwrap();
    mutation.join().unwrap();
    assert_eq!(permissions.mode(), PermissionMode::Plan);
}
