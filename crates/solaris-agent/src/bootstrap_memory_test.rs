use std::sync::Arc;

use serial_test::serial;
use tempfile::tempdir;

use solaris_config::config::{CliArgs, Config};
use solaris_memory::paths::auto_memory_dir;
use solaris_memory::service::{MemoryMutation, MemoryScope, MemoryService};
use solaris_memory::types::MemoryType;

use super::AgentBootstrap;
use crate::execution_context::stable_digest_bytes;
use crate::memory_runtime::MemoryRuntime;
use crate::output::null_sink::NullSink;
use crate::session::{ActiveSessionError, SessionManager};

fn test_config(workspace: &std::path::Path) -> Config {
    Config::resolve(&CliArgs {
        provider: Some("openai".into()),
        api_key: Some("test".into()),
        base_url: Some("https://provider.example.test".into()),
        model: Some("test-model".into()),
        max_tokens: None,
        thinking: None,
        thinking_budget: None,
        max_turns: None,
        max_tool_call_malformed_turns: None,
        max_tool_call_failure_turns: None,
        system_prompt: None,
        profile: None,
        auto_approve: false,
        project_dir: Some(workspace.to_path_buf()),
    })
    .unwrap()
}

fn with_memory_base<T>(base: &std::path::Path, run: impl FnOnce() -> T) -> T {
    struct EnvironmentGuard(Option<std::ffi::OsString>);

    impl Drop for EnvironmentGuard {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => unsafe { std::env::set_var("SOLARIS_MEMORY_DIR", value) },
                None => unsafe { std::env::remove_var("SOLARIS_MEMORY_DIR") },
            }
        }
    }

    let guard = EnvironmentGuard(std::env::var_os("SOLARIS_MEMORY_DIR"));
    unsafe { std::env::set_var("SOLARIS_MEMORY_DIR", base) };
    let result = run();
    drop(guard);
    result
}

#[test]
#[serial]
fn disabled_memory_does_not_read_or_create_the_legacy_memory_directory() {
    let workspace = tempdir().unwrap();
    let memory_base = tempdir().unwrap();
    with_memory_base(memory_base.path(), || {
        let config = test_config(workspace.path());
        assert!(!config.memory.enabled);
        let mut bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink));

        let environment = bootstrap.resolve_environment(workspace.path().to_path_buf()).unwrap();

        assert!(environment.memory.is_none());
        assert!(!memory_base.path().join("projects").exists());
    });
}

#[test]
#[serial]
fn enabled_memory_freezes_a_service_snapshot_and_uses_it_in_the_prompt() {
    let workspace = tempdir().unwrap();
    let memory_base = tempdir().unwrap();
    with_memory_base(memory_base.path(), || {
        let directory = auto_memory_dir(workspace.path()).unwrap();
        let service = MemoryService::open(directory.join("memory.sqlite3")).unwrap();
        service
            .apply(MemoryMutation::Create {
                scope: MemoryScope::User,
                memory_type: MemoryType::Feedback,
                name: "response-style".to_owned(),
                description: "preferred response style".to_owned(),
                content: "private content is loaded only through Memory search".to_owned(),
            })
            .unwrap();
        drop(service);

        let mut config = test_config(workspace.path());
        config.memory.enabled = true;
        let mut bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink));
        let environment = bootstrap.resolve_environment(workspace.path().to_path_buf()).unwrap();
        bootstrap.configure_system_prompt(&environment, &[]);

        let prompt = bootstrap.config.system_prompt.as_deref().unwrap();
        assert!(prompt.contains("response-style"));
        assert!(prompt.contains("preferred response style"));
        assert!(!prompt.contains("private content is loaded"));
        assert!(environment.memory.is_some());
        assert!(
            bootstrap
                .permission_context
                .protected_path_fingerprint_material()
                .iter()
                .any(|entry| entry.contains("memory"))
        );
    });
}

#[test]
#[serial]
fn durable_session_restart_restores_its_original_memory_snapshot() {
    let workspace = tempdir().unwrap();
    let memory_base = tempdir().unwrap();
    let session_directory = workspace.path().join(".solaris").join("sessions");
    with_memory_base(memory_base.path(), || {
        let directory = auto_memory_dir(workspace.path()).unwrap();
        let service = MemoryService::open(directory.join("memory.sqlite3")).unwrap();
        service
            .apply(MemoryMutation::Create {
                scope: MemoryScope::Memory,
                memory_type: MemoryType::Project,
                name: "before-session".to_owned(),
                description: "visible in the first frozen view".to_owned(),
                content: "original durable memory".to_owned(),
            })
            .unwrap();
        drop(service);

        let mut config = test_config(workspace.path());
        config.memory.enabled = true;
        config.session.enabled = true;
        config.session.directory = session_directory.to_string_lossy().into_owned();

        let mut first_bootstrap =
            AgentBootstrap::new(config.clone(), workspace.path().to_string_lossy(), Arc::new(NullSink));
        first_bootstrap.session_manager = Some(SessionManager::new(session_directory.clone(), 20));
        let first_environment = first_bootstrap
            .resolve_environment(workspace.path().to_path_buf())
            .unwrap();
        let run_id = first_bootstrap.run_id.to_string();
        let first_manager = first_bootstrap.session_manager.as_ref().unwrap();
        let session = first_manager
            .create_active_session(
                "openai",
                "test-model",
                &workspace.path().to_string_lossy(),
                Some("stable"),
                &run_id,
            )
            .unwrap();
        first_manager.release_active_session().unwrap();

        first_environment
            .memory
            .as_ref()
            .unwrap()
            .service()
            .apply(MemoryMutation::Create {
                scope: MemoryScope::Memory,
                memory_type: MemoryType::Project,
                name: "written-during-session".to_owned(),
                description: "must wait until the next session".to_owned(),
                content: "new durable memory".to_owned(),
            })
            .unwrap();
        assert!(
            first_environment
                .memory
                .as_ref()
                .unwrap()
                .snapshot()
                .search("written-during-session")
                .unwrap()
                .is_empty()
        );
        drop(first_environment);
        drop(first_bootstrap);

        let restarted_manager = SessionManager::new(session_directory.clone(), 20);
        let restored_session = restarted_manager.load(&session.id).unwrap();
        drop(restarted_manager);
        assert!(
            restored_session
                .runtime_state
                .as_ref()
                .and_then(|state| state.memory_snapshot.as_ref())
                .is_some()
        );
        let mut restarted_bootstrap =
            AgentBootstrap::new(config.clone(), workspace.path().to_string_lossy(), Arc::new(NullSink))
                .resume(restored_session);
        restarted_bootstrap.acquire_resumed_session_lease().unwrap();
        let restored_environment = restarted_bootstrap
            .resolve_environment(workspace.path().to_path_buf())
            .unwrap();
        let restored_runtime = restored_environment.memory.as_ref().unwrap();
        assert_eq!(restored_runtime.snapshot().search("before-session").unwrap().len(), 1);
        assert!(
            restored_runtime
                .snapshot()
                .search("written-during-session")
                .unwrap()
                .is_empty()
        );
        restarted_bootstrap
            .session_manager
            .as_ref()
            .unwrap()
            .release_active_session()
            .unwrap();

        let mut next_bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink));
        next_bootstrap.session_manager = Some(SessionManager::new(session_directory.clone(), 20));
        let next_environment = next_bootstrap
            .resolve_environment(workspace.path().to_path_buf())
            .unwrap();
        let next_session_runtime = next_environment.memory.as_ref().unwrap();
        assert_eq!(
            next_session_runtime
                .snapshot()
                .search("written-during-session")
                .unwrap()
                .len(),
            1
        );
    });
}

#[test]
#[serial]
fn new_session_commits_its_reference_before_writing_snapshot_bytes() {
    let workspace = tempdir().unwrap();
    let memory_base = tempdir().unwrap();
    let session_directory = workspace.path().join(".solaris").join("sessions");
    with_memory_base(memory_base.path(), || {
        let directory = auto_memory_dir(workspace.path()).unwrap();
        let service = MemoryService::open(directory.join("memory.sqlite3")).unwrap();
        service
            .apply(MemoryMutation::Create {
                scope: MemoryScope::Memory,
                memory_type: MemoryType::Project,
                name: "sensitive-before-create".to_owned(),
                description: "must not become an orphan blob".to_owned(),
                content: "sensitive snapshot bytes".to_owned(),
            })
            .unwrap();
        drop(service);

        let mut config = test_config(workspace.path());
        config.memory.enabled = true;
        config.session.enabled = true;
        config.session.directory = session_directory.to_string_lossy().into_owned();
        let mut bootstrap = AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink));
        bootstrap.session_manager = Some(SessionManager::new(session_directory.clone(), 20));
        let environment = bootstrap.resolve_environment(workspace.path().to_path_buf()).unwrap();
        let run_id = bootstrap.run_id.to_string();
        let snapshot_root = workspace
            .path()
            .join(".solaris/runtime/effect-outcomes")
            .join(stable_digest_bytes(run_id.as_bytes()));
        assert!(!snapshot_root.exists(), "snapshot preparation must not write bytes");

        let manager = bootstrap.session_manager.as_ref().unwrap();
        let result = manager.create_active_session_with_memory_test_observer(
            "openai",
            "test-model",
            &workspace.path().to_string_lossy(),
            Some("crash-before-blob"),
            &run_id,
            || {
                let stored = manager.load("crash-before-blob").unwrap();
                assert!(
                    stored
                        .runtime_state
                        .as_ref()
                        .and_then(|state| state.memory_snapshot.as_ref())
                        .is_some(),
                    "the session reference must already be durable"
                );
                assert!(
                    !snapshot_root.exists(),
                    "the blob must not precede its session reference"
                );
                Err(std::io::Error::other("simulated crash before blob write"))
            },
        );
        assert!(matches!(result, Err(ActiveSessionError::MemorySnapshotPersistence(_))));
        assert!(
            !snapshot_root.exists(),
            "the interrupted creation must not leave an orphan blob"
        );
        assert!(manager.load("crash-before-blob").is_ok());
        drop(environment);
    });
}

#[test]
#[serial]
fn resumed_legacy_session_cas_commits_snapshot_reference_before_blob_write() {
    let workspace = tempdir().unwrap();
    let memory_base = tempdir().unwrap();
    let session_directory = workspace.path().join(".solaris").join("sessions");
    with_memory_base(memory_base.path(), || {
        let manager = SessionManager::new(session_directory.clone(), 20);
        let run_id = "run-legacy-memory-crash";
        let legacy = manager
            .create_active_session(
                "openai",
                "test-model",
                &workspace.path().to_string_lossy(),
                Some("legacy"),
                run_id,
            )
            .unwrap();
        manager.release_active_session().unwrap();
        assert!(legacy.runtime_state.is_none());

        let directory = auto_memory_dir(workspace.path()).unwrap();
        let service = MemoryService::open(directory.join("memory.sqlite3")).unwrap();
        service
            .apply(MemoryMutation::Create {
                scope: MemoryScope::Memory,
                memory_type: MemoryType::Project,
                name: "before-crash".to_owned(),
                description: "the only acceptable frozen view".to_owned(),
                content: "original sensitive Memory".to_owned(),
            })
            .unwrap();
        drop(service);

        let snapshot_root = workspace
            .path()
            .join(".solaris/runtime/effect-outcomes")
            .join(stable_digest_bytes(run_id.as_bytes()));
        let (_runtime, _reference, prepared) =
            MemoryRuntime::open_for_session(workspace.path(), false, &snapshot_root, None).unwrap();
        let prepared = prepared.expect("legacy session needs a prepared snapshot");
        let snapshot_path = prepared.path();
        let mut leased = manager.load_active_session(&legacy.id).unwrap();
        let result = manager.install_active_memory_snapshot_with_test_observer(&mut leased, &prepared, || {
            Err(std::io::Error::other("simulated crash after reference CAS"))
        });
        assert!(matches!(result, Err(ActiveSessionError::MemorySnapshotPersistence(_))));
        manager.release_active_session().unwrap();
        assert!(!snapshot_path.exists());

        let stored = manager.load(&legacy.id).unwrap();
        let committed_reference = stored
            .runtime_state
            .as_ref()
            .and_then(|state| state.memory_snapshot.as_ref())
            .cloned()
            .expect("the CAS must survive the interrupted blob write");
        assert_eq!(committed_reference, prepared.reference().clone());

        let service = MemoryService::open(directory.join("memory.sqlite3")).unwrap();
        service
            .apply(MemoryMutation::Create {
                scope: MemoryScope::Memory,
                memory_type: MemoryType::Project,
                name: "after-crash".to_owned(),
                description: "must never replace the committed reference".to_owned(),
                content: "newer sensitive Memory".to_owned(),
            })
            .unwrap();
        drop(service);

        let (_new_runtime, _new_reference, replacement) =
            MemoryRuntime::open_for_session(workspace.path(), false, &snapshot_root, None).unwrap();
        let replacement = replacement.unwrap();
        let mut leased = manager.load_active_session(&legacy.id).unwrap();
        let replace_result = manager.install_active_memory_snapshot(&mut leased, &replacement);
        assert!(matches!(
            replace_result,
            Err(ActiveSessionError::MemorySnapshotAlreadySet { .. })
        ));
        manager.release_active_session().unwrap();
        assert!(!replacement.path().exists());
        drop(manager);

        let mut config = test_config(workspace.path());
        config.memory.enabled = true;
        config.session.enabled = true;
        config.session.directory = session_directory.to_string_lossy().into_owned();
        let mut restarted =
            AgentBootstrap::new(config, workspace.path().to_string_lossy(), Arc::new(NullSink)).resume(stored);
        restarted.acquire_resumed_session_lease().unwrap();
        let error = restarted
            .resolve_environment(workspace.path().to_path_buf())
            .err()
            .expect("missing committed blob must fail closed");
        assert!(
            error
                .to_string()
                .contains("failed to read the durable session memory snapshot")
        );
        restarted
            .session_manager
            .as_ref()
            .unwrap()
            .release_active_session()
            .unwrap();
    });
}
