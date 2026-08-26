use std::sync::Arc;

use serde_json::json;
use solaris_types::identity::{AgentId, RunId, TeamId};
use solaris_types::runtime::{AgentLifecycleState, AgentRecord};
use solaris_types::spawner::AgentCollaborationContext;
use solaris_types::workflow::CollaborationStrategy;

use super::*;
use crate::resource_policy::ResourcePolicy;
use crate::runtime_ledger::{InMemoryRuntimeLedger, RuntimeLedger};
use crate::scheduler::Scheduler;

fn register_agents(runtime: &CollaborationRuntime<()>, run_id: &RunId, agents: &[AgentId]) {
    for agent_id in agents {
        runtime.agents().upsert(AgentRecord {
            run_id: run_id.clone(),
            agent_id: agent_id.clone(),
            team_id: None,
            parent_agent_id: None,
            state: AgentLifecycleState::Active,
        });
    }
}

fn create_team_runtime(
    ledger: Arc<dyn RuntimeLedger>,
    run_id: &RunId,
    team_id: &TeamId,
    sender: &AgentId,
    recipients: &[AgentId],
) -> CollaborationRuntime<()> {
    let runtime = CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(4)), ledger);
    let mut agents = vec![sender.clone()];
    agents.extend_from_slice(recipients);
    register_agents(&runtime, run_id, &agents);
    runtime
        .ensure_collaboration_team(
            run_id.clone(),
            "stable message team",
            &AgentCollaborationContext {
                team_id: team_id.clone(),
                strategy: CollaborationStrategy::Team,
                coordinator_agent_id: sender.clone(),
                max_pending_messages: 8,
                max_message_bytes: 1_024,
            },
        )
        .unwrap();
    for agent_id in agents {
        runtime.join_team(run_id, team_id, agent_id).unwrap();
    }
    runtime
}

#[test]
fn stable_send_and_broadcast_replay_precede_membership_changes_in_same_process() {
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let send_run = RunId::from("stable-send-membership-same-process");
    let send_team = TeamId::from("stable-send-team");
    let send_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = create_team_runtime(
        send_ledger.clone(),
        &send_run,
        &send_team,
        &sender,
        std::slice::from_ref(&target),
    );
    let first = runtime
        .send_message_with_id(
            "send-effect:first".into(),
            send_run.clone(),
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    let second = runtime
        .send_message_with_id(
            "send-effect:second".into(),
            send_run.clone(),
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    assert_ne!(first.message_id, second.message_id);
    assert!(
        runtime
            .send_message_with_id(
                "send-effect:first".into(),
                send_run.clone(),
                Some(send_team.clone()),
                sender.clone(),
                target.clone(),
                "result",
                json!({"value": 1}),
            )
            .unwrap_err()
            .to_string()
            .contains("different content")
    );
    assert!(runtime.teams().leave(&send_team, &target));
    assert!(runtime.agents().remove(&target).is_some());
    let replayed = runtime
        .send_message_with_id(
            "send-effect:first".into(),
            send_run.clone(),
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    assert_eq!(replayed, first);
    assert_eq!(runtime.messages().inbox(&target).len(), 2);
    assert_eq!(
        send_ledger
            .records_for_run(&send_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "message_delivered")
            .count(),
        2
    );

    let first_target = AgentId::from("first");
    let second_target = AgentId::from("second");
    let broadcast_run = RunId::from("stable-broadcast-membership-same-process");
    let broadcast_team = TeamId::from("stable-broadcast-team");
    let broadcast_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let runtime = create_team_runtime(
        broadcast_ledger.clone(),
        &broadcast_run,
        &broadcast_team,
        &sender,
        &[first_target.clone(), second_target.clone()],
    );
    let first_broadcast = runtime
        .broadcast_message_with_id(
            "broadcast-effect:first".into(),
            broadcast_run.clone(),
            broadcast_team.clone(),
            sender.clone(),
            "assignment",
            json!({"task": 1}),
        )
        .unwrap();
    assert!(runtime.teams().leave(&broadcast_team, &second_target));
    let replayed = runtime
        .broadcast_message_with_id(
            "broadcast-effect:first".into(),
            broadcast_run.clone(),
            broadcast_team.clone(),
            sender.clone(),
            "assignment",
            json!({"task": 1}),
        )
        .unwrap();
    assert_eq!(replayed, first_broadcast);
    let second_broadcast = runtime
        .broadcast_message_with_id(
            "broadcast-effect:second".into(),
            broadcast_run.clone(),
            broadcast_team,
            sender,
            "assignment",
            json!({"task": 1}),
        )
        .unwrap();
    assert_eq!(second_broadcast.len(), 1);
    assert_eq!(second_broadcast[0].to, first_target);
    assert_eq!(runtime.messages().inbox(&second_target).len(), 1);
    assert_eq!(
        broadcast_ledger
            .records_for_run(&broadcast_run)
            .unwrap()
            .iter()
            .filter(|record| record.record_type == "messages_broadcast")
            .count(),
        2
    );
}

#[test]
fn stable_send_and_broadcast_replay_precede_membership_changes_after_restart() {
    let sender = AgentId::from("sender");
    let target = AgentId::from("target");
    let send_run = RunId::from("stable-send-membership-restart");
    let send_team = TeamId::from("stable-send-restart-team");
    let send_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let original = create_team_runtime(
        send_ledger.clone(),
        &send_run,
        &send_team,
        &sender,
        std::slice::from_ref(&target),
    );
    let sent = original
        .send_message_with_id(
            "send-effect:restart".into(),
            send_run.clone(),
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    drop(original);
    let restarted = CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(4)), send_ledger);
    register_agents(&restarted, &send_run, &[sender.clone(), target.clone()]);
    restarted.restore_projection(&send_run).unwrap();
    assert!(restarted.teams().leave(&send_team, &target));
    assert!(restarted.agents().remove(&target).is_some());
    let replayed = restarted
        .send_message_with_id(
            "send-effect:restart".into(),
            send_run,
            None,
            sender.clone(),
            target.clone(),
            "result",
            json!({"value": 1}),
        )
        .unwrap();
    assert_eq!(replayed, sent);
    assert_eq!(restarted.messages().inbox(&target).len(), 1);

    let first_target = AgentId::from("first");
    let second_target = AgentId::from("second");
    let broadcast_run = RunId::from("stable-broadcast-membership-restart");
    let broadcast_team = TeamId::from("stable-broadcast-restart-team");
    let broadcast_ledger = Arc::new(InMemoryRuntimeLedger::default());
    let original = create_team_runtime(
        broadcast_ledger.clone(),
        &broadcast_run,
        &broadcast_team,
        &sender,
        &[first_target.clone(), second_target.clone()],
    );
    let sent = original
        .broadcast_message_with_id(
            "broadcast-effect:restart".into(),
            broadcast_run.clone(),
            broadcast_team.clone(),
            sender.clone(),
            "assignment",
            json!({"task": 1}),
        )
        .unwrap();
    drop(original);
    let restarted = CollaborationRuntime::with_ledger(Scheduler::new(ResourcePolicy::new(4)), broadcast_ledger);
    register_agents(
        &restarted,
        &broadcast_run,
        &[sender.clone(), first_target.clone(), second_target.clone()],
    );
    restarted.restore_projection(&broadcast_run).unwrap();
    assert!(restarted.teams().leave(&broadcast_team, &second_target));
    let replayed = restarted
        .broadcast_message_with_id(
            "broadcast-effect:restart".into(),
            broadcast_run,
            broadcast_team,
            sender,
            "assignment",
            json!({"task": 1}),
        )
        .unwrap();
    assert_eq!(replayed, sent);
    assert_eq!(restarted.messages().inbox(&first_target).len(), 1);
    assert_eq!(restarted.messages().inbox(&second_target).len(), 1);
}
