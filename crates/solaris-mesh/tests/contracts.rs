use std::collections::BTreeSet;

use async_trait::async_trait;
use solaris_mesh::{
    AgentId, AgentIdentity, AgentLink, AgentRole, MailboxMessage, MailboxMessageError, MessageId, MessageKind,
    MessageTarget, Runtime, RuntimeCommand, RuntimeError, RuntimeEvent, TaskGraph, TaskGraphError, TaskId, TaskSpec,
    TeamId, TeamTopology, TopologyError, WorkspaceId,
};

fn agent(id: &str, role: AgentRole) -> AgentIdentity {
    AgentIdentity::new(AgentId::new(id).unwrap(), role)
}

fn task(id: &str, dependencies: &[&str]) -> TaskSpec {
    TaskSpec {
        id: TaskId::new(id).unwrap(),
        summary: format!("Task {id}"),
        assignee: None,
        dependencies: dependencies
            .iter()
            .map(|dependency| TaskId::new(*dependency).unwrap())
            .collect(),
    }
}

#[test]
fn identifiers_reject_blank_values_and_round_trip() {
    assert_eq!(
        AgentId::new("  ").unwrap_err().to_string(),
        "agent id must not be empty"
    );

    let id: AgentId = "worker-1".parse().unwrap();
    assert_eq!(id.as_str(), "worker-1");
    assert_eq!(id.to_string(), "worker-1");
    assert_eq!(serde_json::to_string(&id).unwrap(), "\"worker-1\"");
}

#[test]
fn identity_reports_supported_capabilities() {
    let mut identity = AgentIdentity::new(AgentId::new("worker-1").unwrap(), AgentRole::Worker);
    identity.capabilities = BTreeSet::from(["code".to_owned(), "review".to_owned()]);

    assert!(identity.supports("code"));
    assert!(!identity.supports("design"));
}

#[test]
fn topology_accepts_supervisor_worker_and_peer_links() {
    let lead = agent("lead", AgentRole::Supervisor);
    let worker = agent("worker", AgentRole::Worker);
    let peer = agent("peer", AgentRole::Peer);
    let topology = TeamTopology::new(
        TeamId::new("team-1").unwrap(),
        vec![lead.clone(), worker.clone(), peer.clone()],
        vec![
            AgentLink::directed(lead.id.clone(), worker.id.clone()),
            AgentLink::peer(worker.id.clone(), peer.id.clone()),
        ],
    )
    .unwrap();

    assert_eq!(topology.agents().len(), 3);
    assert_eq!(topology.links().len(), 2);
    assert_eq!(topology.agent(&peer.id), Some(&peer));
}

#[test]
fn topology_rejects_empty_duplicate_unknown_and_self_links() {
    let team_id = TeamId::new("team-1").unwrap();
    assert_eq!(
        TeamTopology::new(team_id.clone(), vec![], vec![]),
        Err(TopologyError::Empty)
    );

    let worker = agent("worker", AgentRole::Worker);
    assert_eq!(
        TeamTopology::new(team_id.clone(), vec![worker.clone(), worker.clone()], vec![]),
        Err(TopologyError::DuplicateAgent(worker.id.clone()))
    );
    assert_eq!(
        TeamTopology::new(
            team_id.clone(),
            vec![worker.clone()],
            vec![AgentLink::directed(worker.id.clone(), AgentId::new("missing").unwrap())],
        ),
        Err(TopologyError::UnknownAgent(AgentId::new("missing").unwrap()))
    );
    assert_eq!(
        TeamTopology::new(
            team_id,
            vec![worker.clone()],
            vec![AgentLink::peer(worker.id.clone(), worker.id.clone())],
        ),
        Err(TopologyError::SelfLink(worker.id))
    );
}

#[test]
fn task_graph_returns_only_ready_incomplete_tasks() {
    let graph = TaskGraph::new(vec![task("a", &[]), task("b", &["a"]), task("c", &["b"])]).unwrap();
    let completed = BTreeSet::from([TaskId::new("a").unwrap()]);
    let ready: Vec<_> = graph
        .ready_tasks(&completed)
        .iter()
        .map(|task| task.id.as_str())
        .collect();

    assert_eq!(graph.tasks().len(), 3);
    assert_eq!(graph.task(&TaskId::new("b").unwrap()).unwrap().summary, "Task b");
    assert_eq!(ready, vec!["b"]);
}

#[test]
fn task_graph_rejects_invalid_graphs() {
    let blank = TaskSpec {
        id: TaskId::new("blank").unwrap(),
        summary: " ".to_owned(),
        assignee: None,
        dependencies: vec![],
    };
    assert_eq!(
        TaskGraph::new(vec![blank]),
        Err(TaskGraphError::EmptySummary(TaskId::new("blank").unwrap()))
    );
    assert_eq!(
        TaskGraph::new(vec![task("a", &[]), task("a", &[])]),
        Err(TaskGraphError::DuplicateTask(TaskId::new("a").unwrap()))
    );
    assert_eq!(
        TaskGraph::new(vec![task("a", &["missing"])]),
        Err(TaskGraphError::UnknownDependency {
            task_id: TaskId::new("a").unwrap(),
            dependency_id: TaskId::new("missing").unwrap(),
        })
    );
    assert_eq!(
        TaskGraph::new(vec![task("a", &["a"])]),
        Err(TaskGraphError::SelfDependency(TaskId::new("a").unwrap()))
    );
    assert_eq!(
        TaskGraph::new(vec![task("a", &["b"]), task("b", &["a"])]),
        Err(TaskGraphError::Cycle)
    );
}

#[test]
fn mailbox_message_validates_content_and_serializes_target() {
    let result = MailboxMessage::new(
        MessageId::new("message-1").unwrap(),
        TeamId::new("team-1").unwrap(),
        AgentId::new("lead").unwrap(),
        MessageTarget::Agent(AgentId::new("worker").unwrap()),
        MessageKind::Task,
        "Implement the parser",
        42,
        None,
    )
    .unwrap();

    let value = serde_json::to_value(result).unwrap();
    assert_eq!(value["to"]["type"], "agent");
    assert_eq!(value["to"]["agent_id"], "worker");

    let error = MailboxMessage::new(
        MessageId::new("message-2").unwrap(),
        TeamId::new("team-1").unwrap(),
        AgentId::new("lead").unwrap(),
        MessageTarget::Broadcast,
        MessageKind::Status,
        " ",
        43,
        None,
    )
    .unwrap_err();
    assert_eq!(error, MailboxMessageError::EmptyContent);
}

struct RecordingRuntime;

#[async_trait]
impl Runtime for RecordingRuntime {
    async fn execute(&self, command: RuntimeCommand) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        Ok(vec![RuntimeEvent::TeamStarted {
            team_id: command.team_id().clone(),
        }])
    }
}

#[tokio::test]
async fn runtime_trait_is_host_implementable_and_preserves_team_identity() {
    let team_id = TeamId::new("team-1").unwrap();
    let topology = TeamTopology::new(
        team_id.clone(),
        vec![AgentIdentity::new(AgentId::new("lead").unwrap(), AgentRole::Supervisor)],
        vec![],
    )
    .unwrap();
    let command = RuntimeCommand::StartTeam {
        topology,
        workspace_id: Some(WorkspaceId::new("workspace-1").unwrap()),
    };

    assert_eq!(command.team_id(), &team_id);
    let events = RecordingRuntime.execute(command).await.unwrap();
    assert_eq!(events[0].team_id(), &team_id);
    assert_eq!(serde_json::to_value(&events[0]).unwrap()["type"], "team_started");
}

#[test]
fn runtime_error_variants_have_stable_messages() {
    assert_eq!(
        RuntimeError::InvalidCommand("missing task".to_owned()).to_string(),
        "runtime command is invalid: missing task"
    );
    assert_eq!(
        RuntimeError::NotFound("team-1".to_owned()).to_string(),
        "runtime resource was not found: team-1"
    );
    assert_eq!(
        RuntimeError::Conflict("busy".to_owned()).to_string(),
        "runtime state conflict: busy"
    );
    assert_eq!(
        RuntimeError::Unavailable("offline".to_owned()).to_string(),
        "runtime is unavailable: offline"
    );
    assert_eq!(
        RuntimeError::Internal("failed".to_owned()).to_string(),
        "runtime operation failed: failed"
    );
}
