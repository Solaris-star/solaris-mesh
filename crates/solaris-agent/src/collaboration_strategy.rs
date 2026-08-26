use serde::{Deserialize, Serialize};
use solaris_types::identity::AgentId;
use solaris_types::workflow::CollaborationStrategy;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationPlan {
    pub strategy: CollaborationStrategy,
    pub members: Vec<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinator: Option<AgentId>,
    pub direct_peer_messaging: bool,
    pub requires_synthesis: bool,
}

pub trait StrategyPlanner: Send + Sync {
    fn strategy(&self) -> CollaborationStrategy;
    fn plan(&self, coordinator: AgentId, workers: Vec<AgentId>) -> CollaborationPlan;
}

pub struct SupervisorPlanner;
pub struct TeamPlanner;
pub struct FanoutPlanner;
pub struct IndependentReviewerPlanner;

pub fn plan_collaboration(
    strategy: CollaborationStrategy,
    coordinator: AgentId,
    workers: Vec<AgentId>,
) -> CollaborationPlan {
    match strategy {
        CollaborationStrategy::Single => CollaborationPlan {
            strategy,
            members: vec![coordinator.clone()],
            coordinator: Some(coordinator),
            direct_peer_messaging: false,
            requires_synthesis: false,
        },
        CollaborationStrategy::Supervisor => SupervisorPlanner.plan(coordinator, workers),
        CollaborationStrategy::Team => TeamPlanner.plan(coordinator, workers),
        CollaborationStrategy::Fanout => FanoutPlanner.plan(coordinator, workers),
        CollaborationStrategy::IndependentReviewer => IndependentReviewerPlanner.plan(coordinator, workers),
    }
}

impl StrategyPlanner for SupervisorPlanner {
    fn strategy(&self) -> CollaborationStrategy {
        CollaborationStrategy::Supervisor
    }

    fn plan(&self, coordinator: AgentId, workers: Vec<AgentId>) -> CollaborationPlan {
        let mut members = vec![coordinator.clone()];
        members.extend(workers);
        CollaborationPlan {
            strategy: self.strategy(),
            members,
            coordinator: Some(coordinator),
            direct_peer_messaging: false,
            requires_synthesis: true,
        }
    }
}

impl StrategyPlanner for TeamPlanner {
    fn strategy(&self) -> CollaborationStrategy {
        CollaborationStrategy::Team
    }

    fn plan(&self, coordinator: AgentId, workers: Vec<AgentId>) -> CollaborationPlan {
        let mut members = vec![coordinator.clone()];
        members.extend(workers);
        CollaborationPlan {
            strategy: self.strategy(),
            members,
            coordinator: Some(coordinator),
            direct_peer_messaging: true,
            requires_synthesis: false,
        }
    }
}

impl StrategyPlanner for FanoutPlanner {
    fn strategy(&self) -> CollaborationStrategy {
        CollaborationStrategy::Fanout
    }

    fn plan(&self, coordinator: AgentId, workers: Vec<AgentId>) -> CollaborationPlan {
        CollaborationPlan {
            strategy: self.strategy(),
            members: workers,
            coordinator: Some(coordinator),
            direct_peer_messaging: false,
            requires_synthesis: true,
        }
    }
}

impl StrategyPlanner for IndependentReviewerPlanner {
    fn strategy(&self) -> CollaborationStrategy {
        CollaborationStrategy::IndependentReviewer
    }

    fn plan(&self, coordinator: AgentId, workers: Vec<AgentId>) -> CollaborationPlan {
        CollaborationPlan {
            strategy: self.strategy(),
            members: workers,
            coordinator: Some(coordinator),
            direct_peer_messaging: false,
            requires_synthesis: false,
        }
    }
}
