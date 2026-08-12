use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AgentId, TaskId};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSpec {
    pub id: TaskId,
    pub summary: String,
    pub assignee: Option<AgentId>,
    pub dependencies: Vec<TaskId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TaskGraphError {
    #[error("task summary must not be empty: {0}")]
    EmptySummary(TaskId),
    #[error("duplicate task id: {0}")]
    DuplicateTask(TaskId),
    #[error("task {task_id} depends on unknown task {dependency_id}")]
    UnknownDependency { task_id: TaskId, dependency_id: TaskId },
    #[error("task {0} depends on itself")]
    SelfDependency(TaskId),
    #[error("task graph contains a dependency cycle")]
    Cycle,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskGraph {
    tasks: Vec<TaskSpec>,
}

impl TaskGraph {
    pub fn new(tasks: Vec<TaskSpec>) -> Result<Self, TaskGraphError> {
        let mut indices = BTreeMap::new();
        for (index, task) in tasks.iter().enumerate() {
            if task.summary.trim().is_empty() {
                return Err(TaskGraphError::EmptySummary(task.id.clone()));
            }
            if indices.insert(task.id.clone(), index).is_some() {
                return Err(TaskGraphError::DuplicateTask(task.id.clone()));
            }
        }

        for task in &tasks {
            for dependency in &task.dependencies {
                if dependency == &task.id {
                    return Err(TaskGraphError::SelfDependency(task.id.clone()));
                }
                if !indices.contains_key(dependency) {
                    return Err(TaskGraphError::UnknownDependency {
                        task_id: task.id.clone(),
                        dependency_id: dependency.clone(),
                    });
                }
            }
        }

        let graph = Self { tasks };
        if graph.has_cycle(&indices) {
            return Err(TaskGraphError::Cycle);
        }
        Ok(graph)
    }

    pub fn tasks(&self) -> &[TaskSpec] {
        &self.tasks
    }

    pub fn task(&self, id: &TaskId) -> Option<&TaskSpec> {
        self.tasks.iter().find(|task| &task.id == id)
    }

    pub fn ready_tasks(&self, completed: &BTreeSet<TaskId>) -> Vec<&TaskSpec> {
        self.tasks
            .iter()
            .filter(|task| !completed.contains(&task.id) && task.dependencies.iter().all(|id| completed.contains(id)))
            .collect()
    }

    fn has_cycle(&self, indices: &BTreeMap<TaskId, usize>) -> bool {
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        self.tasks
            .iter()
            .any(|task| self.visit(&task.id, indices, &mut visiting, &mut visited))
    }

    fn visit(
        &self,
        task_id: &TaskId,
        indices: &BTreeMap<TaskId, usize>,
        visiting: &mut BTreeSet<TaskId>,
        visited: &mut BTreeSet<TaskId>,
    ) -> bool {
        if visited.contains(task_id) {
            return false;
        }
        if !visiting.insert(task_id.clone()) {
            return true;
        }

        let task = &self.tasks[indices[task_id]];
        if task
            .dependencies
            .iter()
            .any(|dependency| self.visit(dependency, indices, visiting, visited))
        {
            return true;
        }

        visiting.remove(task_id);
        visited.insert(task_id.clone());
        false
    }
}
