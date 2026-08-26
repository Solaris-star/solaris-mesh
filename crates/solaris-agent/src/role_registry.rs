use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use solaris_types::workflow::AgentRoleDefinition;

#[derive(Default)]
pub struct AgentRoleRegistry {
    state: RwLock<RoleRegistryState>,
}

#[derive(Default)]
struct RoleRegistryState {
    roles: HashMap<String, AgentRoleDefinition>,
    owners: HashMap<String, HashSet<String>>,
}

impl AgentRoleRegistry {
    pub fn register(&self, role: AgentRoleDefinition) {
        let _ = self.register_owned("core", role);
    }

    pub fn register_owned(&self, owner: &str, role: AgentRoleDefinition) -> Result<(), String> {
        if owner.trim().is_empty() {
            return Err("agent role owner must not be empty".into());
        }
        let mut state = self.state.write().unwrap_or_else(|error| error.into_inner());
        if let Some(existing) = state.roles.get(&role.id)
            && existing != &role
        {
            return Err(format!(
                "agent role {} is already registered with a different definition",
                role.id
            ));
        }
        let role_id = role.id.clone();
        state.roles.entry(role_id.clone()).or_insert(role);
        state.owners.entry(role_id).or_default().insert(owner.to_owned());
        Ok(())
    }

    pub fn unregister_owned(&self, owner: &str, role_id: &str) -> bool {
        let mut state = self.state.write().unwrap_or_else(|error| error.into_inner());
        let Some(owners) = state.owners.get_mut(role_id) else {
            return false;
        };
        if !owners.remove(owner) {
            return false;
        }
        if owners.is_empty() {
            state.owners.remove(role_id);
            state.roles.remove(role_id);
        }
        true
    }

    pub fn get(&self, id: &str) -> Option<AgentRoleDefinition> {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .roles
            .get(id)
            .cloned()
    }

    pub fn snapshot(&self) -> Vec<AgentRoleDefinition> {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .roles
            .values()
            .cloned()
            .collect()
    }
}
