use solaris_compact::CompactLevel;
use solaris_types::config::{
    ConfigField, ConfigFieldResult, ConfigFieldStatus, ConfigUpdateOutcome, RuntimeConfigUpdate,
};
use solaris_types::llm::ThinkingConfig;
use solaris_types::resource::{MAX_ACTIVE_AGENTS, MIN_ACTIVE_AGENTS};

use super::AgentEngine;

const DEFAULT_THINKING_BUDGET: u32 = 10_000;

pub(super) fn apply(engine: &mut AgentEngine, update: RuntimeConfigUpdate) -> ConfigUpdateOutcome {
    let RuntimeConfigUpdate {
        model,
        thinking,
        thinking_budget,
        effort,
        compaction,
        multi_agent_policy,
        max_active_agents,
    } = update;
    if model.is_none()
        && thinking.is_none()
        && thinking_budget.is_none()
        && effort.is_none()
        && compaction.is_none()
        && multi_agent_policy.is_none()
        && max_active_agents.is_none()
    {
        return ConfigUpdateOutcome {
            applied: false,
            changed: false,
            results: Vec::new(),
            message: "set_config requires at least one field".to_owned(),
        };
    }

    let mut next_model = engine.model.clone();
    let mut next_thinking = engine.thinking.clone();
    let mut next_effort = engine.reasoning_effort.clone();
    let mut next_compaction = engine.compact_level;
    let mut next_multi_agent_policy = *engine
        .multi_agent_policy
        .read()
        .unwrap_or_else(|error| error.into_inner());
    let mut next_resource_budget = engine.resources.budget();
    let mut results = Vec::new();

    if let Some(new_model) = model {
        if new_model.trim().is_empty() {
            results.push(ConfigFieldResult::new(
                ConfigField::Model,
                ConfigFieldStatus::Rejected,
                "model must not be empty",
            ));
        } else {
            results.push(ConfigFieldResult::new(
                ConfigField::Model,
                ConfigFieldStatus::Applied,
                format!("model: {} -> {new_model}", engine.model),
            ));
            next_model = new_model;
        }
    }

    validate_thinking(
        engine,
        thinking.as_deref(),
        thinking_budget,
        &mut next_thinking,
        &mut results,
    );
    validate_effort(engine, effort, &mut next_effort, &mut results);
    validate_compaction(engine, compaction, &mut next_compaction, &mut results);
    if let Some(policy) = multi_agent_policy {
        results.push(ConfigFieldResult::new(
            ConfigField::MultiAgentPolicy,
            ConfigFieldStatus::Applied,
            format!("multi_agent_policy: {next_multi_agent_policy} -> {policy}"),
        ));
        next_multi_agent_policy = policy;
    }
    if let Some(limit) = max_active_agents {
        if !(MIN_ACTIVE_AGENTS..=MAX_ACTIVE_AGENTS).contains(&limit) {
            results.push(ConfigFieldResult::new(
                ConfigField::MaxActiveAgents,
                ConfigFieldStatus::Rejected,
                format!("max_active_agents must be between {MIN_ACTIVE_AGENTS} and {MAX_ACTIVE_AGENTS}"),
            ));
        } else {
            results.push(ConfigFieldResult::new(
                ConfigField::MaxActiveAgents,
                ConfigFieldStatus::Applied,
                format!(
                    "max_active_agents: {} -> {limit}",
                    engine.resources.effective_agent_limit()
                ),
            ));
            next_resource_budget.max_active_agents = Some(limit);
        }
    }

    if results.iter().any(|result| result.status != ConfigFieldStatus::Applied) {
        reject_applied_fields(&mut results);
        return ConfigUpdateOutcome {
            applied: false,
            changed: false,
            message: summary(&results),
            results,
        };
    }

    let changed = engine.model != next_model
        || !thinking_config_eq(&engine.thinking, &next_thinking)
        || engine.reasoning_effort != next_effort
        || engine.compact_level != next_compaction
        || *engine
            .multi_agent_policy
            .read()
            .unwrap_or_else(|error| error.into_inner())
            != next_multi_agent_policy
        || engine.resources.budget() != next_resource_budget;
    if engine.resources.budget() != next_resource_budget
        && let Err(error) = engine.resources.set_budget(next_resource_budget)
    {
        reject_applied_fields(&mut results);
        if let Some(result) = results
            .iter_mut()
            .find(|result| result.field == ConfigField::MaxActiveAgents)
        {
            result.message = Some(format!("max_active_agents persistence failed: {error}"));
        }
        return ConfigUpdateOutcome {
            applied: false,
            changed: false,
            message: summary(&results),
            results,
        };
    }
    engine.model = next_model;
    engine.thinking = next_thinking;
    engine.reasoning_effort = next_effort;
    engine.compact_level = next_compaction;
    *engine
        .multi_agent_policy
        .write()
        .unwrap_or_else(|error| error.into_inner()) = next_multi_agent_policy;
    engine.refresh_runtime_configuration();

    ConfigUpdateOutcome {
        applied: true,
        changed,
        message: summary(&results),
        results,
    }
}

fn validate_thinking(
    engine: &AgentEngine,
    thinking: Option<&str>,
    thinking_budget: Option<u32>,
    next_thinking: &mut Option<ThinkingConfig>,
    results: &mut Vec<ConfigFieldResult>,
) {
    if let Some(thinking_value) = thinking {
        if !matches!(thinking_value, "enabled" | "disabled") {
            results.push(ConfigFieldResult::new(
                ConfigField::Thinking,
                ConfigFieldStatus::Rejected,
                format!("invalid thinking value: {thinking_value} (expected: enabled, disabled)"),
            ));
        } else if !engine.compat.supports_thinking() {
            results.push(ConfigFieldResult::new(
                ConfigField::Thinking,
                ConfigFieldStatus::Unsupported,
                "current provider does not support thinking",
            ));
        } else if thinking_value == "enabled" {
            let budget = thinking_budget.unwrap_or(DEFAULT_THINKING_BUDGET);
            *next_thinking = Some(ThinkingConfig::Enabled { budget_tokens: budget });
            results.push(ConfigFieldResult::new(
                ConfigField::Thinking,
                ConfigFieldStatus::Applied,
                format!("thinking: enabled (budget: {budget})"),
            ));
        } else {
            *next_thinking = Some(ThinkingConfig::Disabled);
            results.push(ConfigFieldResult::new(
                ConfigField::Thinking,
                ConfigFieldStatus::Applied,
                "thinking: disabled",
            ));
        }
    }

    if let Some(new_budget) = thinking_budget {
        let (status, message) = if new_budget == 0 {
            (
                ConfigFieldStatus::Rejected,
                "thinking_budget must be greater than zero".to_owned(),
            )
        } else if !matches!(next_thinking.as_ref(), Some(ThinkingConfig::Enabled { .. })) {
            (
                ConfigFieldStatus::Rejected,
                "thinking_budget requires thinking to be enabled".to_owned(),
            )
        } else if !engine.compat.supports_thinking() {
            (
                ConfigFieldStatus::Unsupported,
                "current provider does not support a thinking budget".to_owned(),
            )
        } else {
            *next_thinking = Some(ThinkingConfig::Enabled {
                budget_tokens: new_budget,
            });
            (ConfigFieldStatus::Applied, format!("thinking_budget: {new_budget}"))
        };
        results.push(ConfigFieldResult::new(ConfigField::ThinkingBudget, status, message));
    }
}

fn validate_effort(
    engine: &AgentEngine,
    effort: Option<String>,
    next_effort: &mut Option<String>,
    results: &mut Vec<ConfigFieldResult>,
) {
    let Some(new_effort) = effort else {
        return;
    };
    if new_effort.is_empty() {
        *next_effort = None;
        results.push(ConfigFieldResult::new(
            ConfigField::Effort,
            ConfigFieldStatus::Applied,
            "effort: cleared",
        ));
    } else if !engine.compat.supports_effort() {
        results.push(ConfigFieldResult::new(
            ConfigField::Effort,
            ConfigFieldStatus::Unsupported,
            "current provider does not support effort",
        ));
    } else {
        let levels = engine.compat.effort_levels();
        if !levels.is_empty() && !levels.iter().any(|level| level == &new_effort) {
            results.push(ConfigFieldResult::new(
                ConfigField::Effort,
                ConfigFieldStatus::Rejected,
                format!("invalid effort level: {new_effort} (valid: {})", levels.join(", ")),
            ));
        } else {
            results.push(ConfigFieldResult::new(
                ConfigField::Effort,
                ConfigFieldStatus::Applied,
                format!(
                    "effort: {} -> {new_effort}",
                    engine.reasoning_effort.as_deref().unwrap_or("none")
                ),
            ));
            *next_effort = Some(new_effort);
        }
    }
}

fn validate_compaction(
    engine: &AgentEngine,
    compaction: Option<String>,
    next_compaction: &mut CompactLevel,
    results: &mut Vec<ConfigFieldResult>,
) {
    let Some(level_value) = compaction else {
        return;
    };
    match level_value.parse::<CompactLevel>() {
        Ok(new_level) => {
            results.push(ConfigFieldResult::new(
                ConfigField::Compaction,
                ConfigFieldStatus::Applied,
                format!("compaction: {} -> {new_level}", engine.compact_level),
            ));
            *next_compaction = new_level;
        }
        Err(error) => results.push(ConfigFieldResult::new(
            ConfigField::Compaction,
            ConfigFieldStatus::Rejected,
            error,
        )),
    }
}

fn reject_applied_fields(results: &mut [ConfigFieldResult]) {
    for result in results {
        if result.status == ConfigFieldStatus::Applied {
            result.status = ConfigFieldStatus::Rejected;
            result.message = Some("not applied because another field was rejected or unsupported".to_owned());
        }
    }
}

fn thinking_config_eq(left: &Option<ThinkingConfig>, right: &Option<ThinkingConfig>) -> bool {
    match (left, right) {
        (None, None) | (Some(ThinkingConfig::Disabled), Some(ThinkingConfig::Disabled)) => true,
        (
            Some(ThinkingConfig::Enabled {
                budget_tokens: left_budget,
            }),
            Some(ThinkingConfig::Enabled {
                budget_tokens: right_budget,
            }),
        ) => left_budget == right_budget,
        _ => false,
    }
}

fn summary(results: &[ConfigFieldResult]) -> String {
    results
        .iter()
        .map(|result| {
            let message = result.message.as_deref().unwrap_or("no details");
            format!("{}: {} ({message})", result.field, result.status)
        })
        .collect::<Vec<_>>()
        .join(", ")
}
