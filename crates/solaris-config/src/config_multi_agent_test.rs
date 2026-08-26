#[test]
fn multi_agent_defaults_are_on_demand_and_auto() {
    let config = MultiAgentConfig::resolve(MultiAgentConfigFile::default()).unwrap();
    assert_eq!(config.policy, solaris_types::workflow::MultiAgentPolicy::OnDemand);
    assert!(matches!(config.strategy, solaris_types::workflow::CollaborationSelection::Auto));
    assert_eq!(config.max_active_agents, None);
    assert_eq!(config.max_tasks_per_run, 32);
    assert!(!config.policy_is_explicit());
    assert!(!config.strategy_is_explicit());
}

#[test]
fn multi_agent_config_accepts_aliases_and_validates_limits() {
    let mut config = MultiAgentConfig::resolve(MultiAgentConfigFile {
        policy: Some(solaris_types::workflow::MultiAgentPolicy::OnDemand),
        strategy: Some("independent-reviewer".to_owned()),
        max_active_agents: Some(4),
        max_tasks_per_run: Some(64),
    })
    .unwrap();
    assert_eq!(config.max_active_agents, Some(4));
    assert_eq!(config.max_tasks_per_run, 64);
    assert!(config.policy_is_explicit());
    assert!(config.strategy_is_explicit());
    assert!(matches!(
        config.strategy,
        solaris_types::workflow::CollaborationSelection::Configured(ref value)
            if value.strategy == solaris_types::workflow::CollaborationStrategy::IndependentReviewer
    ));

    config
        .merge_overrides(MultiAgentOverrides {
            policy: Some("proactive".to_owned()),
            strategy: Some("team".to_owned()),
            max_active_agents: Some(8),
            max_tasks_per_run: Some(128),
        })
        .unwrap();
    assert_eq!(config.policy, solaris_types::workflow::MultiAgentPolicy::Proactive);
    assert_eq!(config.max_active_agents, Some(8));
    assert_eq!(config.max_tasks_per_run, 128);
}

#[test]
fn multi_agent_config_rejects_invalid_limits_and_strategy() {
    assert!(MultiAgentConfig::resolve(MultiAgentConfigFile {
        max_active_agents: Some(65),
        ..MultiAgentConfigFile::default()
    })
    .is_err());
    assert!(MultiAgentConfig::resolve(MultiAgentConfigFile {
        max_tasks_per_run: Some(257),
        ..MultiAgentConfigFile::default()
    })
    .is_err());
    assert!(MultiAgentConfig::resolve(MultiAgentConfigFile {
        strategy: Some("unknown".to_owned()),
        ..MultiAgentConfigFile::default()
    })
    .is_err());
}

#[test]
fn project_multi_agent_values_override_global_values() {
    let merged = merge_config_files(
        ConfigFile {
            multi_agent: MultiAgentConfigFile {
                policy: Some(solaris_types::workflow::MultiAgentPolicy::Disabled),
                strategy: Some("single".to_owned()),
                max_active_agents: Some(2),
                max_tasks_per_run: Some(32),
            },
            ..ConfigFile::default()
        },
        ConfigFile {
            multi_agent: MultiAgentConfigFile {
                policy: Some(solaris_types::workflow::MultiAgentPolicy::Proactive),
                strategy: None,
                max_active_agents: None,
                max_tasks_per_run: Some(64),
            },
            ..ConfigFile::default()
        },
    );
    assert_eq!(merged.multi_agent.policy, Some(solaris_types::workflow::MultiAgentPolicy::Proactive));
    assert_eq!(merged.multi_agent.strategy.as_deref(), Some("single"));
    assert_eq!(merged.multi_agent.max_active_agents, Some(2));
    assert_eq!(merged.multi_agent.max_tasks_per_run, Some(64));
}
