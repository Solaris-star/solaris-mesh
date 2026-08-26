use super::*;
use solaris_types::workflow::MultiAgentPolicy;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_context_ready_deserializes() {
        let command: ProtocolCommand = serde_json::from_str(r#"{"type":"host_context_ready"}"#).unwrap();
        assert_eq!(command, ProtocolCommand::HostContextReady);
    }

    #[test]
    fn get_plan_artifacts_deserializes_with_optional_run() {
        let current: ProtocolCommand =
            serde_json::from_str(r#"{"type":"get_plan_artifacts","request_id":"req-1"}"#).unwrap();
        assert_eq!(
            current,
            ProtocolCommand::GetPlanArtifacts {
                request_id: "req-1".to_owned(),
                run_id: None,
            }
        );

        let selected: ProtocolCommand =
            serde_json::from_str(r#"{"type":"get_plan_artifacts","request_id":"req-2","run_id":"run-2"}"#).unwrap();
        assert_eq!(
            selected,
            ProtocolCommand::GetPlanArtifacts {
                request_id: "req-2".to_owned(),
                run_id: Some("run-2".to_owned()),
            }
        );
    }

    #[test]
    fn set_config_debug_format() {
        let cmd = ProtocolCommand::SetConfig {
            request_id: None,
            update: RuntimeConfigUpdate {
                model: Some("test-model".into()),
                ..RuntimeConfigUpdate::default()
            },
        };
        let dbg = format!("{cmd:?}");
        assert!(dbg.contains("SetConfig"));
        assert!(dbg.contains("test-model"));
    }

    #[test]
    fn set_config_equality() {
        let a = ProtocolCommand::SetConfig {
            request_id: None,
            update: RuntimeConfigUpdate {
                model: Some("m".into()),
                ..RuntimeConfigUpdate::default()
            },
        };
        let b = ProtocolCommand::SetConfig {
            request_id: None,
            update: RuntimeConfigUpdate {
                model: Some("m".into()),
                ..RuntimeConfigUpdate::default()
            },
        };
        assert_eq!(a, b);

        let c = ProtocolCommand::SetConfig {
            request_id: None,
            update: RuntimeConfigUpdate::default(),
        };
        assert_ne!(a, c);
    }

    #[test]
    fn set_config_with_all_fields_equality() {
        let a = ProtocolCommand::SetConfig {
            request_id: None,
            update: RuntimeConfigUpdate {
                model: Some("m".into()),
                thinking: Some("enabled".into()),
                thinking_budget: Some(8000),
                effort: Some("high".into()),
                multi_agent_policy: Some(MultiAgentPolicy::Proactive),
                max_active_agents: Some(4),
                ..RuntimeConfigUpdate::default()
            },
        };
        let b = ProtocolCommand::SetConfig {
            request_id: None,
            update: RuntimeConfigUpdate {
                model: Some("m".into()),
                thinking: Some("enabled".into()),
                thinking_budget: Some(8000),
                effort: Some("high".into()),
                multi_agent_policy: Some(MultiAgentPolicy::Proactive),
                max_active_agents: Some(4),
                ..RuntimeConfigUpdate::default()
            },
        };
        assert_eq!(a, b);
    }

    #[test]
    fn set_config_all_none_fields() {
        let cmd = ProtocolCommand::SetConfig {
            request_id: None,
            update: RuntimeConfigUpdate::default(),
        };
        let dbg = format!("{cmd:?}");
        assert!(dbg.contains("SetConfig"));
    }

    #[test]
    fn set_config_with_compaction() {
        let json = r#"{"type":"set_config","compaction":"full"}"#;
        let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
        match cmd {
            ProtocolCommand::SetConfig { update, .. } => {
                assert_eq!(update.compaction.unwrap(), "full");
            }
            _ => panic!("expected SetConfig"),
        }
    }

    #[test]
    fn set_config_compaction_none_by_default() {
        let json = r#"{"type":"set_config","model":"test"}"#;
        let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
        match cmd {
            ProtocolCommand::SetConfig { update, .. } => {
                assert!(update.compaction.is_none());
            }
            _ => panic!("expected SetConfig"),
        }
    }

    #[test]
    fn set_config_accepts_typed_multi_agent_policy() {
        let command: ProtocolCommand =
            serde_json::from_str(r#"{"type":"set_config","multi_agent_policy":"disabled"}"#).unwrap();

        match command {
            ProtocolCommand::SetConfig { update, .. } => {
                assert_eq!(update.multi_agent_policy, Some(MultiAgentPolicy::Disabled))
            }
            _ => panic!("expected SetConfig"),
        }
    }

    #[test]
    fn set_config_accepts_typed_max_active_agents() {
        let command: ProtocolCommand = serde_json::from_str(r#"{"type":"set_config","max_active_agents":4}"#).unwrap();

        match command {
            ProtocolCommand::SetConfig { update, .. } => {
                assert_eq!(update.max_active_agents, Some(4));
            }
            _ => panic!("expected SetConfig"),
        }
    }

    #[test]
    fn add_mcp_server_stdio_deserialize() {
        let json = r#"{
            "type": "add_mcp_server",
            "name": "team-tools",
            "transport": "stdio",
            "command": "node",
            "args": ["bridge.js", "--port", "9000"],
            "env": {"TOKEN": "abc123"}
        }"#;
        let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
        match cmd {
            ProtocolCommand::AddMcpServer {
                name,
                transport,
                command,
                args,
                env,
                url,
                headers,
                network,
            } => {
                assert_eq!(name, "team-tools");
                assert_eq!(transport, "stdio");
                assert_eq!(command.unwrap(), "node");
                assert_eq!(args.unwrap(), vec!["bridge.js", "--port", "9000"]);
                assert_eq!(env.unwrap().get("TOKEN").unwrap(), "abc123");
                assert!(url.is_none());
                assert!(headers.is_none());
                assert!(network.network_domains.is_empty());
            }
            _ => panic!("expected AddMcpServer"),
        }
    }

    #[test]
    fn add_mcp_server_network_domains_deserialize() {
        let command: ProtocolCommand = serde_json::from_str(
            r#"{
                "type": "add_mcp_server",
                "name": "network-tools",
                "transport": "stdio",
                "command": "node",
                "network": {"network_domains": ["api.example.test:443"]}
            }"#,
        )
        .unwrap();

        match command {
            ProtocolCommand::AddMcpServer { network, .. } => {
                assert_eq!(network.network_domains, ["api.example.test:443"]);
            }
            _ => panic!("expected AddMcpServer"),
        }
    }

    #[test]
    fn ping_deserialize() {
        let json = r#"{"type":"ping"}"#;
        let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
        assert_eq!(cmd, ProtocolCommand::Ping);
    }

    #[test]
    fn add_mcp_server_sse_deserialize() {
        let json = r#"{
            "type": "add_mcp_server",
            "name": "remote-tools",
            "transport": "sse",
            "url": "http://localhost:8080/sse",
            "headers": {"Authorization": "Bearer tok"}
        }"#;
        let cmd: ProtocolCommand = serde_json::from_str(json).unwrap();
        match cmd {
            ProtocolCommand::AddMcpServer {
                name,
                transport,
                command,
                url,
                headers,
                ..
            } => {
                assert_eq!(name, "remote-tools");
                assert_eq!(transport, "sse");
                assert!(command.is_none());
                assert_eq!(url.unwrap(), "http://localhost:8080/sse");
                assert_eq!(headers.unwrap().get("Authorization").unwrap(), "Bearer tok");
            }
            _ => panic!("expected AddMcpServer"),
        }
    }

    #[test]
    fn plugin_lifecycle_commands_deserialize() {
        let install: ProtocolCommand = serde_json::from_str(
            r#"{"type":"install_plugin","request_id":"i1","manifest_path":".solaris/plugins/demo/plugin.json"}"#,
        )
        .unwrap();
        assert_eq!(
            install,
            ProtocolCommand::InstallPlugin {
                request_id: "i1".into(),
                manifest_path: ".solaris/plugins/demo/plugin.json".into(),
            }
        );

        let activate: ProtocolCommand =
            serde_json::from_str(r#"{"type":"activate_plugin","request_id":"a1","plugin_id":"demo"}"#).unwrap();
        assert_eq!(
            activate,
            ProtocolCommand::ActivatePlugin {
                request_id: "a1".into(),
                plugin_id: "demo".into(),
            }
        );

        let deactivate: ProtocolCommand =
            serde_json::from_str(r#"{"type":"deactivate_plugin","request_id":"d1","plugin_id":"demo"}"#).unwrap();
        assert_eq!(
            deactivate,
            ProtocolCommand::DeactivatePlugin {
                request_id: "d1".into(),
                plugin_id: "demo".into(),
            }
        );
    }

    #[test]
    fn runtime_journal_command_defaults_and_page_fields() {
        let defaulted: ProtocolCommand =
            serde_json::from_str(r#"{"type":"get_runtime_journal","request_id":"j1"}"#).unwrap();
        assert_eq!(
            defaulted,
            ProtocolCommand::GetRuntimeJournal {
                request_id: "j1".into(),
                run_id: None,
                after_sequence: 0,
                limit: None,
            }
        );

        let paged: ProtocolCommand =
            serde_json::from_str(r#"{"type":"get_runtime_journal","request_id":"j2","run_id":"root:workflow:w1","after_sequence":41,"limit":250}"#)
                .unwrap();
        assert_eq!(
            paged,
            ProtocolCommand::GetRuntimeJournal {
                request_id: "j2".into(),
                run_id: Some("root:workflow:w1".into()),
                after_sequence: 41,
                limit: Some(250),
            }
        );
    }
}
