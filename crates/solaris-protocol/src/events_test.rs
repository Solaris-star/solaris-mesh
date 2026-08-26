use super::*;

#[cfg(test)]
mod tests {
    use solaris_compact::CompactLevel;
    use solaris_types::config::{ConfigField, ConfigFieldResult, ConfigFieldStatus};
    use solaris_types::llm::ThinkingConfig;
    use solaris_types::permission::PermissionMode;
    use solaris_types::run_preset::Intensity;

    use super::*;
    use serde_json::json;

    #[test]
    fn test_ready_event_serialization() {
        let event = ProtocolEvent::Ready {
            version: "0.1.0".to_string(),
            session_id: Some("abc123".to_string()),
            resumed: false,
            capabilities: Capabilities {
                tool_approval: true,
                thinking: true,
                effort: false,
                effort_levels: vec![],
                modes: vec!["default".into(), "auto_edit".into(), "yolo".into()],
                current_mode: "default".into(),
                mcp: false,
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "ready");
        assert_eq!(json["version"], "0.1.0");
        assert_eq!(json["session_id"], "abc123");
        assert_eq!(json["capabilities"]["tool_approval"], true);

        // session_id omitted when None
        let event_no_sid = ProtocolEvent::Ready {
            version: "0.1.0".to_string(),
            session_id: None,
            resumed: false,
            capabilities: Capabilities {
                tool_approval: true,
                thinking: true,
                effort: false,
                effort_levels: vec![],
                modes: vec!["default".into(), "auto_edit".into(), "yolo".into()],
                current_mode: "default".into(),
                mcp: false,
            },
        };
        let json2 = serde_json::to_value(&event_no_sid).unwrap();
        assert!(json2.get("session_id").is_none());
    }

    #[test]
    fn test_text_delta_event_serialization() {
        let event = ProtocolEvent::TextDelta {
            text: "hello".to_string(),
            msg_id: "m1".to_string(),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "text_delta");
        assert_eq!(json["text"], "hello");
        assert_eq!(json["msg_id"], "m1");
    }

    #[test]
    fn test_tool_request_event_serialization() {
        let event = ProtocolEvent::ToolRequest {
            msg_id: "m1".to_string(),
            call_id: "c1".to_string(),
            run_id: Some("run-1".into()),
            agent_id: Some("child-1".into()),
            operation_id: Some("operation-1".into()),
            effect_id: Some("effect-1".into()),
            tool: ToolInfo {
                name: "ExecCommand".to_string(),
                category: ToolCategory::Exec,
                args: json!({"cmd": "ls"}),
                effect: None,
                description: "Execute: ls".to_string(),
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "tool_request");
        assert_eq!(json["tool"]["category"], "exec");
        assert_eq!(json["run_id"], "run-1");
        assert_eq!(json["agent_id"], "child-1");
        assert_eq!(json["operation_id"], "operation-1");
        assert_eq!(json["effect_id"], "effect-1");
    }

    #[test]
    fn test_tool_result_event_serialization() {
        let event = ProtocolEvent::ToolResult {
            msg_id: "m1".to_string(),
            call_id: "c1".to_string(),
            tool_name: "Read".to_string(),
            status: ToolStatus::Executed,
            output: "file content".to_string(),
            output_type: OutputType::Text,
            metadata: None,
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "tool_result");
        assert_eq!(json["status"], "executed");
        assert!(json.get("metadata").is_none());
    }

    #[test]
    fn denied_tool_result_serializes_typed_sandbox_metadata() {
        use solaris_types::sandbox::{SandboxBackend, SandboxEnforcement, SandboxReason, SandboxReport};
        use solaris_types::tool::ToolResultMetadata;

        let event = ProtocolEvent::ToolResult {
            msg_id: "m1".into(),
            call_id: "c1".into(),
            tool_name: "ExecCommand".into(),
            status: ToolStatus::Denied,
            output: "strict sandbox unavailable".into(),
            output_type: OutputType::Text,
            metadata: Some(ToolResultMetadata::sandbox_report(SandboxReport::new(
                SandboxEnforcement::Unavailable,
                SandboxBackend::WindowsAppContainer,
                SandboxReason::NetworkProxyUnavailable,
            ))),
        };

        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["status"], "denied");
        assert_eq!(value["metadata"]["sandbox_report"]["backend"], "windows_app_container");
        assert_eq!(value["metadata"]["sandbox_report"]["enforcement"], "unavailable");
        assert_eq!(
            value["metadata"]["sandbox_report"]["reason"],
            "network_proxy_unavailable"
        );
    }

    #[test]
    fn tool_status_reads_legacy_success_and_error() {
        let success: ToolStatus = serde_json::from_value(json!("success")).unwrap();
        let error: ToolStatus = serde_json::from_value(json!("error")).unwrap();

        assert_eq!(success, ToolStatus::Executed);
        assert_eq!(error, ToolStatus::Failed);
    }

    #[test]
    fn tool_cancelled_serializes_its_terminal_status() {
        let event = ProtocolEvent::ToolCancelled {
            msg_id: "m1".into(),
            call_id: "c1".into(),
            status: ToolStatus::Denied,
            reason: "permission denied".into(),
        };

        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["type"], "tool_cancelled");
        assert_eq!(value["status"], "denied");
    }

    #[test]
    fn test_error_event_serialization() {
        let event = ProtocolEvent::Error {
            msg_id: None,
            error: ErrorInfo {
                code: "rate_limit".to_string(),
                message: "Too many requests".to_string(),
                retryable: true,
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "error");
        assert!(json.get("msg_id").is_none());
        assert_eq!(json["error"]["retryable"], true);
    }

    #[test]
    fn test_stream_end_with_usage() {
        let event = ProtocolEvent::StreamEnd {
            msg_id: "m1".to_string(),
            usage: Some(Usage {
                input_tokens: 100,
                output_tokens: 50,
                uncached_input_tokens: Some(80),
                cache_read_tokens: Some(20),
                cache_write_tokens: None,
                cost_usd: Some(0.001),
            }),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "stream_end");
        assert_eq!(json["usage"]["input_tokens"], 100);
        assert_eq!(json["usage"]["uncached_input_tokens"], 80);
        assert_eq!(json["usage"]["cost_usd"], 0.001);
        assert!(json["usage"].get("cache_write_tokens").is_none());
    }

    #[test]
    fn test_tool_category_display() {
        assert_eq!(ToolCategory::Info.to_string(), "info");
        assert_eq!(ToolCategory::Edit.to_string(), "edit");
        assert_eq!(ToolCategory::Exec.to_string(), "exec");
        assert_eq!(ToolCategory::Mcp.to_string(), "mcp");
    }

    #[test]
    fn test_ready_event_with_expanded_capabilities() {
        let event = ProtocolEvent::Ready {
            version: "0.2.0".to_string(),
            session_id: Some("abc".to_string()),
            resumed: false,
            capabilities: Capabilities {
                tool_approval: true,
                thinking: true,
                effort: true,
                effort_levels: vec!["low".into(), "medium".into(), "high".into()],
                modes: vec!["default".into(), "auto_edit".into(), "yolo".into()],
                current_mode: "default".into(),
                mcp: false,
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["capabilities"]["thinking"], true);
        assert_eq!(json["capabilities"]["effort"], true);
        assert_eq!(json["capabilities"]["effort_levels"][0], "low");
        assert_eq!(json["capabilities"]["modes"][2], "yolo");
    }

    #[test]
    fn test_mcp_ready_event_serialization() {
        let event = ProtocolEvent::McpReady {
            name: "team-tools".to_string(),
            tools: vec!["team_send_message".into(), "team_task_create".into()],
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "mcp_ready");
        assert_eq!(json["name"], "team-tools");
        assert_eq!(json["tools"][0], "team_send_message");
        assert_eq!(json["tools"][1], "team_task_create");
        assert_eq!(json["tools"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_pong_event_serialization() {
        let event = ProtocolEvent::Pong;
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "pong");
        assert_eq!(json.as_object().unwrap().len(), 1);
    }

    #[test]
    fn test_config_changed_event_serialization() {
        let event = ProtocolEvent::ConfigChanged {
            capabilities: Capabilities {
                tool_approval: true,
                thinking: false,
                effort: true,
                effort_levels: vec!["low".into(), "medium".into(), "high".into()],
                modes: vec!["default".into(), "auto_edit".into(), "yolo".into()],
                current_mode: "default".into(),
                mcp: true,
            },
            configuration: RuntimeConfiguration {
                provider: "custom-openai".into(),
                model: "deepseek-v4-flash".into(),
                permission: PermissionMode::Auto,
                selected_intensity: Intensity::Extra,
                multi_agent_policy: MultiAgentPolicy::Proactive,
                max_active_agents: Some(4),
                effective_max_active_agents: 4,
                effective_effort: Some("high".into()),
                thinking: Some(ThinkingConfig::Enabled { budget_tokens: 4_096 }),
                thinking_budget: Some(4_096),
                compaction: CompactLevel::Full,
            },
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["type"], "config_changed");
        assert_eq!(json["capabilities"]["thinking"], false);
        assert_eq!(json["capabilities"]["effort"], true);
        assert_eq!(json["configuration"]["provider"], "custom-openai");
        assert_eq!(json["configuration"]["model"], "deepseek-v4-flash");
        assert_eq!(json["configuration"]["permission"], "auto");
        assert_eq!(json["configuration"]["selected_intensity"], "extra");
        assert_eq!(json["configuration"]["effective_effort"], "high");
        assert_eq!(json["configuration"]["thinking"]["type"], "enabled");
        assert_eq!(json["configuration"]["thinking_budget"], 4_096);
        assert_eq!(json["configuration"]["compaction"], "full");
        assert_eq!(json["configuration"]["max_active_agents"], 4);
        assert_eq!(json["configuration"]["effective_max_active_agents"], 4);
    }

    #[test]
    fn runtime_snapshot_keeps_legacy_intensity_beside_typed_configuration() {
        let configuration = RuntimeConfiguration {
            provider: "provider-a".into(),
            model: "model-a".into(),
            permission: PermissionMode::Plan,
            selected_intensity: Intensity::Ultracode,
            multi_agent_policy: MultiAgentPolicy::Proactive,
            max_active_agents: Some(4),
            effective_max_active_agents: 4,
            effective_effort: None,
            thinking: None,
            thinking_budget: None,
            compaction: CompactLevel::Safe,
        };
        let event = ProtocolEvent::RuntimeSnapshot {
            request_id: "snapshot-1".into(),
            schema_version: 1,
            timestamp_unix_ms: 10,
            live_sequence: 2,
            journal_sequence: 1,
            run_id: "run-1".into(),
            snapshot: RuntimeSnapshotPayload::new(
                configuration.clone(),
                serde_json::json!({"intensity": configuration.selected_intensity}),
            ),
        };

        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["snapshot"]["intensity"], "ultracode");
        assert_eq!(json["snapshot"]["configuration"]["selected_intensity"], "ultracode");
        assert!(json["snapshot"]["configuration"]["effective_effort"].is_null());
    }

    #[test]
    fn runtime_snapshot_payload_rejects_an_untyped_configuration_extension() {
        let configuration = RuntimeConfiguration {
            provider: "provider-a".into(),
            model: "model-a".into(),
            permission: PermissionMode::Auto,
            selected_intensity: Intensity::High,
            multi_agent_policy: MultiAgentPolicy::Proactive,
            max_active_agents: Some(4),
            effective_max_active_agents: 4,
            effective_effort: Some("high".into()),
            thinking: None,
            thinking_budget: None,
            compaction: CompactLevel::Safe,
        };
        let payload = RuntimeSnapshotPayload::new(
            configuration,
            serde_json::json!({
                "configuration": {"max_active_agents": "not-a-number"},
                "runtime": {},
            }),
        );

        let json = serde_json::to_value(payload).unwrap();
        assert_eq!(json["configuration"]["max_active_agents"], 4);
        assert_eq!(json["configuration"]["effective_max_active_agents"], 4);
        assert!(json["runtime"].is_object());
    }

    #[test]
    fn command_result_serializes_request_correlation() {
        let event = ProtocolEvent::CommandResult {
            request_id: "studio-command-1".into(),
            command: "set_mode".into(),
            applied: true,
            message: None,
            config_results: None,
        };
        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["type"], "command_result");
        assert_eq!(json["request_id"], "studio-command-1");
        assert_eq!(json["applied"], true);
        assert!(json.get("message").is_none());
        assert!(json.get("config_results").is_none());
    }

    #[test]
    fn command_result_serializes_typed_config_results() {
        let event = ProtocolEvent::CommandResult {
            request_id: "studio-config-1".into(),
            command: "set_config".into(),
            applied: false,
            message: Some("configuration rejected".into()),
            config_results: Some(vec![ConfigFieldResult::new(
                ConfigField::Effort,
                ConfigFieldStatus::Unsupported,
                "current provider does not support effort",
            )]),
        };

        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["type"], "command_result");
        assert_eq!(json["applied"], false);
        assert_eq!(json["config_results"][0]["field"], "effort");
        assert_eq!(json["config_results"][0]["status"], "unsupported");
    }

    #[test]
    fn runtime_journal_and_watermark_serialize() {
        let live = ProtocolEvent::RuntimeEvent {
            schema_version: 1,
            kind: "team_fact_set".into(),
            sequence: 9,
            timestamp_unix_ms: 123,
            journal_sequence: Some(42),
            run_id: "run".into(),
            payload: json!({"key":"answer"}),
        };
        let live = serde_json::to_value(live).unwrap();
        assert_eq!(live["sequence"], 9);
        assert_eq!(live["journal_sequence"], 42);
        assert_eq!(live["timestamp_unix_ms"], 123);

        let journal = ProtocolEvent::RuntimeJournal {
            request_id: "j1".into(),
            run_id: "run".into(),
            after_sequence: 41,
            last_sequence: 42,
            records: vec![RuntimeJournalRecord {
                schema_version: 1,
                sequence: 42,
                run_id: "run".into(),
                timestamp_unix_ms: 123,
                durability: solaris_types::effect::DurabilityClass::SyncCritical,
                record_type: "team_fact_set".into(),
                payload: json!({"key":"answer"}),
            }],
            truncated: false,
        };
        let journal = serde_json::to_value(journal).unwrap();
        assert_eq!(journal["type"], "runtime_journal");
        assert_eq!(journal["records"][0]["sequence"], 42);
        assert_eq!(journal["last_sequence"], 42);
    }

    #[test]
    fn plan_artifacts_event_serializes_revision_and_references() {
        let event = ProtocolEvent::PlanArtifacts {
            request_id: "plans-1".into(),
            run_id: "run-1".into(),
            artifacts: vec![solaris_types::plan::PlanArtifact {
                id: "plan:v1:abc".into(),
                revision: 3,
                markdown: "# Plan".into(),
                digest: solaris_types::plan::PlanArtifact::markdown_digest("# Plan"),
                run_id: solaris_types::identity::RunId::new("run-1"),
                msg_id: "msg-1".into(),
                created_at_unix_ms: 10,
                updated_at_unix_ms: 20,
            }],
        };

        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["type"], "plan_artifacts");
        assert_eq!(value["artifacts"][0]["revision"], 3);
        assert_eq!(value["artifacts"][0]["run_id"], "run-1");
        assert_eq!(value["artifacts"][0]["msg_id"], "msg-1");
    }

    #[test]
    fn runtime_configuration_serializes_typed_turn_configuration() {
        let configuration = RuntimeConfiguration {
            provider: "provider-a".into(),
            model: "model-a".into(),
            permission: PermissionMode::Auto,
            selected_intensity: Intensity::High,
            multi_agent_policy: MultiAgentPolicy::OnDemand,
            max_active_agents: None,
            effective_max_active_agents: 8,
            effective_effort: None,
            thinking: Some(ThinkingConfig::Enabled { budget_tokens: 16_000 }),
            thinking_budget: Some(16_000),
            compaction: CompactLevel::Full,
        };

        let value = serde_json::to_value(configuration).unwrap();
        assert_eq!(value["thinking"]["type"], "enabled");
        assert_eq!(value["thinking_budget"], 16_000);
        assert_eq!(value["compaction"], "full");
    }

    #[test]
    fn legacy_runtime_configuration_uses_safe_defaults_for_new_fields() {
        let configuration: RuntimeConfiguration = serde_json::from_value(json!({
            "provider": "provider-a",
            "model": "model-a",
            "permission": "auto",
            "selected_intensity": "high",
            "effective_effort": null
        }))
        .unwrap();

        assert!(configuration.thinking.is_none());
        assert!(configuration.thinking_budget.is_none());
        assert_eq!(configuration.compaction, CompactLevel::Safe);
        assert_eq!(configuration.multi_agent_policy, MultiAgentPolicy::OnDemand);
        assert_eq!(configuration.max_active_agents, None);
        assert_eq!(configuration.effective_max_active_agents, 1);
    }
}
