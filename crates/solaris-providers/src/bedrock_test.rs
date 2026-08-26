use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use solaris_types::message::{ContentBlock, Message, Role};
    use solaris_types::tool::ToolDef;

    // --- Golden body snapshots (baseline for compat-split / seam-extraction refactors) ---

    fn bedrock_test_provider() -> BedrockProvider {
        BedrockProvider::new(
            "us-east-1",
            AwsCredentials::Explicit {
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
                session_token: None,
            },
            false,
            ProviderCompat::bedrock_defaults(),
        )
    }

    #[test]
    fn shared_credentials_parser_reads_only_the_selected_static_profile() {
        let parsed = parse_shared_credentials_profile(
            "[default]\naws_access_key_id = default-key\naws_secret_access_key = default-secret\n\
             [research]\naws_access_key_id = research-key\naws_secret_access_key = research-secret\n",
            "research",
        )
        .unwrap();
        assert_eq!(
            parsed.get("aws_access_key_id").map(String::as_str),
            Some("research-key")
        );
        assert_eq!(
            parsed.get("aws_secret_access_key").map(String::as_str),
            Some("research-secret")
        );
        assert!(!parsed.contains_key("credential_process"));
    }

    #[test]
    fn default_bedrock_credentials_use_the_aws_sdk_provider_chain() {
        let profile = AwsCredentials::Profile {
            profile: "sso-profile".to_owned(),
            credentials_file: None,
        };
        assert_eq!(sdk_chain_profile(&profile), Some(Some("sso-profile")));

        let environment = AwsCredentials::Environment { credentials_file: None };
        assert_eq!(sdk_chain_profile(&environment), Some(None));

        let explicit_file = AwsCredentials::Profile {
            profile: "static".to_owned(),
            credentials_file: Some(PathBuf::from("credentials")),
        };
        assert_eq!(sdk_chain_profile(&explicit_file), None);
    }

    fn bedrock_req(messages: Vec<Message>, tools: Vec<ToolDef>) -> LlmRequest {
        LlmRequest {
            model: "test-model".to_string(),
            system: "You are a test assistant.".to_string(),
            messages,
            tools,
            max_tokens: Some(8192),
            thinking: None,
            reasoning_effort: None,
        }
    }

    fn bedrock_tools() -> Vec<ToolDef> {
        vec![ToolDef {
            name: "read".to_string(),
            description: "Read".to_string(),
            input_schema: json!({
                "type": "object",
                "properties": {"path": {"type": ["string", "null"]}},
                "additionalProperties": false
            }),
            deferred: false,
        }]
    }

    macro_rules! assert_bedrock_json_snapshot {
        ($name:literal, $value:expr) => {
            insta::with_settings!({ prepend_module_to_snapshot => false }, {
                insta::assert_json_snapshot!(
                    concat!("solaris_providers__bedrock__tests__", $name),
                    crate::test_support::canonicalize_json($value)
                );
            });
        };
    }

    #[test]
    fn golden_bedrock_basic() {
        let p = bedrock_test_provider();
        let r = bedrock_req(
            vec![Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "Hello".to_string(),
                }],
            )],
            vec![],
        );
        assert_bedrock_json_snapshot!(
            "bedrock_basic",
            p.build_request_body(&r)
                .expect("request body projection should succeed")
        );
    }

    #[test]
    fn golden_bedrock_with_tools() {
        let p = bedrock_test_provider();
        let r = bedrock_req(
            vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: "go".to_string() }],
            )],
            bedrock_tools(),
        );
        assert_bedrock_json_snapshot!(
            "bedrock_with_tools",
            p.build_request_body(&r)
                .expect("request body projection should succeed")
        );
    }
}
