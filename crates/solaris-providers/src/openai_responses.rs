use async_trait::async_trait;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use solaris_config::compat::ProviderCompat;
use solaris_types::llm::{LlmEvent, LlmRequest};
use solaris_types::message::{ContentBlock, Role, StopReason, TokenUsage};

use crate::error::ProviderError;
use crate::framing::SseBlockFramer;
use crate::provider::LlmProvider;
use crate::stream_runner::{RetryPolicy, StreamOutcome, run_stream};
use crate::transport::redirect_safe_client_for_url;

const REASONING_SIGNATURE_PREFIX: &str = "openai-responses:";

#[derive(Clone)]
pub struct OpenAIResponsesProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    compat: ProviderCompat,
    retries_enabled: bool,
}

impl OpenAIResponsesProvider {
    pub fn new(api_key: &str, base_url: &str, compat: ProviderCompat) -> Self {
        Self {
            client: redirect_safe_client_for_url(base_url),
            api_key: api_key.to_owned(),
            base_url: normalize_base_url(base_url),
            compat,
            retries_enabled: true,
        }
    }

    pub fn with_retries_enabled(mut self, enabled: bool) -> Self {
        self.retries_enabled = enabled;
        self
    }

    fn project(&self, request: &LlmRequest) -> Result<Value, ProviderError> {
        project_responses_request(request, &self.compat)
    }

    async fn send(&self, body: Value) -> Result<reqwest::Response, ProviderError> {
        let mut headers = HeaderMap::new();
        let bearer = HeaderValue::from_str(&format!("Bearer {}", self.api_key))
            .map_err(|error| ProviderError::Connection(format!("Invalid authorization header: {error}")))?;
        headers.insert(AUTHORIZATION, bearer);
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response = self
            .client
            .post(format!("{}/responses", self.base_url.trim_end_matches('/')))
            .headers(headers)
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let body = response.text().await.unwrap_or_default();
        if status.as_u16() == 429 {
            return Err(ProviderError::RateLimited {
                retry_after_ms: 5_000,
                body: (!body.is_empty()).then_some(body),
            });
        }
        Err(ProviderError::Api {
            status: status.as_u16(),
            message: body,
        })
    }

    async fn stream_with_retries(
        &self,
        request: &LlmRequest,
        retries_enabled: bool,
    ) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        let body = self.project(request)?;
        let provider = self.clone();
        let send = move || {
            let provider = provider.clone();
            let body = body.clone();
            async move { provider.send(body).await }
        };
        run_stream(
            send,
            |response, tx| async move { process_responses_stream(response, &tx).await },
            if retries_enabled && self.retries_enabled {
                RetryPolicy::new(2, true, true, true)
            } else {
                RetryPolicy::single_attempt()
            },
        )
        .await
    }
}

#[async_trait]
impl LlmProvider for OpenAIResponsesProvider {
    async fn stream(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.stream_with_retries(request, true).await
    }

    async fn stream_once(&self, request: &LlmRequest) -> Result<mpsc::Receiver<LlmEvent>, ProviderError> {
        self.stream_with_retries(request, false).await
    }
}

fn project_responses_request(request: &LlmRequest, compat: &ProviderCompat) -> Result<Value, ProviderError> {
    if let Some(max_tools) = compat.max_tool_count()
        && request.tools.len() > max_tools
    {
        return Err(ProviderError::PromptTooLong(format!(
            "openai-responses tools count {} exceeds configured limit {max_tools}",
            request.tools.len()
        )));
    }

    let mut input = Vec::new();
    for message in &request.messages {
        match message.role {
            Role::System => {}
            Role::User | Role::Tool => {
                let mut text = Vec::new();
                for block in &message.content {
                    match block {
                        ContentBlock::Text { text: value } => text.push(value.clone()),
                        ContentBlock::ToolResult {
                            tool_use_id, content, ..
                        } => input.push(json!({
                            "type": "function_call_output",
                            "call_id": tool_use_id,
                            "output": content,
                        })),
                        _ => {}
                    }
                }
                if !text.is_empty() {
                    input.push(json!({
                        "type": "message",
                        "role": "user",
                        "content": text.join("\n"),
                    }));
                }
            }
            Role::Assistant => {
                let mut text = Vec::new();
                let openai_metadata = message.provider_metadata.get("openai");
                let mut reasoning_metadata_emitted = false;
                for block in &message.content {
                    match block {
                        ContentBlock::Text { text: value } => text.push(value.clone()),
                        ContentBlock::Thinking { signature, .. } => {
                            if !reasoning_metadata_emitted
                                && let Some(items) = openai_metadata
                                    .and_then(|value| value.get("reasoning_items"))
                                    .and_then(Value::as_array)
                            {
                                input.extend(items.iter().cloned());
                                reasoning_metadata_emitted = true;
                            } else if let Some(signature) = signature
                                && signature.starts_with(REASONING_SIGNATURE_PREFIX)
                            {
                                let raw = &signature[REASONING_SIGNATURE_PREFIX.len()..];
                                if let Ok(item) = serde_json::from_str::<Value>(raw) {
                                    input.push(item);
                                }
                            }
                        }
                        ContentBlock::ToolUse {
                            id,
                            name,
                            input: arguments,
                            extra,
                        } => {
                            let mut item = json!({
                                "type": "function_call",
                                "call_id": id,
                                "name": name,
                                "arguments": serde_json::to_string(arguments).unwrap_or_else(|_| "{}".into()),
                            });
                            if let Some(raw) = openai_metadata
                                .and_then(|value| value.get("tool_calls"))
                                .and_then(|value| value.get(id))
                                .and_then(|value| value.get("id"))
                                .cloned()
                                .or_else(|| {
                                    extra
                                        .as_ref()
                                        .and_then(|value| value.get("openai_responses"))
                                        .and_then(Value::as_object)
                                        .and_then(|value| value.get("id"))
                                        .cloned()
                                })
                            {
                                item["id"] = raw;
                            }
                            input.push(item);
                        }
                        _ => {}
                    }
                }
                if !text.is_empty() {
                    input.push(json!({
                        "type": "message",
                        "role": "assistant",
                        "content": text.join(""),
                    }));
                }
            }
        }
    }

    let tools: Vec<Value> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.input_schema,
                "strict": false,
            })
        })
        .collect();

    let mut body = json!({
        "model": request.model,
        "instructions": request.system,
        "input": input,
        "stream": true,
        "store": false,
    });
    if !tools.is_empty() && compat.emit_tools() {
        body["tools"] = json!(tools);
    }
    if let Some(max_tokens) = request
        .max_tokens
        .or_else(|| compat.default_max_tokens_for_model(&request.model))
    {
        body["max_output_tokens"] = json!(max_tokens);
    }
    if let Some(effort) = &request.reasoning_effort
        && compat.supports_effort()
    {
        body["reasoning"] = json!({"effort": effort});
    }

    if let Some(max_bytes) = compat.max_request_body_bytes() {
        let bytes = serde_json::to_vec(&body)
            .map_err(|error| ProviderError::Parse(format!("serialize Responses request: {error}")))?
            .len();
        if bytes > max_bytes {
            return Err(ProviderError::PromptTooLong(format!(
                "openai-responses request body is {bytes} bytes, exceeding configured limit {max_bytes}"
            )));
        }
    }
    Ok(body)
}

async fn process_responses_stream(response: reqwest::Response, tx: &mpsc::Sender<LlmEvent>) -> StreamOutcome {
    let mut framer = SseBlockFramer::default();
    let mut stream = response.bytes_stream();
    let mut emitted_content = false;
    let mut saw_tool_call = false;

    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(value) => value,
            Err(error) => {
                let error = ProviderError::Connection(error.to_string());
                return if emitted_content {
                    StreamOutcome::FailedPartial(error)
                } else {
                    StreamOutcome::FailedEmpty(error)
                };
            }
        };
        let text = String::from_utf8_lossy(&chunk);
        for frame in framer.push_text(&text) {
            let Ok(event) = serde_json::from_str::<Value>(&frame.data) else {
                continue;
            };
            let event_type = event.get("type").and_then(Value::as_str).unwrap_or_default();
            let mapped = match event_type {
                "response.output_text.delta" => event
                    .get("delta")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(|value| LlmEvent::TextDelta(value.to_owned()))
                    .into_iter()
                    .collect(),
                "response.reasoning_text.delta" | "response.reasoning_summary_text.delta" => event
                    .get("delta")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(|value| LlmEvent::ThinkingDelta(value.to_owned()))
                    .into_iter()
                    .collect(),
                "response.output_item.done" => map_output_item_done(&event, &mut saw_tool_call),
                "response.completed" => vec![LlmEvent::Done {
                    stop_reason: if saw_tool_call {
                        StopReason::ToolUse
                    } else {
                        StopReason::EndTurn
                    },
                    usage: usage_from_response(event.get("response")),
                }],
                "response.incomplete" => vec![LlmEvent::Done {
                    stop_reason: StopReason::MaxTokens,
                    usage: usage_from_response(event.get("response")),
                }],
                "response.failed" | "error" => vec![LlmEvent::Error(
                    event
                        .pointer("/response/error/message")
                        .or_else(|| event.get("message"))
                        .and_then(Value::as_str)
                        .unwrap_or("OpenAI Responses stream failed")
                        .to_owned(),
                )],
                _ => Vec::new(),
            };
            for mapped in mapped {
                if matches!(
                    mapped,
                    LlmEvent::TextDelta(_)
                        | LlmEvent::ThinkingDelta(_)
                        | LlmEvent::ProviderMetadata { .. }
                        | LlmEvent::ToolUse { .. }
                ) {
                    emitted_content = true;
                }
                if tx.send(mapped).await.is_err() {
                    return StreamOutcome::Ok;
                }
            }
        }
    }
    StreamOutcome::Ok
}

fn map_output_item_done(event: &Value, saw_tool_call: &mut bool) -> Vec<LlmEvent> {
    let Some(item) = event.get("item") else {
        return Vec::new();
    };
    match item.get("type").and_then(Value::as_str).unwrap_or_default() {
        "function_call" => {
            *saw_tool_call = true;
            let Some(call_id) = item.get("call_id").and_then(Value::as_str).map(str::to_owned) else {
                return Vec::new();
            };
            let Some(name) = item.get("name").and_then(Value::as_str).map(str::to_owned) else {
                return Vec::new();
            };
            let arguments = item.get("arguments").and_then(Value::as_str).unwrap_or("{}");
            let input = serde_json::from_str(arguments).unwrap_or_else(|_| json!({}));
            vec![
                LlmEvent::ProviderMetadata {
                    namespace: "openai".to_owned(),
                    value: json!({"tool_calls": {call_id.clone(): item.clone()}}),
                },
                LlmEvent::ToolUse {
                    id: call_id,
                    name,
                    input,
                    extra: None,
                },
            ]
        }
        "reasoning" => vec![
            LlmEvent::ThinkingSignature("openai-responses-reasoning".to_owned()),
            LlmEvent::ProviderMetadata {
                namespace: "openai".to_owned(),
                value: json!({"reasoning_items": [item.clone()]}),
            },
        ],
        _ => Vec::new(),
    }
}

fn usage_from_response(response: Option<&Value>) -> TokenUsage {
    let usage = response.and_then(|value| value.get("usage"));
    TokenUsage {
        input_tokens: usage
            .and_then(|value| value.get("input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .and_then(|value| value.get("output_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        cache_creation_tokens: 0,
        cache_read_tokens: usage
            .and_then(|value| value.pointer("/input_tokens_details/cached_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
    }
}

fn normalize_base_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if trimmed.eq_ignore_ascii_case("https://api.openai.com") || trimmed.eq_ignore_ascii_case("http://api.openai.com") {
        format!("{trimmed}/v1")
    } else {
        trimmed.to_owned()
    }
}

#[cfg(test)]
#[path = "openai_responses_test.rs"]
mod openai_responses_test;
