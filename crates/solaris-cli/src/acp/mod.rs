//! Agent Client Protocol (ACP) agent endpoint for Solaris Mesh.
//!
//! The endpoint deliberately reuses the normal CLI bootstrap. ACP sessions
//! therefore get the same provider, permission checks, durable session store,
//! collaboration runtime and tools as the terminal and JSON stream hosts.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use agent_client_protocol::schema::v1::{
    AgentCapabilities, CancelNotification, CloseSessionRequest, CloseSessionResponse, ContentBlock, ContentChunk,
    Error as AcpError, InitializeRequest, InitializeResponse, LoadSessionRequest, LoadSessionResponse,
    NewSessionRequest, NewSessionResponse, PromptCapabilities, PromptRequest, PromptResponse, SessionCapabilities,
    SessionCloseCapabilities, SessionConfigOption, SessionConfigSelectOption, SessionId, SessionNotification,
    SessionResumeCapabilities, SessionUpdate, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
    StopReason, TextContent, ToolCall, ToolCallId, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields,
};
use agent_client_protocol::{Agent, ConnectionTo, Responder, Stdio};
use solaris_agent::engine::AgentEngine;
use solaris_agent::error::AgentError;
use solaris_agent::output::OutputSink;
use solaris_agent::spawner::AgentSpawner;
use solaris_config::config::Config;
use solaris_mcp::manager::McpManager;
use solaris_types::config::RuntimeConfigUpdate;
use solaris_types::permission::PermissionMode;
use solaris_types::run_preset::Intensity;
use solaris_types::spawner::AgentOutcomeStatus;
use solaris_types::workflow::{CollaborationSelection, CollaborationStrategy, MultiAgentPolicy};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

use crate::bootstrap::build_engine;

/// Serve one ACP connection over stdin/stdout.
pub(crate) async fn run(
    mut config: Config,
    _cwd: &str,
    permission_mode: PermissionMode,
    intensity: Intensity,
) -> anyhow::Result<()> {
    // ACP sessions are durable by contract. A host may have disabled terminal
    // session persistence, but that setting must not turn session/new into an
    // in-memory conversation that cannot be resumed.
    config.session.enabled = true;
    let state = Arc::new(AcpState {
        config,
        permission_mode,
        intensity,
        sessions: Arc::new(Mutex::new(HashMap::new())),
    });

    Agent
        .builder()
        .name("solaris-mesh")
        .on_receive_request(
            async |_request: InitializeRequest, responder: Responder<InitializeResponse>, _connection| {
                let capabilities = AgentCapabilities::new()
                    .load_session(true)
                    .prompt_capabilities(PromptCapabilities::new().embedded_context(true))
                    .session_capabilities(
                        SessionCapabilities::new()
                            .resume(SessionResumeCapabilities::new())
                            .close(SessionCloseCapabilities::new()),
                    );
                responder.respond(
                    InitializeResponse::new(agent_client_protocol::schema::ProtocolVersion::V1)
                        .agent_capabilities(capabilities)
                        .agent_info(agent_client_protocol::schema::v1::Implementation::new(
                            "solaris-mesh",
                            env!("CARGO_PKG_VERSION"),
                        )),
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |request: NewSessionRequest,
                            responder: Responder<NewSessionResponse>,
                            connection: ConnectionTo<agent_client_protocol::Client>| {
                    let state = Arc::clone(&state);
                    let session_connection = connection.clone();
                    connection.spawn(async move {
                        let session_id = SessionId::new(format!("session-{}", Uuid::now_v7()));
                        let response = state
                            .create_session(session_id, request.cwd, session_connection, None)
                            .await;
                        respond_result(responder, response.map(|session| new_session_response(&session)))
                    })?;
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |request: LoadSessionRequest,
                            responder: Responder<LoadSessionResponse>,
                            connection: ConnectionTo<agent_client_protocol::Client>| {
                    let state = Arc::clone(&state);
                    let session_connection = connection.clone();
                    connection.spawn(async move {
                        let response = state
                            .create_session(
                                request.session_id,
                                request.cwd,
                                session_connection,
                                Some(LoadKind::Load),
                            )
                            .await
                            .map(|session| load_session_response(&session));
                        respond_result(responder, response)
                    })?;
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |request: PromptRequest,
                            responder: Responder<PromptResponse>,
                            connection: ConnectionTo<agent_client_protocol::Client>| {
                    let Some(session) = state.session(&request.session_id).await else {
                        return responder
                            .respond_with_error(invalid_params("unknown ACP session"))
                            .map(|_| ());
                    };
                    let prompt = match prompt_text(&request.prompt) {
                        Ok(prompt) if !prompt.trim().is_empty() => prompt,
                        Ok(_) => {
                            return responder
                                .respond_with_error(invalid_params("session/prompt requires text content"))
                                .map(|_| ());
                        }
                        Err(error) => return responder.respond_with_error(error).map(|_| ()),
                    };
                    let cancellation = responder.cancellation();
                    connection.spawn(async move {
                        let response = run_prompt(session, prompt, cancellation).await;
                        respond_result(responder, response)
                    })?;
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |request: SetSessionConfigOptionRequest,
                            responder: Responder<SetSessionConfigOptionResponse>,
                            connection: ConnectionTo<agent_client_protocol::Client>| {
                    let Some(session) = state.session(&request.session_id).await else {
                        return responder
                            .respond_with_error(invalid_params("unknown ACP session"))
                            .map(|_| ());
                    };
                    connection.spawn(async move {
                        let result = set_session_config(&session, request).await;
                        respond_result(responder, result)
                    })?;
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let state = Arc::clone(&state);
                async move |request: CloseSessionRequest,
                            responder: Responder<CloseSessionResponse>,
                            connection: ConnectionTo<agent_client_protocol::Client>| {
                    let state = Arc::clone(&state);
                    connection.spawn(async move {
                        let response = state.close_session(&request.session_id).await;
                        respond_result(responder, response.map(|_| CloseSessionResponse::new()))
                    })?;
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            {
                let state = Arc::clone(&state);
                async move |notification: CancelNotification, _connection| {
                    if let Some(session) = state.session(&notification.session_id).await {
                        session.request_cancel();
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_to(Stdio::new())
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;

    state.close_all().await;
    Ok(())
}

#[derive(Clone)]
struct AcpState {
    config: Config,
    permission_mode: PermissionMode,
    intensity: Intensity,
    sessions: Arc<Mutex<HashMap<SessionId, Arc<AcpSession>>>>,
}

#[derive(Clone, Copy)]
enum LoadKind {
    Load,
}

struct AcpSession {
    id: SessionId,
    engine: Arc<Mutex<AgentEngine>>,
    spawner: Arc<AgentSpawner>,
    mcp_managers: Vec<Arc<McpManager>>,
    cancel_requested: AtomicBool,
    cancel_notify: Notify,
}

impl AcpSession {
    fn request_cancel(&self) {
        self.cancel_requested.store(true, Ordering::Release);
        self.cancel_notify.notify_waiters();
    }

    async fn shutdown(&self) {
        self.request_cancel();
        for manager in &self.mcp_managers {
            manager.shutdown().await;
        }
    }
}

impl AcpState {
    async fn session(&self, id: &SessionId) -> Option<Arc<AcpSession>> {
        self.sessions.lock().await.get(id).cloned()
    }

    async fn create_session(
        &self,
        id: SessionId,
        cwd: PathBuf,
        connection: ConnectionTo<agent_client_protocol::Client>,
        load: Option<LoadKind>,
    ) -> Result<Arc<AcpSession>, AcpError> {
        if self.sessions.lock().await.contains_key(&id) {
            return Err(invalid_params("ACP session is already active"));
        }
        if !cwd.is_absolute() {
            return Err(invalid_params("ACP session cwd must be absolute"));
        }
        let session_name = id.to_string();

        let output_errors = Arc::new(StdMutex::new(None));
        let output: Arc<dyn OutputSink> =
            Arc::new(AcpOutputSink::new(id.clone(), connection, Arc::clone(&output_errors)));
        let result = build_engine(
            self.config.clone(),
            &cwd.to_string_lossy(),
            output,
            self.permission_mode,
            load.map(|_| session_name.as_str()),
            |_| {},
        )
        .await
        .map_err(|error| internal_error(error.to_string()))?;
        let mut engine = result.engine;
        let policy = engine.runtime_configuration_view().snapshot().multi_agent_policy;
        engine.apply_intensity(self.intensity);
        // Intensity controls model effort; an explicitly resolved multi-agent
        // policy must not be replaced by the preset's policy.
        let _ = engine.apply_runtime_config_update(RuntimeConfigUpdate {
            multi_agent_policy: Some(policy),
            ..RuntimeConfigUpdate::default()
        });
        if load.is_none() {
            engine
                .init_session(
                    &self.config.provider_label,
                    &cwd.to_string_lossy(),
                    Some(session_name.as_str()),
                )
                .map_err(|error| internal_error(error.to_string()))?;
        }
        let session = Arc::new(AcpSession {
            id: id.clone(),
            engine: Arc::new(Mutex::new(engine)),
            spawner: result.spawner,
            mcp_managers: result.mcp_managers,
            cancel_requested: AtomicBool::new(false),
            cancel_notify: Notify::new(),
        });
        self.sessions.lock().await.insert(id, Arc::clone(&session));
        Ok(session)
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), AcpError> {
        let session = self.sessions.lock().await.remove(id);
        if let Some(session) = session {
            session.shutdown().await;
            Ok(())
        } else {
            Err(invalid_params("unknown ACP session"))
        }
    }

    async fn close_all(&self) {
        let sessions: Vec<_> = self.sessions.lock().await.drain().map(|(_, session)| session).collect();
        for session in sessions {
            session.shutdown().await;
        }
    }
}

fn new_session_response(session: &AcpSession) -> NewSessionResponse {
    let options = session_config_options(session);
    NewSessionResponse::new(session.id.clone()).config_options(options)
}

fn load_session_response(session: &AcpSession) -> LoadSessionResponse {
    LoadSessionResponse::new().config_options(session_config_options(session))
}

fn session_config_options(session: &AcpSession) -> Vec<SessionConfigOption> {
    let engine = session.engine.try_lock();
    let (policy, intensity) = engine
        .as_ref()
        .map(|engine| {
            let config = engine.runtime_configuration_view().snapshot();
            (
                config.multi_agent_policy.to_string(),
                config.selected_intensity.to_string(),
            )
        })
        .unwrap_or_else(|_| {
            (
                MultiAgentPolicy::default().to_string(),
                Intensity::default().to_string(),
            )
        });
    let strategy = selection_name(&session.spawner.collaboration_strategy());
    vec![
        select_option(
            "multi_agent_policy",
            "Multi-agent policy",
            &policy,
            &["disabled", "on_demand", "proactive"],
        ),
        select_option(
            "collaboration_strategy",
            "Collaboration strategy",
            &strategy,
            &["auto", "single", "supervisor", "team", "fanout", "independent_reviewer"],
        ),
        select_option("intensity", "Execution intensity", &intensity, &Intensity::USER_LEVELS),
    ]
}

fn select_option(id: &str, name: &str, current: &str, values: &[&str]) -> SessionConfigOption {
    let choices = values
        .iter()
        .map(|value| SessionConfigSelectOption::new((*value).to_owned(), (*value).to_owned()))
        .collect::<Vec<_>>();
    SessionConfigOption::select(id.to_owned(), name.to_owned(), current.to_owned(), choices)
}

async fn set_session_config(
    session: &AcpSession,
    request: SetSessionConfigOptionRequest,
) -> Result<SetSessionConfigOptionResponse, AcpError> {
    let id = request.config_id.to_string();
    let value = request
        .value
        .as_value_id()
        .map(ToString::to_string)
        .ok_or_else(|| invalid_params("ACP configuration option requires a string value"))?;
    match id.as_str() {
        "multi_agent_policy" => {
            let policy = parse_policy(&value)?;
            let mut engine = session.engine.lock().await;
            let outcome = engine.apply_runtime_config_update(RuntimeConfigUpdate {
                multi_agent_policy: Some(policy),
                ..RuntimeConfigUpdate::default()
            });
            if !outcome.applied {
                return Err(invalid_params(outcome.message));
            }
        }
        "collaboration_strategy" => {
            session.spawner.set_collaboration_strategy(parse_strategy(&value)?);
        }
        "intensity" => {
            let intensity = value.parse::<Intensity>().map_err(invalid_params)?;
            let mut engine = session.engine.lock().await;
            let policy = engine.runtime_configuration_view().snapshot().multi_agent_policy;
            engine.apply_intensity(intensity);
            // A policy explicitly selected through ACP remains in force when
            // only the reasoning intensity changes.
            let _ = engine.apply_runtime_config_update(RuntimeConfigUpdate {
                multi_agent_policy: Some(policy),
                ..RuntimeConfigUpdate::default()
            });
        }
        _ => return Err(invalid_params(format!("unknown ACP configuration option: {id}"))),
    }
    Ok(SetSessionConfigOptionResponse::new(session_config_options(session)))
}

async fn run_prompt(
    session: Arc<AcpSession>,
    prompt: String,
    cancellation: agent_client_protocol::RequestCancellation,
) -> Result<PromptResponse, AcpError> {
    // A CancelNotification may arrive before the host's prompt task starts.
    // Consume that request before entering the engine so it cannot be lost by
    // an unconditional reset.
    if session.cancel_requested.swap(false, Ordering::AcqRel) {
        return Ok(PromptResponse::new(StopReason::Cancelled));
    }
    let cancel_wait = session.cancel_notify.notified();
    tokio::pin!(cancel_wait);
    let mut engine = session.engine.lock().await;
    let msg_id = format!("acp-{}", Uuid::now_v7());
    let outcome = {
        let run = engine.run(&prompt, &msg_id);
        tokio::pin!(run);
        tokio::select! {
            result = &mut run => PromptOutcome::Run(result),
            _ = cancellation.cancelled() => PromptOutcome::Cancelled,
            _ = &mut cancel_wait => PromptOutcome::Cancelled,
        }
    };

    match outcome {
        PromptOutcome::Cancelled => {
            engine.abort_current_turn("ACP session cancelled");
            session.cancel_requested.store(false, Ordering::Release);
            Ok(PromptResponse::new(StopReason::Cancelled))
        }
        PromptOutcome::Run(result) => {
            session.cancel_requested.store(false, Ordering::Release);
            let result = result.map_err(|error| map_agent_error(&error))?;
            Ok(PromptResponse::new(map_stop_reason(&result.status, result.stop_reason)))
        }
    }
}

enum PromptOutcome {
    Run(Result<solaris_agent::engine::AgentResult, AgentError>),
    Cancelled,
}

fn map_stop_reason(status: &AgentOutcomeStatus, reason: solaris_types::message::StopReason) -> StopReason {
    if *status == AgentOutcomeStatus::Completed {
        return match reason {
            solaris_types::message::StopReason::MaxTokens => StopReason::MaxTokens,
            solaris_types::message::StopReason::MaxTurns => StopReason::MaxTurnRequests,
            _ => StopReason::EndTurn,
        };
    }
    StopReason::Refusal
}

fn map_agent_error(error: &AgentError) -> AcpError {
    if matches!(error, AgentError::UserAborted) {
        AcpError::request_cancelled()
    } else {
        internal_error(error.to_string())
    }
}

fn prompt_text(blocks: &[ContentBlock]) -> Result<String, AcpError> {
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            ContentBlock::Text(text) => parts.push(text.text.clone()),
            ContentBlock::ResourceLink(resource) => parts.push(format!("Resource: {}", resource.uri)),
            ContentBlock::Resource(resource) => {
                let value = serde_json::to_string(resource).map_err(|error| internal_error(error.to_string()))?;
                parts.push(format!("Embedded resource: {value}"));
            }
            ContentBlock::Image(_) | ContentBlock::Audio(_) => {
                return Err(invalid_params(
                    "Solaris ACP currently accepts text and resource content blocks only",
                ));
            }
            _ => return Err(invalid_params("unsupported ACP content block")),
        }
    }
    Ok(parts.join("\n"))
}

fn parse_policy(value: &str) -> Result<MultiAgentPolicy, AcpError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "disabled" => Ok(MultiAgentPolicy::Disabled),
        "on_demand" | "on-demand" | "ondemand" => Ok(MultiAgentPolicy::OnDemand),
        "proactive" => Ok(MultiAgentPolicy::Proactive),
        _ => Err(invalid_params(
            "multi_agent_policy must be disabled, on_demand, or proactive",
        )),
    }
}

fn parse_strategy(value: &str) -> Result<CollaborationSelection, AcpError> {
    let selection = match value.trim().to_ascii_lowercase().replace('-', "_").as_str() {
        "auto" => CollaborationSelection::Auto,
        "single" => CollaborationSelection::Fixed(CollaborationStrategy::Single),
        "supervisor" => CollaborationSelection::Configured(solaris_types::workflow::CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Supervisor,
            ..Default::default()
        }),
        "team" => CollaborationSelection::Configured(solaris_types::workflow::CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Team,
            ..Default::default()
        }),
        "fanout" => CollaborationSelection::Configured(solaris_types::workflow::CollaborationRuntimeConfig {
            strategy: CollaborationStrategy::Fanout,
            ..Default::default()
        }),
        "independent_reviewer" | "reviewer" => {
            CollaborationSelection::Configured(solaris_types::workflow::CollaborationRuntimeConfig {
                strategy: CollaborationStrategy::IndependentReviewer,
                ..Default::default()
            })
        }
        _ => return Err(invalid_params("unknown collaboration_strategy")),
    };
    Ok(selection)
}

fn selection_name(selection: &CollaborationSelection) -> String {
    match selection {
        CollaborationSelection::Auto => "auto".to_owned(),
        CollaborationSelection::Fixed(strategy) => strategy.as_str().to_owned(),
        CollaborationSelection::Configured(config) => config.strategy.as_str().to_owned(),
        CollaborationSelection::Inherit => "auto".to_owned(),
    }
}

fn respond_result<T: agent_client_protocol::JsonRpcResponse>(
    responder: Responder<T>,
    result: Result<T, AcpError>,
) -> Result<(), AcpError> {
    responder.respond_with_result(result)
}

fn invalid_params(message: impl Into<String>) -> AcpError {
    AcpError::invalid_params().data(message.into())
}

fn internal_error(message: impl Into<String>) -> AcpError {
    AcpError::internal_error().data(message.into())
}

/// Synchronous bridge from Mesh's output sink to ACP session/update events.
struct AcpOutputSink {
    session_id: SessionId,
    connection: ConnectionTo<agent_client_protocol::Client>,
    errors: Arc<StdMutex<Option<String>>>,
}

impl AcpOutputSink {
    fn new(
        session_id: SessionId,
        connection: ConnectionTo<agent_client_protocol::Client>,
        errors: Arc<StdMutex<Option<String>>>,
    ) -> Self {
        Self {
            session_id,
            connection,
            errors,
        }
    }

    fn send(&self, update: SessionUpdate) {
        if let Err(error) = self
            .connection
            .send_notification(SessionNotification::new(self.session_id.clone(), update))
            && let Ok(mut slot) = self.errors.lock()
        {
            *slot = Some(error.to_string());
        }
    }
}

impl OutputSink for AcpOutputSink {
    fn emit_text_delta(&self, text: &str, _msg_id: &str) {
        self.send(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
            TextContent::new(text),
        ))));
    }

    fn emit_thinking(&self, text: &str, _msg_id: &str) {
        self.send(SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::Text(
            TextContent::new(text),
        ))));
    }

    fn emit_tool_call(&self, tool_use_id: &str, name: &str, input: &str) {
        self.send(SessionUpdate::ToolCall(
            ToolCall::new(ToolCallId::new(tool_use_id), format!("{name}: {input}")).status(ToolCallStatus::InProgress),
        ));
    }

    fn emit_tool_result(&self, tool_use_id: &str, name: &str, is_error: bool, content: &str) {
        let status = if is_error {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Completed
        };
        let fields = ToolCallUpdateFields::new()
            .status(status)
            .title(name)
            .content(vec![ContentBlock::Text(TextContent::new(content)).into()]);
        self.send(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            ToolCallId::new(tool_use_id),
            fields,
        )));
    }

    fn emit_stream_start(&self, _msg_id: &str) {}

    fn emit_stream_end(
        &self,
        _msg_id: &str,
        _turns: usize,
        _input_tokens: u64,
        _output_tokens: u64,
        _cache_creation_tokens: u64,
        _cache_read_tokens: u64,
    ) {
    }

    fn emit_error(&self, msg: &str) {
        self.emit_text_delta(msg, "acp-error");
    }

    fn emit_info(&self, msg: &str) {
        self.emit_text_delta(msg, "acp-info");
    }
}

#[cfg(test)]
#[path = "test.rs"]
mod test;
