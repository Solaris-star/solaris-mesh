use std::path::{Path, PathBuf};

use solaris_config::config::{Config, ProviderType};
use solaris_types::effect::{EffectClass, EffectDescriptor, EffectReplayPolicy, ResourceFootprint};
use solaris_types::identity::{AgentId, RunId};

pub(crate) fn effect_descriptor(endpoint: &str) -> EffectDescriptor {
    EffectDescriptor {
        class: EffectClass::Network,
        action: "request configured LLM provider".into(),
        resources: ResourceFootprint {
            network_domains: vec![endpoint.to_owned()],
            external_resources: vec!["provider:configured".into()],
            ..Default::default()
        },
        replay_policy: EffectReplayPolicy::ReconcileRequired,
    }
}

pub(crate) fn effect_descriptor_for_config(config: &Config, workspace: &Path) -> EffectDescriptor {
    let mut descriptor = effect_descriptor(&config.base_url);
    let add_file = |files: &mut Vec<String>, path: PathBuf| {
        let resolved = if path.is_absolute() { path } else { workspace.join(path) };
        files.push(
            resolved
                .canonicalize()
                .unwrap_or(resolved)
                .to_string_lossy()
                .into_owned(),
        );
    };
    match config.provider {
        ProviderType::Vertex => {
            let vertex = config.vertex.clone().unwrap_or_default();
            let region = vertex.region.unwrap_or_else(|| "us-central1".to_owned());
            descriptor
                .resources
                .network_domains
                .push(format!("{region}-aiplatform.googleapis.com"));
            descriptor
                .resources
                .network_domains
                .push("oauth2.googleapis.com".into());
            if let Some(path) = vertex.credentials_file {
                add_file(&mut descriptor.resources.file_reads, PathBuf::from(path));
            } else if vertex.service_account_json.is_none() {
                if let Some(home) = home_dir() {
                    add_file(
                        &mut descriptor.resources.file_reads,
                        home.join(".config/gcloud/application_default_credentials.json"),
                    );
                }
                descriptor
                    .resources
                    .network_domains
                    .push("metadata.google.internal".into());
            }
        }
        ProviderType::Bedrock => {
            let bedrock = config.bedrock.clone().unwrap_or_default();
            let region = bedrock
                .region
                .or_else(|| std::env::var("AWS_REGION").ok())
                .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
                .unwrap_or_else(|| "us-east-1".to_owned());
            descriptor
                .resources
                .network_domains
                .push(format!("bedrock-runtime.{region}.amazonaws.com"));
            if bedrock.access_key_id.is_none() || bedrock.secret_access_key.is_none() {
                let credential_path = bedrock.credentials_file.map(PathBuf::from);
                if let Some(path) = credential_path {
                    add_file(&mut descriptor.resources.file_reads, path);
                }
            }
        }
        ProviderType::Anthropic | ProviderType::OpenAI => {}
    }
    descriptor.resources.file_reads.sort();
    descriptor.resources.file_reads.dedup();
    descriptor.resources.network_domains.retain(|value| !value.is_empty());
    descriptor.resources.network_domains.sort();
    descriptor.resources.network_domains.dedup();
    descriptor
}

pub(super) fn pin_credential_paths(config: &mut Config, workspace: &Path) {
    let resolve = |raw: PathBuf| {
        let joined = if raw.is_absolute() { raw } else { workspace.join(raw) };
        joined.canonicalize().unwrap_or(joined).to_string_lossy().into_owned()
    };

    if config.provider == ProviderType::Vertex && config.vertex.is_none() {
        config.vertex = Some(Default::default());
    }
    if config.provider == ProviderType::Bedrock && config.bedrock.is_none() {
        config.bedrock = Some(Default::default());
    }

    if let Some(vertex) = config.vertex.as_mut()
        && let Some(path) = vertex.credentials_file.take()
    {
        vertex.credentials_file = Some(resolve(PathBuf::from(path)));
    }

    if let Some(bedrock) = config.bedrock.as_mut()
        && (bedrock.access_key_id.is_none() || bedrock.secret_access_key.is_none())
    {
        let path = bedrock
            .credentials_file
            .take()
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("AWS_SHARED_CREDENTIALS_FILE").map(PathBuf::from))
            .or_else(|| home_dir().map(|home| home.join(".aws/credentials")));
        bedrock.credentials_file = path.map(resolve);
    }
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
}

pub(super) fn root_agent_id_for_run(run_id: &RunId) -> AgentId {
    AgentId::new(format!("agent:root:{}", run_id.as_str()))
}
