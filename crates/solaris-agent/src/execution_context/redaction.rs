use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use solaris_types::effect::{EffectAuditProjection, EffectDescriptor, ProcessInvocation, ResourceFootprint};

fn effect_descriptor_digest(value: &Value) -> Option<String> {
    let projection = serde_json::from_value::<EffectAuditProjection>(value.clone()).ok()?;
    projection
        .has_supported_version()
        .then_some(projection.descriptor_digest)
}

pub(super) fn payload_effect_descriptor_digest(payload: &Value) -> Option<String> {
    payload.get("effect").and_then(effect_descriptor_digest).or_else(|| {
        payload
            .get("descriptor")
            .and_then(|value| serde_json::from_value::<EffectDescriptor>(value.clone()).ok())
            .map(|descriptor| EffectAuditProjection::from_descriptor(&descriptor).descriptor_digest)
    })
}

pub(super) fn secret_safe_descriptor(descriptor: &EffectDescriptor) -> EffectDescriptor {
    let mut descriptor = descriptor.clone();
    let has_process_details = !descriptor.resources.process_commands.is_empty()
        || descriptor
            .resources
            .process_invocations
            .iter()
            .any(|invocation| !invocation.argv.is_empty());
    if has_process_details {
        descriptor.action = format!(
            "Process effect (sha256:{})",
            stable_digest_bytes(descriptor.action.as_bytes())
        );
    }
    descriptor.resources.process_commands = descriptor
        .resources
        .process_commands
        .iter()
        .map(|command| format!("sha256:{}", stable_digest_bytes(command.as_bytes())))
        .collect();
    descriptor.resources.process_invocations = descriptor
        .resources
        .process_invocations
        .iter()
        .map(secret_safe_invocation)
        .collect();
    descriptor
}

fn secret_safe_invocation(invocation: &ProcessInvocation) -> ProcessInvocation {
    ProcessInvocation {
        executable: invocation.executable.clone(),
        argv: if invocation.argv.is_empty() {
            Vec::new()
        } else {
            vec![
                format!("sha256:{}", stable_digest_serializable(&invocation.argv)),
                format!("argc:{}", invocation.argv.len()),
            ]
        },
    }
}

pub(super) fn normalize_descriptor(mut descriptor: EffectDescriptor) -> EffectDescriptor {
    descriptor.resources = normalize_footprint(descriptor.resources);
    descriptor
}

fn normalize_footprint(mut footprint: ResourceFootprint) -> ResourceFootprint {
    footprint.file_reads = footprint
        .file_reads
        .into_iter()
        .map(|path| normalize_path(&path))
        .collect();
    footprint.file_writes = footprint
        .file_writes
        .into_iter()
        .map(|path| normalize_path(&path))
        .collect();
    footprint.network_domains = footprint
        .network_domains
        .into_iter()
        .map(|domain| domain.trim_end_matches('.').to_ascii_lowercase())
        .collect();
    footprint.file_reads.sort();
    footprint.file_reads.dedup();
    footprint.file_writes.sort();
    footprint.file_writes.dedup();
    footprint.network_domains.sort();
    footprint.network_domains.dedup();
    footprint.process_commands.sort();
    footprint.process_commands.dedup();
    footprint
        .process_invocations
        .sort_by(|left, right| (&left.executable, &left.argv).cmp(&(&right.executable, &right.argv)));
    footprint.process_invocations.dedup();
    footprint.external_resources.sort();
    footprint.external_resources.dedup();
    footprint.mesh_resources.sort();
    footprint.mesh_resources.dedup();
    footprint
}

fn normalize_path(value: &str) -> String {
    let path = Path::new(value);
    if let Ok(canonical) = path.canonicalize() {
        return canonical.to_string_lossy().into_owned();
    }
    if let Some(parent) = path.parent()
        && let Ok(canonical_parent) = parent.canonicalize()
        && let Some(name) = path.file_name()
    {
        return canonical_parent.join(name).to_string_lossy().into_owned();
    }
    let absolute = if path.is_absolute() {
        PathBuf::from(path)
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    absolute.to_string_lossy().into_owned()
}

pub(super) fn stable_digest_serializable<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .map(|value| digest_json_value(&value))
        .unwrap_or_else(|_| "0000000000000000".to_owned())
}

pub(crate) fn stable_digest_value(value: &Value) -> String {
    digest_json_value(value)
}

fn digest_json_value(value: &Value) -> String {
    serde_json::to_vec(&canonicalize_json_value(value))
        .map(|bytes| stable_digest_bytes(&bytes))
        .unwrap_or_else(|_| "0000000000000000".to_owned())
}

/// Canonicalize object insertion order before hashing.
///
/// `serde_json` can be built with its `preserve_order` feature.  That feature
/// is useful for display, but it must not change durable identities: a value
/// loaded from a provider response and the same value reconstructed from a
/// ledger record may otherwise hash differently solely because their object
/// keys were inserted in a different order.
fn canonicalize_json_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort_unstable();
            let mut canonical = Map::with_capacity(object.len());
            for key in keys {
                canonical.insert(key.clone(), canonicalize_json_value(&object[key]));
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json_value).collect()),
        _ => value.clone(),
    }
}

/// Remove command bodies, argv values, and common credential fields before
/// durable records are returned through a Host protocol boundary.
pub fn secret_safe_ledger_payload(mut payload: Value) -> Value {
    redact_ledger_value(&mut payload);
    payload
}

fn redact_ledger_value(value: &mut Value) {
    let Value::Object(object) = value else {
        if let Value::Array(values) = value {
            for value in values {
                redact_ledger_value(value);
            }
        }
        return;
    };

    if let Some(raw_descriptor) = object.remove("descriptor") {
        object.insert("effect".to_owned(), legacy_effect_projection(raw_descriptor));
    }

    let has_process_details = object
        .get("process_commands")
        .is_some_and(|value| value.as_array().is_some_and(|values| !values.is_empty()))
        || object
            .get("process_command_prefix")
            .is_some_and(|value| !value.is_null())
        || object
            .get("process_invocations")
            .is_some_and(|value| value.as_array().is_some_and(|values| !values.is_empty()));
    let is_process_descriptor = object.get("class").and_then(Value::as_str) == Some("process");
    if (has_process_details || is_process_descriptor)
        && let Some(action) = object.get_mut("action")
    {
        redact_scalar(action, "Process effect");
    }
    if let Some(commands) = object.get_mut("process_commands").and_then(Value::as_array_mut) {
        for command in commands {
            redact_scalar(command, "command");
        }
    }
    if let Some(command) = object.get_mut("process_command_prefix") {
        redact_scalar(command, "command");
    }
    if let Some(invocations) = object.get_mut("process_invocations").and_then(Value::as_array_mut) {
        for invocation in invocations {
            if let Some(argv) = invocation.get_mut("argv") {
                redact_argv(argv);
            }
        }
    }

    for (key, value) in object {
        if key == "effect" && normalize_effect_projection(value) {
            continue;
        }
        if key.eq_ignore_ascii_case("argv") {
            redact_argv(value);
        } else if is_sensitive_ledger_key(key) {
            redact_scalar(value, "secret");
        } else {
            redact_ledger_value(value);
        }
    }
}

fn normalize_effect_projection(value: &mut Value) -> bool {
    if is_exact_fallback_projection(value) {
        return true;
    }
    if serde_json::from_value::<EffectAuditProjection>(value.clone())
        .is_ok_and(|projection| projection.is_secret_safe())
    {
        return true;
    }
    match serde_json::from_value::<EffectDescriptor>(value.clone()) {
        Ok(descriptor) => {
            *value = effect_projection_value(&descriptor);
            true
        }
        Err(_) if value.get("version").is_some() || value.get("descriptor_digest").is_some() => {
            *value = fallback_projection(value);
            true
        }
        Err(_) => false,
    }
}

fn legacy_effect_projection(value: Value) -> Value {
    match serde_json::from_value::<EffectDescriptor>(value.clone()) {
        Ok(descriptor) => effect_projection_value(&descriptor),
        Err(error) => {
            tracing::warn!(%error, "legacy effect descriptor could not be projected");
            fallback_projection(&value)
        }
    }
}

fn effect_projection_value(descriptor: &EffectDescriptor) -> Value {
    match serde_json::to_value(EffectAuditProjection::from_descriptor(descriptor)) {
        Ok(projection) => projection,
        Err(error) => {
            tracing::warn!(%error, "failed to serialize secret-safe effect projection");
            fallback_projection(&json!({"serialization_error": true}))
        }
    }
}

fn fallback_projection(value: &Value) -> Value {
    json!({
        "version": EffectAuditProjection::VERSION,
        "redacted": true,
        "descriptor_digest": format!("sha256:{}", stable_digest_value(value)),
    })
}

pub(super) fn is_exact_fallback_projection(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.len() == 3
        && object.get("version").and_then(Value::as_str) == Some(EffectAuditProjection::VERSION)
        && object.get("redacted").and_then(Value::as_bool) == Some(true)
        && object
            .get("descriptor_digest")
            .and_then(Value::as_str)
            .is_some_and(is_bare_sha256_marker)
}

fn redact_argv(argv: &mut Value) {
    let already_redacted = argv.as_array().is_some_and(|values| {
        values.len() == 2
            && values
                .first()
                .and_then(Value::as_str)
                .is_some_and(is_bare_sha256_marker)
            && values.get(1).and_then(Value::as_str).is_some_and(is_argc_marker)
    });
    if already_redacted {
        return;
    }
    let count = argv.as_array().map_or(0, Vec::len);
    let digest = stable_digest_value(argv);
    *argv = json!([format!("sha256:{digest}"), format!("argc:{count}")]);
}

fn redact_scalar(value: &mut Value, label: &str) {
    if value.is_null() || value.as_str().is_some_and(is_redaction_marker) {
        return;
    }
    let digest = stable_digest_value(value);
    *value = Value::String(format!("{label}:sha256:{digest}"));
}

pub(super) fn is_redaction_marker(value: &str) -> bool {
    is_bare_sha256_marker(value)
        || ["secret", "command", "Process effect"].iter().any(|label| {
            value
                .strip_prefix(label)
                .and_then(|suffix| suffix.strip_prefix(':'))
                .is_some_and(is_bare_sha256_marker)
        })
}

pub(super) fn is_bare_sha256_marker(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

pub(super) fn is_argc_marker(value: &str) -> bool {
    value
        .strip_prefix("argc:")
        .and_then(|count| count.parse::<u64>().ok().map(|parsed| (count, parsed)))
        .is_some_and(|(count, parsed)| count == parsed.to_string())
}

fn is_sensitive_ledger_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    matches!(
        key.as_str(),
        "authorization"
            | "api_key"
            | "api-key"
            | "access_token"
            | "refresh_token"
            | "password"
            | "client_secret"
            | "effective_input"
            | "user_input"
            | "headers"
            | "cmd"
            | "command"
            | "env"
    ) || key.contains("secret")
}

pub(crate) fn stable_digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
#[path = "redaction_test.rs"]
mod redaction_test;
