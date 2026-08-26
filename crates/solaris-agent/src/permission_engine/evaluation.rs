use super::*;

pub(super) fn replace_runtime_rules(generated: &mut BTreeMap<String, Vec<PermissionRule>>) {
    let rules = ["ProviderRequest", "AutoCompact"]
        .into_iter()
        .map(|capability| PermissionRule {
            capability: Some(capability.into()),
            action: None,
            effect_class: Some(EffectClass::Network),
            resource_prefixes: Vec::new(),
            decision: PermissionDecision::Allow,
        })
        .collect::<Vec<_>>();
    generated.insert("runtime:permission-mode".to_owned(), rules);
}

pub(super) fn preset_default(mode: PermissionMode, class: EffectClass) -> PermissionDecision {
    match mode {
        PermissionMode::Plan => match class {
            EffectClass::ReadOnly | EffectClass::AgentLifecycle | EffectClass::MeshStateMutation => {
                PermissionDecision::Allow
            }
            EffectClass::WorkspaceMutation
            | EffectClass::Process
            | EffectClass::Network
            | EffectClass::ExternalSideEffect => PermissionDecision::Deny,
        },
        PermissionMode::Auto => match class {
            EffectClass::ReadOnly
            | EffectClass::AgentLifecycle
            | EffectClass::MeshStateMutation
            | EffectClass::WorkspaceMutation => PermissionDecision::Allow,
            EffectClass::Process | EffectClass::Network => PermissionDecision::AutoReview,
            EffectClass::ExternalSideEffect => PermissionDecision::Ask,
        },
        PermissionMode::Bypass => PermissionDecision::Allow,
    }
}

pub(super) fn rule_matches(rule: &PermissionRule, capability: &str, descriptor: &EffectDescriptor) -> bool {
    if rule.capability.as_deref().is_some_and(|value| value != capability) {
        return false;
    }
    if rule.action.as_deref().is_some_and(|value| value != descriptor.action) {
        return false;
    }
    if rule.effect_class.is_some_and(|value| value != descriptor.class) {
        return false;
    }
    if rule.resource_prefixes.is_empty() {
        return true;
    }

    let matches = descriptor
        .resources
        .file_reads
        .iter()
        .chain(descriptor.resources.file_writes.iter())
        .map(|resource| {
            rule.resource_prefixes
                .iter()
                .any(|prefix| permission_path_matches(resource, prefix))
        })
        .chain(descriptor.resources.network_domains.iter().map(|domain| {
            rule.resource_prefixes
                .iter()
                .any(|allowed| domain_matches(domain, allowed))
        }))
        .chain(descriptor.resources.process_commands.iter().map(|command| {
            rule.resource_prefixes
                .iter()
                .any(|approved| command == approved && command_is_structurally_safe(command))
        }))
        .chain(descriptor.resources.external_resources.iter().map(|resource| {
            rule.resource_prefixes
                .iter()
                .any(|prefix| namespaced_resource_matches(resource, prefix))
        }))
        .chain(descriptor.resources.mesh_resources.iter().map(|resource| {
            rule.resource_prefixes
                .iter()
                .any(|prefix| namespaced_resource_matches(resource, prefix))
        }))
        .collect::<Vec<_>>();

    if matches.is_empty() {
        return false;
    }
    if rule.decision == PermissionDecision::Deny {
        matches.into_iter().any(|value| value)
    } else {
        matches.into_iter().all(|value| value)
    }
}

fn namespaced_resource_matches(resource: &str, allowed: &str) -> bool {
    resource == allowed
        || resource
            .strip_prefix(allowed)
            .is_some_and(|suffix| suffix.starts_with(':') || suffix.starts_with('/') || suffix.starts_with('#'))
}

pub(super) fn lease_matches(entry: &LeaseEntry, run_id: &RunId, request: &EffectRequest, now: i64) -> bool {
    let lease = &entry.lease;
    if lease.capability != request.capability {
        return false;
    }
    if lease
        .action
        .as_deref()
        .is_some_and(|action| action != request.descriptor.action)
    {
        return false;
    }
    if lease.expires_at_unix_ms.is_some_and(|expires| expires <= now) {
        return false;
    }
    if lease.max_uses.is_some_and(|max_uses| entry.uses >= max_uses) {
        return false;
    }
    let scope_matches = match &lease.scope {
        LeaseScope::Effect { effect_id } => effect_id == &request.effect_id,
        LeaseScope::Operation { operation_id } => operation_id == &request.operation_id,
        LeaseScope::Run { run_id: lease_run } => lease_run == run_id,
    };
    if !scope_matches {
        return false;
    }
    if matches!(lease.scope, LeaseScope::Effect { .. }) {
        return true;
    }
    grants_cover(&lease.grants, &request.descriptor)
}

fn grants_cover(grants: &AdditionalPermissions, descriptor: &EffectDescriptor) -> bool {
    if !descriptor.resources.external_resources.is_empty() {
        return false;
    }
    (!descriptor.resources.unrestricted_file_reads || grants.unrestricted_file_reads)
        && (!descriptor.resources.unrestricted_file_writes || grants.unrestricted_file_writes)
        && (!descriptor.resources.unrestricted_network || grants.unrestricted_network)
        && (!descriptor.resources.unrestricted_process || grants.unrestricted_process)
        && descriptor
            .resources
            .file_reads
            .iter()
            .all(|resource| grants.file_reads.iter().any(|grant| path_within(resource, grant)))
        && descriptor
            .resources
            .file_writes
            .iter()
            .all(|resource| grants.file_writes.iter().any(|grant| path_within(resource, grant)))
        && descriptor
            .resources
            .network_domains
            .iter()
            .all(|domain| grants.network_domains.iter().any(|grant| domain_matches(domain, grant)))
        && descriptor.resources.process_commands.iter().all(|command| {
            grants.process_command_prefix.as_deref().is_some_and(|approved| {
                command == approved
                    && (!descriptor.resources.process_invocations.is_empty() || command_is_structurally_safe(command))
            })
        })
        && descriptor
            .resources
            .process_invocations
            .iter()
            .all(|invocation| grants.process_invocations.contains(invocation))
}

pub(super) fn boundary_allows(boundary: &ExecutionBoundary, descriptor: &EffectDescriptor) -> bool {
    let reads_allowed = (!descriptor.resources.unrestricted_file_reads || boundary.unrestricted_file_reads)
        && (descriptor.resources.file_reads.is_empty()
            || boundary.unrestricted_file_reads
            || descriptor
                .resources
                .file_reads
                .iter()
                .all(|resource| boundary.readable_roots.iter().any(|root| path_within(resource, root))));
    let writes_allowed = if descriptor.resources.unrestricted_file_writes && !boundary.unrestricted_file_writes {
        false
    } else if descriptor.resources.file_writes.is_empty() {
        descriptor.class != EffectClass::WorkspaceMutation || boundary.unrestricted_file_writes
    } else {
        boundary.unrestricted_file_writes
            || descriptor
                .resources
                .file_writes
                .iter()
                .all(|resource| boundary.writable_roots.iter().any(|root| path_within(resource, root)))
    };
    let network_allowed = if descriptor.resources.unrestricted_network && !boundary.unrestricted_network {
        false
    } else if descriptor.resources.network_domains.is_empty() {
        descriptor.class != EffectClass::Network || boundary.unrestricted_network
    } else {
        boundary.unrestricted_network
            || descriptor.resources.network_domains.iter().all(|domain| {
                boundary
                    .network_domains
                    .iter()
                    .any(|allowed| domain_matches(domain, allowed))
            })
    };
    let process_commands_allowed = descriptor.resources.process_commands.is_empty()
        || boundary.unrestricted_process
        || descriptor.resources.process_commands.iter().all(|command| {
            boundary.process_command_prefixes.iter().any(|approved| {
                command == approved
                    && (!descriptor.resources.process_invocations.is_empty() || command_is_structurally_safe(command))
            })
        });
    let process_invocations_allowed = descriptor.resources.process_invocations.is_empty()
        || boundary.unrestricted_process
        || descriptor
            .resources
            .process_invocations
            .iter()
            .all(|invocation| boundary.process_invocations.contains(invocation));
    let process_allowed = if descriptor.resources.unrestricted_process && !boundary.unrestricted_process {
        false
    } else if descriptor.resources.process_commands.is_empty() && descriptor.resources.process_invocations.is_empty() {
        descriptor.class != EffectClass::Process || boundary.unrestricted_process
    } else {
        process_commands_allowed && process_invocations_allowed
    };
    let external_allowed = if descriptor.resources.external_resources.is_empty() {
        descriptor.class != EffectClass::ExternalSideEffect || boundary.unrestricted_external_side_effects
    } else {
        boundary.unrestricted_external_side_effects
            || descriptor.resources.external_resources.iter().all(|resource| {
                boundary
                    .external_resource_prefixes
                    .iter()
                    .any(|prefix| namespaced_resource_matches(resource, prefix))
            })
    };
    reads_allowed && writes_allowed && network_allowed && process_allowed && external_allowed
}

fn command_is_structurally_safe(command: &str) -> bool {
    let trimmed = command.trim();
    !trimmed.is_empty()
        && !trimmed
            .chars()
            .any(|ch| matches!(ch, '&' | '|' | ';' | '>' | '<' | '`' | '\n' | '\r'))
        && !trimmed.contains("$(")
}

fn domain_matches(domain: &str, allowed: &str) -> bool {
    allowed == "*" || solaris_process::permission_domain_is_covered(domain, allowed).unwrap_or(false)
}
