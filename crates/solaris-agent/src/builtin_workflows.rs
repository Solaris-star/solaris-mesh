use std::collections::BTreeMap;

use serde_json::json;
use solaris_types::permission::PermissionCeiling;
use solaris_types::resource::ResourceBudget;
use solaris_types::workflow::{
    AgentRoleDefinition, CollaborationSelection, CollaborationStrategy, ModelPolicy, OutputBinding, RetryPolicy,
    WorkflowDefinition, WorkflowNode,
};

use crate::role_registry::AgentRoleRegistry;
use crate::workflow_controller::WorkflowController;

pub const STANDARD_PLAN_WORKFLOW: &str = "standard-plan";
pub const DEEP_RESEARCH_WORKFLOW: &str = "deep-research-v1";
pub const ULTRACODE_WORKFLOW: &str = "ultracode-v1";
pub(crate) const DEFAULT_WORKFLOW_ROLE_TOKEN_BUDGET: u64 = 128_000;

pub fn register_builtin_workflows(controller: &WorkflowController, roles: &AgentRoleRegistry) -> Result<(), String> {
    for role in builtin_roles() {
        roles.register(role);
    }
    controller.register(standard_plan())?;
    controller.register(deep_research())?;
    controller.register(ultracode())?;
    Ok(())
}

pub fn builtin_roles() -> Vec<AgentRoleDefinition> {
    vec![
        role(
            "planner",
            "Inspect the task and workspace without mutating user files. Begin with exact paths and input sets named by the task. Use no more than one targeted discovery call, and inspect at most one representative file when the task already defines the input format. Do not inventory the workspace. Treat .solaris as internal runtime metadata and do not inspect it unless the user explicitly requests runtime diagnostics. Return a concise JSON plan with steps, risks and verification commands.",
            PermissionCeiling::plan(),
            vec!["Read", "Grep", "Glob"],
        ),
        role(
            "researcher",
            "Gather concrete evidence from code, documentation and available read-only sources. Begin with exact paths and input sets named by the task. Use no more than one targeted Glob. Read each directly relevant file at most once, and return as soon as the assigned task has enough evidence. Do not inventory unrelated file types or search for generic project metadata. Treat .solaris as internal runtime metadata and do not inspect it unless the user explicitly requests runtime diagnostics. Return JSON with evidence, locations, constraints and uncertainties.",
            PermissionCeiling::plan(),
            vec!["Read", "Grep", "Glob"],
        ),
        role(
            "implementer",
            "Implement the assigned engineering task, following supplied plan/research outputs. Create the requested deliverable first once the required inputs are known. For small data transformations, write the final artifact directly instead of adding a helper program or depending on an unverified runtime. Do not create validation helper files unless explicitly requested. Limit post-write verification to at most one smallest relevant check that succeeds: use one successful verification command, then return JSON immediately. For simple output validation, use the configured shell's built-in capabilities and set an explicit timeout of at most 15,000 ms. Do not invoke an additional language runtime unless the task requires it or the workspace already establishes it as available. If the check fails, repair once and run one final check. Treat .solaris as internal runtime metadata and do not inspect it unless the user explicitly requests runtime diagnostics. Return a concise JSON summary of changes and checks.",
            PermissionCeiling::unrestricted(),
            vec!["Read", "Write", "Edit", "ExecCommand", "Grep", "Glob"],
        ),
        AgentRoleDefinition {
            id: "verifier".into(),
            description: "Independently inspect the implementation and run the smallest real checks needed. Do not edit files. Use one targeted inspection pass and at most one ExecCommand. For simple output validation, use the configured shell's built-in capabilities and set an explicit timeout of at most 15,000 ms. Do not invoke an additional language runtime unless the task requires it or the workspace already establishes it as available. When required outputs and input immutability pass, return PASS immediately without broader or repeated checks. Treat .solaris as internal runtime metadata and do not inspect it unless the user explicitly requests runtime diagnostics. Return strict JSON with verdict PASS/FAIL, evidence, and issues.".into(),
            input_schema: None,
            output_schema: Some(json!({
                "type": "object",
                "required": ["verdict", "evidence"],
                "properties": {
                    "verdict": {"enum": ["PASS", "FAIL"]},
                    "evidence": {},
                    "issues": {"type": "array"}
                }
            })),
            model_policy: ModelPolicy { model: None, reasoning_effort: Some("high".into()) },
            capability_scope: vec!["Read".into(), "Grep".into(), "Glob".into(), "ExecCommand".into()],
            permission_ceiling: PermissionCeiling { process: true, ..PermissionCeiling::plan() },
            context_policy: Some("isolated_verification".into()),
            recursion_policy: Some("none".into()),
            budget: ResourceBudget {
                max_turns: Some(80),
                max_tokens: Some(DEFAULT_WORKFLOW_ROLE_TOKEN_BUDGET),
                ..Default::default()
            },
        },
        role(
            "synthesizer",
            "Synthesize supplied structured outputs into the final answer without performing new side effects. Return concise JSON.",
            PermissionCeiling::plan(),
            vec![],
        ),
    ]
}

fn role(id: &str, description: &str, permission_ceiling: PermissionCeiling, tools: Vec<&str>) -> AgentRoleDefinition {
    AgentRoleDefinition {
        id: id.into(),
        description: format!(
            "{description} Set completed to true when this role's assigned stage is finished; completed does not describe the overall workflow. Keep all array entries as short plain strings without Markdown, backticks, nested quotes, or command snippets."
        ),
        input_schema: None,
        output_schema: Some(role_output_schema(id)),
        model_policy: ModelPolicy::default(),
        capability_scope: tools.into_iter().map(str::to_owned).collect(),
        permission_ceiling,
        context_policy: Some("isolated".into()),
        recursion_policy: Some("bounded".into()),
        budget: ResourceBudget {
            max_turns: Some(120),
            max_tokens: Some(DEFAULT_WORKFLOW_ROLE_TOKEN_BUDGET),
            ..Default::default()
        },
    }
}

fn role_output_schema(id: &str) -> serde_json::Value {
    let fields = match id {
        "planner" => json!({
            "steps": {"type": "array", "items": {"type": "string"}},
            "risks": {"type": "array", "items": {"type": "string"}},
            "verification": {"type": "array", "items": {"type": "string"}},
            "completed": {"type": "boolean"}
        }),
        "researcher" => json!({
            "evidence": {"type": "array", "items": {"type": "string"}},
            "constraints": {"type": "array", "items": {"type": "string"}},
            "uncertainties": {"type": "array", "items": {"type": "string"}},
            "completed": {"type": "boolean"}
        }),
        "implementer" => json!({
            "changes": {"type": "array", "items": {"type": "string"}},
            "checks": {"type": "array", "items": {"type": "string"}},
            "completed": {"type": "boolean"}
        }),
        "synthesizer" => json!({
            "summary": {},
            "completed": {"type": "boolean"}
        }),
        _ => json!({"completed": {"type": "boolean"}}),
    };
    let required: Vec<&str> = fields
        .as_object()
        .map(|values| values.keys().map(String::as_str).collect())
        .unwrap_or_default();
    json!({
        "type": "object",
        "required": required,
        "properties": fields,
        "additionalProperties": true
    })
}

fn bind(node: &mut WorkflowNode, from: &str, to: &str) {
    node.output_bindings.push(OutputBinding {
        from: from.to_owned(),
        to: to.to_owned(),
    });
}

fn node(
    id: &str,
    role: &str,
    depends_on: &[&str],
    collaboration: CollaborationSelection,
    ceiling: PermissionCeiling,
) -> WorkflowNode {
    WorkflowNode {
        id: id.into(),
        depends_on: depends_on.iter().map(|value| (*value).to_owned()).collect(),
        when: None,
        role: Some(role.into()),
        collaboration,
        model_policy: ModelPolicy::default(),
        capability_scope: Vec::new(),
        permission_ceiling: ceiling,
        retry: RetryPolicy { max_attempts: 1 },
        timeout_ms: None,
        output_bindings: Vec::new(),
        workflow_ref: None,
    }
}

pub fn standard_plan() -> WorkflowDefinition {
    WorkflowDefinition {
        id: STANDARD_PLAN_WORKFLOW.into(),
        schema_version: 1,
        version: "1".into(),
        description: "Create a typed read-only implementation plan.".into(),
        roles: builtin_roles()
            .into_iter()
            .filter(|role| role.id == "planner")
            .collect(),
        parameters_schema: None,
        nodes: vec![node(
            "plan",
            "planner",
            &[],
            CollaborationSelection::Fixed(CollaborationStrategy::Single),
            PermissionCeiling::plan(),
        )],
        outputs: BTreeMap::from([("plan".into(), "plan".into())]),
    }
}

pub fn deep_research() -> WorkflowDefinition {
    let mut research_primary = node(
        "research_primary",
        "researcher",
        &["research_plan"],
        CollaborationSelection::Fixed(CollaborationStrategy::Fanout),
        PermissionCeiling::plan(),
    );
    bind(&mut research_primary, "research_plan.steps", "plan.steps");
    let mut research_secondary = research_primary.clone();
    research_secondary.id = "research_secondary".into();
    let mut cross_check = node(
        "cross_check",
        "researcher",
        &["research_primary", "research_secondary"],
        CollaborationSelection::Fixed(CollaborationStrategy::IndependentReviewer),
        PermissionCeiling::plan(),
    );
    bind(&mut cross_check, "research_primary.evidence", "primary.evidence");
    bind(&mut cross_check, "research_secondary.evidence", "secondary.evidence");
    let mut synthesis = node(
        "synthesis",
        "synthesizer",
        &["cross_check"],
        CollaborationSelection::Fixed(CollaborationStrategy::Single),
        PermissionCeiling::plan(),
    );
    bind(&mut synthesis, "cross_check.evidence", "research.evidence");
    WorkflowDefinition {
        id: DEEP_RESEARCH_WORKFLOW.into(),
        schema_version: 1,
        version: "1".into(),
        description: "Run parallel evidence gathering, cross-checking, and typed synthesis.".into(),
        roles: builtin_roles()
            .into_iter()
            .filter(|role| matches!(role.id.as_str(), "planner" | "researcher" | "synthesizer"))
            .collect(),
        parameters_schema: None,
        nodes: vec![
            node(
                "research_plan",
                "planner",
                &[],
                CollaborationSelection::Fixed(CollaborationStrategy::Single),
                PermissionCeiling::plan(),
            ),
            research_primary,
            research_secondary,
            cross_check,
            synthesis,
        ],
        outputs: BTreeMap::from([("report".into(), "synthesis".into())]),
    }
}

pub fn ultracode() -> WorkflowDefinition {
    let mut implement = node(
        "implement",
        "implementer",
        &["plan", "research"],
        CollaborationSelection::Fixed(CollaborationStrategy::Single),
        PermissionCeiling::unrestricted(),
    );
    implement.when = Some(json!({
        "parameter_not_equals": {"field": "permission_mode", "value": "plan"}
    }));
    bind(&mut implement, "plan.steps", "plan.steps");
    bind(&mut implement, "research.evidence", "research.evidence");

    let mut repair = node(
        "repair",
        "implementer",
        &["verify"],
        CollaborationSelection::Fixed(CollaborationStrategy::Single),
        PermissionCeiling::unrestricted(),
    );
    repair.when = Some(json!({
        "all": [
            {"node_output_equals": {"node": "verify", "field": "verdict", "value": "FAIL"}},
            {"parameter_not_equals": {"field": "permission_mode", "value": "plan"}}
        ]
    }));
    bind(&mut repair, "verify.issues", "verification.issues");

    let mut reverify = node(
        "reverify",
        "verifier",
        &["repair"],
        CollaborationSelection::Fixed(CollaborationStrategy::Single),
        PermissionCeiling {
            process: true,
            ..PermissionCeiling::plan()
        },
    );
    reverify.retry.max_attempts = 2;
    reverify.when = Some(json!({
        "node_output_equals": {"node": "repair", "field": "completed", "value": true}
    }));
    bind(&mut reverify, "repair.changes", "implementation.changes");

    let mut verify = node(
        "verify",
        "verifier",
        &["implement"],
        CollaborationSelection::Fixed(CollaborationStrategy::Single),
        PermissionCeiling {
            process: true,
            ..PermissionCeiling::plan()
        },
    );
    verify.retry.max_attempts = 2;
    let mut finalize = node(
        "finalize",
        "synthesizer",
        &["verify", "reverify"],
        CollaborationSelection::Fixed(CollaborationStrategy::Single),
        PermissionCeiling::plan(),
    );
    finalize.retry.max_attempts = 2;
    bind(&mut finalize, "verify.verdict", "verification.verdict");

    WorkflowDefinition {
        id: ULTRACODE_WORKFLOW.into(),
        schema_version: 1,
        version: "1".into(),
        description: "Plan, research, implement, verify, repair, and synthesize a typed engineering result.".into(),
        roles: builtin_roles(),
        parameters_schema: Some(json!({
            "type": "object",
            "required": ["prompt", "permission_mode"],
            "properties": {
                "prompt": {"type": "string"},
                "permission_mode": {"enum": ["plan", "auto", "bypass"]}
            }
        })),
        nodes: vec![
            {
                let mut plan = node(
                    "plan",
                    "planner",
                    &[],
                    CollaborationSelection::Fixed(CollaborationStrategy::Single),
                    PermissionCeiling::plan(),
                );
                plan.retry.max_attempts = 2;
                plan
            },
            {
                let mut research = node(
                    "research",
                    "researcher",
                    &[],
                    CollaborationSelection::Fixed(CollaborationStrategy::Single),
                    PermissionCeiling::plan(),
                );
                research.retry.max_attempts = 2;
                research
            },
            implement,
            verify,
            repair,
            reverify,
            finalize,
        ],
        outputs: BTreeMap::from([("result".into(), "finalize".into())]),
    }
}
#[cfg(test)]
#[path = "builtin_workflows_test.rs"]
mod builtin_workflows_test;
