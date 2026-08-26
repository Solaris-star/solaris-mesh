use serde_json::{Value, json};

use super::*;

fn tasks_input(count: usize) -> Value {
    let tasks = (0..count)
        .map(|index| {
            json!({
                "name": format!("task-{index}"),
                "prompt": format!("run task {index}"),
            })
        })
        .collect::<Vec<_>>();
    json!({"tasks": tasks})
}

#[test]
fn spawn_schema_caps_tasks_at_256_items() {
    let schema = spawn_input_schema();

    assert_eq!(schema["properties"]["tasks"]["minItems"], 1);
    assert_eq!(schema["properties"]["tasks"]["maxItems"], 256);
}

#[test]
fn parse_tasks_rejects_an_empty_array() {
    let error = match parse_tasks(&tasks_input(0)) {
        Ok(_) => panic!("empty Spawn tasks must be rejected"),
        Err(error) => error,
    };

    assert_eq!(error, "No tasks provided");
}

#[test]
fn parse_tasks_accepts_32_items() {
    assert_eq!(parse_tasks(&tasks_input(32)).unwrap().len(), 32);
}

#[test]
fn parse_tasks_rejects_the_33rd_item_before_execution() {
    let error = match parse_tasks(&tasks_input(33)) {
        Ok(_) => panic!("33 Spawn tasks must be rejected"),
        Err(error) => error,
    };

    assert!(error.contains("32"), "{error}");
    assert!(error.contains("at most"), "{error}");
}

#[test]
fn parse_spawn_v2_accepts_dependencies_and_strategy() {
    let request = parse_spawn_request(&json!({
        "strategy": "supervisor",
        "tasks": [
            {"id": "search", "name": "Search", "prompt": "search", "role": "researcher"},
            {"id": "write", "name": "Write", "prompt": "write", "depends_on": ["search"], "expected_output": {"type": "object"}}
        ]
    }))
    .unwrap();
    assert_eq!(request.tasks.len(), 2);
    assert_eq!(request.tasks[1].depends_on, vec!["search"]);
    assert!(matches!(
        request.strategy,
        solaris_types::workflow::CollaborationSelection::Configured(ref config)
            if config.strategy == solaris_types::workflow::CollaborationStrategy::Supervisor
    ));
}

#[test]
fn parse_spawn_v2_rejects_duplicate_missing_self_and_cyclic_dependencies() {
    for input in [
        json!({"tasks": [{"id":"a","name":"a","prompt":"a"},{"id":"a","name":"b","prompt":"b"}]}),
        json!({"tasks": [{"id":"a","name":"a","prompt":"a","depends_on":["missing"]}]}),
        json!({"tasks": [{"id":"a","name":"a","prompt":"a","depends_on":["a"]}]}),
        json!({"tasks": [{"id":"a","name":"a","prompt":"a","depends_on":["b"]},{"id":"b","name":"b","prompt":"b","depends_on":["a"]}]}),
    ] {
        assert!(
            parse_spawn_request(&input).is_err(),
            "invalid graph was accepted: {input}"
        );
    }
}

#[test]
fn legacy_tasks_receive_stable_ids_and_v2_can_use_more_than_32() {
    let legacy = parse_spawn_request(&tasks_input(32)).unwrap();
    let again = parse_spawn_request(&tasks_input(32)).unwrap();
    assert_eq!(legacy.tasks[0].id, again.tasks[0].id);
    let tasks = (0..33)
        .map(|index| json!({"id": format!("task-{index}"), "name": "task", "prompt": "prompt"}))
        .collect::<Vec<_>>();
    assert_eq!(parse_spawn_request(&json!({"tasks": tasks})).unwrap().tasks.len(), 33);
}
