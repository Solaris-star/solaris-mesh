use super::*;
use serde_json::json;

#[test]
fn validates_required_and_enum() {
    let schema = json!({
        "type": "object",
        "required": ["verdict"],
        "properties": {"verdict": {"enum": ["PASS", "FAIL"]}}
    });
    assert!(validate_value(&json!({"verdict":"PASS"}), &schema).is_ok());
    assert!(validate_value(&json!({"verdict":"MAYBE"}), &schema).is_err());
    assert!(validate_value(&json!({}), &schema).is_err());
}
