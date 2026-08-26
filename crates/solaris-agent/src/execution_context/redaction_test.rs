use serde_json::json;

use super::{stable_digest_serializable, stable_digest_value};

#[test]
fn json_digest_is_independent_of_object_insertion_order() {
    let first = json!({
        "outer": {"z": 3, "a": 1},
        "items": [{"b": false, "a": true}],
    });
    let second = json!({
        "items": [{"a": true, "b": false}],
        "outer": {"a": 1, "z": 3},
    });

    assert_eq!(stable_digest_value(&first), stable_digest_value(&second));
}

#[test]
fn serializable_digest_uses_the_same_canonical_json() {
    let first = serde_json::Map::from_iter([("z".to_owned(), json!(3)), ("a".to_owned(), json!(1))]);
    let second = serde_json::Map::from_iter([("a".to_owned(), json!(1)), ("z".to_owned(), json!(3))]);

    assert_eq!(stable_digest_serializable(&first), stable_digest_serializable(&second));
}
