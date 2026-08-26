use super::*;

#[test]
fn host_identity_key_is_stable_across_reload() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mcp-identity-key.json");
    let first = McpIdentityKey::load_or_create(&path).unwrap();
    let second = McpIdentityKey::load_or_create(&path).unwrap();
    assert_eq!(first.version(), second.version());
    assert_eq!(first.key(), second.key());
}
