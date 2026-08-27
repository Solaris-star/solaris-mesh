use std::fs;
use std::path::PathBuf;

use serde_json::Value;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("solaris-cli must remain inside the workspace crates directory")
        .to_path_buf()
}

#[test]
fn extension_manifest_declares_the_real_acp_command() {
    let manifest_path = repository_root().join("solaris-extension.json");
    let manifest: Value = serde_json::from_slice(&fs::read(&manifest_path).expect("extension manifest must exist"))
        .expect("extension manifest must be valid JSON");

    assert_eq!(
        manifest["$schema"],
        "https://raw.githubusercontent.com/Solaris-star/solaris-hub/v1.0.0/spec/extension-manifest.schema.json"
    );
    assert_eq!(manifest["name"], "solaris-mesh");
    assert_eq!(manifest["version"], env!("CARGO_PKG_VERSION"));
    let adapters = manifest["contributes"]["acpAdapters"]
        .as_array()
        .expect("manifest must contribute ACP adapters");
    assert_eq!(adapters.len(), 1);
    assert_eq!(adapters[0]["connectionType"], "cli");
    for field in ["id", "name", "description", "cliCommand", "defaultCliPath"] {
        assert!(
            adapters[0][field].as_str().is_some_and(|value| !value.is_empty()),
            "adapter field {field} must be a non-empty string"
        );
    }
    assert!(adapters[0]["authRequired"].is_boolean());
    assert!(adapters[0]["supportsStreaming"].is_boolean());
    assert_eq!(adapters[0]["cliCommand"], "solaris");
    assert_eq!(adapters[0]["defaultCliPath"], "solaris");
    assert_eq!(adapters[0]["acpArgs"], serde_json::json!(["acp"]));
}

#[test]
fn release_and_windows_install_layout_include_the_extension_manifest() {
    let root = repository_root();
    let workflow = fs::read_to_string(root.join(".github/workflows/release.yml")).expect("release workflow must exist");
    let installer =
        fs::read_to_string(root.join("packaging/windows/install-solaris.ps1")).expect("Windows installer must exist");

    assert!(workflow.contains("solaris-extension.json"));
    assert!(workflow.contains("adapter['cliCommand'] == 'solaris'"));
    assert!(workflow.contains("adapter['acpArgs'] == ['acp']"));
    assert!(installer.contains("\"solaris-extension.json\""));
    let release_please =
        fs::read_to_string(root.join("release-please-config.json")).expect("release-please config must exist");
    assert!(release_please.contains("solaris-extension.json"));
}
