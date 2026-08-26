use std::path::Path;

use solaris_types::plugin::PluginDefinition;

pub fn load_plugin_manifest(path: &Path) -> Result<PluginDefinition, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read plugin manifest {}: {error}", path.display()))?;
    match path.extension().and_then(|value| value.to_str()) {
        Some("json") => {
            serde_json::from_str(&content).map_err(|error| format!("invalid JSON plugin manifest: {error}"))
        }
        _ => Err(format!(
            "unsupported plugin manifest format for {}; JSON is supported in the 0.3 runtime",
            path.display()
        )),
    }
}
