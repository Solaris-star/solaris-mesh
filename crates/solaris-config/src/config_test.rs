use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compat::{MessageCompat, ReasoningCompat, SchemaCompat, ToolCompat, ToolWireShape, TransportCompat};
    use solaris_types::provider_contract::CacheTokenAccounting;

    include!("config_layering_test.rs");
    include!("config_provider_test.rs");
    include!("config_file_cache_test.rs");
    include!("config_resolve_test.rs");
    include!("config_multi_agent_test.rs");
}
