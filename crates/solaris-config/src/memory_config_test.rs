use super::{MemoryConfig, MemoryConfigFile};

#[test]
fn memory_is_disabled_by_default() {
    assert_eq!(MemoryConfigFile::default().resolve(), MemoryConfig::default());
    assert!(!MemoryConfig::default().enabled);
    assert!(!MemoryConfig::default().review);
}

#[test]
fn project_can_explicitly_disable_globally_enabled_memory() {
    let merged = MemoryConfigFile::merge(
        MemoryConfigFile {
            enabled: Some(true),
            review: Some(true),
        },
        MemoryConfigFile {
            enabled: Some(false),
            review: None,
        },
    )
    .resolve();

    assert!(!merged.enabled);
    assert!(merged.review);
}

#[test]
fn toml_uses_a_native_memory_section() {
    let parsed: MemoryConfigFile = toml::from_str("enabled = true\nreview = true\n").unwrap();

    assert_eq!(
        parsed.resolve(),
        MemoryConfig {
            enabled: true,
            review: true,
        }
    );
}
