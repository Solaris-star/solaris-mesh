use std::ffi::OsString;

use super::resource_runtime_env_from;

#[test]
fn resource_runtime_env_only_keeps_supported_resource_limits() {
    let values = vec![
        (OsString::from("UNRELATED_SECRET"), OsString::from("must-not-pass")),
        (OsString::from("SOLARIS_MAX_RUN_TOKENS"), OsString::from("32000")),
        (OsString::from("SOLARIS_MAX_ACTIVE_AGENTS"), OsString::from("4")),
        (
            OsString::from("SOLARIS_MAX_RUN_COST"),
            OsString::from("secret-in-safe-key"),
        ),
    ];

    assert_eq!(
        resource_runtime_env_from(values),
        vec![
            ("SOLARIS_MAX_ACTIVE_AGENTS".to_string(), "4".to_string()),
            ("SOLARIS_MAX_RUN_TOKENS".to_string(), "32000".to_string()),
        ]
    );
}
