#[test]
#[serial_test::serial]
fn stdio_mcp_configuration_pins_the_resolved_executable() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let executable_name = if cfg!(windows) {
        "solaris-mcp-pin-test.exe"
    } else {
        "solaris-mcp-pin-test"
    };
    let first_executable = first.path().join(executable_name);
    let second_executable = second.path().join(executable_name);
    std::fs::write(&first_executable, b"first").unwrap();
    std::fs::write(&second_executable, b"second").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&first_executable, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(&second_executable, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let original_path = std::env::var_os("PATH");
    let config = McpServerConfig {
        transport: TransportType::Stdio,
        command: Some(executable_name.into()),
        args: None,
        env: None,
        url: None,
        headers: None,
        network: Default::default(),
        deferred: None,
        startup_timeout_ms: None,
    };

    unsafe { std::env::set_var("PATH", first.path()) };
    let first_pinned = pin_mcp_server_config(&config).unwrap();
    unsafe { std::env::set_var("PATH", second.path()) };
    let second_pinned = pin_mcp_server_config(&config).unwrap();
    if let Some(path) = original_path {
        unsafe { std::env::set_var("PATH", path) };
    } else {
        unsafe { std::env::remove_var("PATH") };
    }

    assert_eq!(
        first_pinned.command.as_deref(),
        Some(first_executable.canonicalize().unwrap().to_string_lossy().as_ref())
    );
    assert_eq!(
        second_pinned.command.as_deref(),
        Some(second_executable.canonicalize().unwrap().to_string_lossy().as_ref())
    );
    assert_ne!(first_pinned.command, second_pinned.command);
}
