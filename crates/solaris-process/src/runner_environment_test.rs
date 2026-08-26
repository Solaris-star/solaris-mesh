#[tokio::test]
async fn raw_command_runner_clears_explicit_arbitrary_environment() {
    const SECRET_KEY: &str = "SOLARIS_RAW_COMMAND_SECRET";
    const SECRET_VALUE: &str = "raw-command-secret-must-not-pass";

    #[cfg(windows)]
    let script = "if ($null -eq $env:SOLARIS_RAW_COMMAND_SECRET) { 'missing' } else { 'present' }";
    #[cfg(not(windows))]
    let script = "if [ -z \"${SOLARIS_RAW_COMMAND_SECRET+x}\" ]; then printf missing; else printf present; fi";
    let mut command = shell_command(script);
    command.env(SECRET_KEY, SECRET_VALUE);

    let result = CommandRunner::new(command).run().await.unwrap();

    assert_eq!(String::from_utf8_lossy(&result.stdout).trim(), "missing");
}
