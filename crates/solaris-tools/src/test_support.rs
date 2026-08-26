#[cfg(windows)]
use std::path::Path;

#[cfg(windows)]
pub(crate) async fn create_windows_junction(target: &Path, junction: &Path) {
    fn literal(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "''"))
    }

    let shell = solaris_config::shell::resolve_shell(Some("powershell")).expect("PowerShell");
    let script = format!(
        "$ErrorActionPreference = 'Stop'; $item = New-Item -ItemType Junction -Path {} -Target {}; \
         if ($item.LinkType -ne 'Junction') {{ throw 'expected a junction' }}",
        literal(junction),
        literal(target),
    );
    let mut command = solaris_config::shell::shell_command_builder(&shell, &script, false);
    let output = command.output().await.expect("create junction");
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}
