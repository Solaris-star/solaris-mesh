use std::io;
use std::path::Path;

#[cfg(windows)]
pub(super) fn create_directory_redirect(target: &Path, junction: &Path) -> io::Result<()> {
    fn literal(path: &Path) -> String {
        format!("'{}'", path.to_string_lossy().replace('\'', "''"))
    }

    let shell = solaris_config::shell::resolve_shell(Some("powershell"))
        .map_err(|source| io::Error::other(source.to_string()))?;
    let script = format!(
        "$ErrorActionPreference = 'Stop'; \
         $item = New-Item -ItemType Junction -Path {} -Target {}; \
         if ($item.LinkType -ne 'Junction') {{ throw 'expected a junction' }}",
        literal(junction),
        literal(target),
    );
    let mut command = solaris_config::shell::shell_command_builder(&shell, &script, false);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    let output = runtime.block_on(command.output())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "junction creation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

#[cfg(unix)]
pub(super) fn create_directory_redirect(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
pub(super) fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
pub(super) fn create_file_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
pub(super) fn optional_symlink_created(result: io::Result<()>, case: &str) -> bool {
    match result {
        Ok(()) => true,
        Err(source) if source.kind() == io::ErrorKind::PermissionDenied || source.raw_os_error() == Some(1314) => {
            eprintln!(
                "SKIP {case}: file symlink privilege is unavailable; Junction tests still cover Windows reparse points"
            );
            false
        }
        Err(source) => panic!("failed to create {case}: {source}"),
    }
}

#[cfg(unix)]
pub(super) fn optional_symlink_created(result: io::Result<()>, case: &str) -> bool {
    result.unwrap_or_else(|source| panic!("failed to create {case}: {source}"));
    true
}
