//! One-time migration from the upstream AionRS data locations to Solaris CLI.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const LEGACY_APP_DIR: &str = "aionrs";
const APP_DIR: &str = "solaris";
const LEGACY_PROJECT_CONFIG: &str = ".aionrs.toml";
const PROJECT_CONFIG: &str = ".solaris.toml";
const LEGACY_PROJECT_DATA: &str = ".aionrs";
const PROJECT_DATA: &str = ".solaris";

/// Copy the legacy global data directory to the Solaris location when needed.
///
/// Existing Solaris data always wins. Legacy data is kept after a successful
/// migration so users can roll back without losing information.
fn migrate_global_data() -> anyhow::Result<()> {
    if let Some(config_root) = dirs::config_dir() {
        migrate_path_with(
            &config_root.join(LEGACY_APP_DIR),
            &config_root.join(APP_DIR),
            |temporary| rewrite_legacy_config_paths(&temporary.join("config.toml")),
        )?;
    }
    Ok(())
}

/// Copy legacy project configuration and data to the Solaris locations.
fn migrate_project_data(project_dir: &Path) -> anyhow::Result<()> {
    migrate_path_with(
        &project_dir.join(LEGACY_PROJECT_CONFIG),
        &project_dir.join(PROJECT_CONFIG),
        rewrite_legacy_config_paths,
    )?;
    migrate_path(&project_dir.join(LEGACY_PROJECT_DATA), &project_dir.join(PROJECT_DATA))?;
    Ok(())
}

/// Run all supported legacy data migrations.
pub fn migrate_legacy_data(project_dir: &Path) -> anyhow::Result<()> {
    migrate_global_data()?;
    migrate_project_data(project_dir)
}

fn migrate_path(source: &Path, destination: &Path) -> anyhow::Result<()> {
    migrate_path_with(source, destination, |_| Ok(()))
}

fn migrate_path_with<F>(source: &Path, destination: &Path, prepare: F) -> anyhow::Result<()>
where
    F: FnOnce(&Path) -> io::Result<()>,
{
    if destination.exists() || !source.exists() {
        return Ok(());
    }

    let parent = destination
        .parent()
        .ok_or_else(|| anyhow::anyhow!("migration destination has no parent: {}", destination.display()))?;
    fs::create_dir_all(parent)?;

    let temporary = temporary_path(destination);
    remove_temporary_path(&temporary)?;

    let result = (|| -> io::Result<()> {
        copy_path(source, &temporary)?;
        prepare(&temporary)?;

        if destination.exists() {
            remove_temporary_path(&temporary)?;
            return Ok(());
        }

        match fs::rename(&temporary, destination) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                remove_temporary_path(&temporary)?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    })();

    if let Err(error) = result {
        let _ = remove_temporary_path(&temporary);
        return Err(anyhow::anyhow!(
            "failed to migrate {} to {}: {error}",
            source.display(),
            destination.display(),
        ));
    }

    Ok(())
}

fn rewrite_legacy_config_paths(path: &Path) -> io::Result<()> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    let mut changed = false;
    let mut current_table = None;
    let rewritten = content
        .split_inclusive('\n')
        .map(|line| {
            if let Some(table) = config_table_name(line) {
                current_table = Some(table.to_string());
                return line.to_string();
            }

            let rewritten = match current_table.as_deref() {
                Some("session") => rewrite_path_assignment(line, "directory"),
                Some("plan") => rewrite_path_assignment(line, "plan_directory"),
                _ => None,
            };
            if let Some(rewritten) = rewritten {
                changed = true;
                rewritten
            } else {
                line.to_string()
            }
        })
        .collect::<String>();

    if changed {
        fs::write(path, rewritten)?;
    }
    Ok(())
}

fn config_table_name(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if let Some(table) = trimmed.strip_prefix("[[") {
        return table.split_once("]]").map(|(name, _)| name.trim());
    }

    let table = trimmed.strip_prefix('[')?;
    table.split_once(']').map(|(name, _)| name.trim())
}
fn rewrite_path_assignment(line: &str, key: &str) -> Option<String> {
    let key_start = line.len() - line.trim_start().len();
    let after_key = line.get(key_start..)?.strip_prefix(key)?;
    let value_with_spacing = after_key.trim_start().strip_prefix('=')?;
    let value = value_with_spacing.trim_start();
    let quote = value.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }

    let quoted_value = value.get(1..)?;
    let value_end = quoted_value.find(quote)?;
    let configured_path = quoted_value.get(..value_end)?;
    if configured_path != LEGACY_PROJECT_DATA
        && !configured_path.starts_with(".aionrs/")
        && !configured_path.starts_with(".aionrs\\")
    {
        return None;
    }

    let value_start = line.len() - value.len() + 1;
    let value_end = value_start + configured_path.len();
    let migrated_path = configured_path.replacen(LEGACY_PROJECT_DATA, PROJECT_DATA, 1);
    Some(format!(
        "{}{}{}",
        &line[..value_start],
        migrated_path,
        &line[value_end..]
    ))
}

fn copy_path(source: &Path, destination: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("legacy path is a symbolic link: {}", source.display()),
        ));
    }

    if metadata.is_file() {
        fs::copy(source, destination)?;
        return Ok(());
    }

    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("legacy path is not a file or directory: {}", source.display()),
        ));
    }

    fs::create_dir(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        copy_path(&entry.path(), &destination.join(entry.file_name()))?;
    }
    Ok(())
}

fn remove_temporary_path(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn temporary_path(destination: &Path) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let name = destination
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("solaris");
    destination.with_file_name(format!(".{name}.migrate-{}-{nonce}", std::process::id()))
}

#[cfg(test)]
#[path = "migration_test.rs"]
mod migration_test;
