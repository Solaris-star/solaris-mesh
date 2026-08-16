use std::fs;

use tempfile::tempdir;

use super::{migrate_path, migrate_project_data};

#[test]
fn migrates_directory_without_removing_legacy_data() {
    let temp = tempdir().unwrap();
    let legacy = temp.path().join("aionrs");
    let destination = temp.path().join("solaris");
    fs::create_dir_all(legacy.join("sessions")).unwrap();
    fs::write(legacy.join("config.toml"), "model = 'test'").unwrap();
    fs::write(legacy.join("sessions").join("one.json"), "{}").unwrap();

    migrate_path(&legacy, &destination).unwrap();

    assert_eq!(
        fs::read_to_string(destination.join("config.toml")).unwrap(),
        "model = 'test'"
    );
    assert_eq!(
        fs::read_to_string(destination.join("sessions").join("one.json")).unwrap(),
        "{}"
    );
    assert!(legacy.join("config.toml").exists());
}

#[test]
fn existing_destination_is_not_overwritten() {
    let temp = tempdir().unwrap();
    let legacy = temp.path().join(".aionrs.toml");
    let destination = temp.path().join(".solaris.toml");
    fs::write(&legacy, "legacy").unwrap();
    fs::write(&destination, "current").unwrap();

    migrate_path(&legacy, &destination).unwrap();

    assert_eq!(fs::read_to_string(destination).unwrap(), "current");
    assert_eq!(fs::read_to_string(legacy).unwrap(), "legacy");
}

#[test]
fn repeated_migration_is_idempotent() {
    let temp = tempdir().unwrap();
    let legacy = temp.path().join(".aionrs");
    let destination = temp.path().join(".solaris");
    fs::create_dir(&legacy).unwrap();
    fs::write(legacy.join("state.json"), "first").unwrap();

    migrate_path(&legacy, &destination).unwrap();
    fs::write(legacy.join("state.json"), "changed legacy").unwrap();
    migrate_path(&legacy, &destination).unwrap();

    assert_eq!(fs::read_to_string(destination.join("state.json")).unwrap(), "first");
}

#[test]
fn project_migration_rewrites_legacy_runtime_paths() {
    let temp = tempdir().unwrap();
    fs::write(
        temp.path().join(".aionrs.toml"),
        "# Keep this comment\n[session]\ndirectory = '.aionrs/custom-sessions'\n[plan]\nplan_directory = \".aionrs/plans\" # Keep this too\n",
    )
    .unwrap();
    fs::create_dir(temp.path().join(".aionrs")).unwrap();
    fs::write(temp.path().join(".aionrs").join("state.json"), "legacy").unwrap();

    migrate_project_data(temp.path()).unwrap();

    let config = fs::read_to_string(temp.path().join(".solaris.toml")).unwrap();
    assert!(config.contains("# Keep this comment"));
    assert!(config.contains("directory = '.solaris/custom-sessions'"));
    assert!(config.contains("plan_directory = \".solaris/plans\" # Keep this too"));
    assert_eq!(
        fs::read_to_string(temp.path().join(".solaris").join("state.json")).unwrap(),
        "legacy"
    );
    assert!(temp.path().join(".aionrs.toml").exists());
    assert!(temp.path().join(".aionrs").exists());
}

#[test]
fn project_migration_does_not_rewrite_unrelated_config_values() {
    let temp = tempdir().unwrap();
    let source = "[custom]\ndirectory = '.aionrs/custom-sessions'\nname = '.aionrs/plans'\n";
    fs::write(temp.path().join(".aionrs.toml"), source).unwrap();

    migrate_project_data(temp.path()).unwrap();

    assert_eq!(fs::read_to_string(temp.path().join(".solaris.toml")).unwrap(), source);
}

#[cfg(unix)]
#[test]
fn failed_copy_cleans_temporary_path() {
    use std::os::unix::fs::symlink;

    let temp = tempdir().unwrap();
    let legacy = temp.path().join(".aionrs");
    let destination = temp.path().join(".solaris");
    fs::create_dir(&legacy).unwrap();
    symlink(temp.path(), legacy.join("unsupported-link")).unwrap();

    assert!(migrate_path(&legacy, &destination).is_err());
    assert!(!destination.exists());
    let leftovers = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains(".solaris.migrate-"))
        .count();
    assert_eq!(leftovers, 0);
}
