use std::fs::{self, File};
use std::io::ErrorKind;

use cap_std::fs::Dir;

use super::{SecureDirectory, sync_directory};

#[cfg(target_os = "linux")]
#[test]
fn atomic_write_and_retry_sync_a_path_only_directory_capability() {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    use super::WriteStep;

    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap();
    let mut secured = SecureDirectory::open_existing(&root).unwrap();
    let path_only = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_DIRECTORY)
        .open(&root)
        .unwrap();
    assert_eq!(path_only.sync_all().unwrap_err().raw_os_error(), Some(libc::EBADF));
    secured.directory = Dir::from_std_file(path_only);
    let mut steps = Vec::new();

    secured
        .write_atomically("output.blob", b"durable-output", |step| steps.push(step))
        .unwrap();

    assert_eq!(
        steps,
        [WriteStep::TemporarySynced, WriteStep::Renamed, WriteStep::ParentSynced]
    );
    steps.clear();
    secured
        .write_atomically("output.blob", b"durable-output", |step| steps.push(step))
        .unwrap();
    assert_eq!(steps, [WriteStep::ParentSynced]);
    drop(secured);
    let reopened = SecureDirectory::open_existing(&root).unwrap();
    assert_eq!(reopened.read_file("output.blob").unwrap(), b"durable-output");
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
}

#[test]
fn sync_retains_directory_authority_after_rename_but_writes_reject_the_stale_path() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap().join("output");
    let moved = root.with_file_name("moved");
    fs::create_dir(&root).unwrap();
    let secured = SecureDirectory::open_existing(&root).unwrap();
    fs::rename(&root, &moved).unwrap();

    sync_directory(&secured.directory, &root).unwrap();

    let mut steps = Vec::new();
    let error = secured
        .write_atomically("output.blob", b"must-not-write", |step| steps.push(step))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(steps.is_empty());
    assert!(fs::read_dir(&moved).unwrap().next().is_none());

    fs::create_dir(&root).unwrap();
    let error = secured
        .write_atomically("output.blob", b"must-not-write", |_| {})
        .unwrap_err();
    assert!(error.to_string().contains("identity changed"));
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    assert!(fs::read_dir(&moved).unwrap().next().is_none());
}

#[cfg(target_os = "linux")]
#[test]
fn sync_retains_an_unlinked_directory_but_writes_reject_the_missing_path() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().canonicalize().unwrap().join("output");
    fs::create_dir(&root).unwrap();
    let secured = SecureDirectory::open_existing(&root).unwrap();
    fs::remove_dir(&root).unwrap();

    sync_directory(&secured.directory, &root).unwrap();

    let mut steps = Vec::new();
    let error = secured
        .write_atomically("output.blob", b"must-not-write", |step| steps.push(step))
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::NotFound);
    assert!(steps.is_empty());
    assert!(!root.exists());
}

#[test]
fn sync_propagates_failure_to_open_the_retained_directory() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("regular-file");
    fs::write(&path, b"keep").unwrap();
    let invalid_directory = Dir::from_std_file(File::open(&path).unwrap());

    let error = sync_directory(&invalid_directory, &path).unwrap_err();

    assert_eq!(error.raw_os_error(), Some(libc::ENOTDIR));
    assert_eq!(fs::read(&path).unwrap(), b"keep");
}

#[cfg(target_os = "linux")]
#[test]
fn sync_propagates_filesystem_flush_failure() {
    use std::path::Path;

    use cap_std::ambient_authority;

    // Linux procfs permits opening directories but rejects fsync with EINVAL.
    let path = Path::new("/proc");
    let directory = Dir::open_ambient_dir(path, ambient_authority()).unwrap();

    let error = sync_directory(&directory, path).unwrap_err();

    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
}
