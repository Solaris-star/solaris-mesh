use std::fs::{File, FileTimes, OpenOptions};
use std::io::Write;

use super::{OpenedFileIdentity, opened_file_link_count};

#[test]
fn opened_link_count_distinguishes_single_and_multiply_linked_files() {
    let directory = tempfile::tempdir().expect("tempdir");
    let original = directory.path().join("original");
    let alias = directory.path().join("alias");
    std::fs::write(&original, b"content").expect("original");
    let file = File::open(&original).expect("open single-linked file");
    assert_eq!(opened_file_link_count(&file).expect("single link count"), 1);

    std::fs::hard_link(&original, &alias).expect("hard link");
    assert_eq!(opened_file_link_count(&file).expect("hard link count"), 2);
}

#[cfg(windows)]
#[test]
fn opened_link_count_reports_an_error_for_an_invalid_handle() {
    use std::os::windows::io::RawHandle;

    let invalid_handle = (-1_isize) as RawHandle;
    assert!(super::opened_windows_handle_link_count(invalid_handle).is_err());
}

#[test]
fn identity_is_derived_from_and_keeps_an_open_handle() {
    let directory = tempfile::tempdir().expect("tempdir");
    let original = directory.path().join("original");
    let alias = directory.path().join("alias");
    std::fs::write(&original, b"protected").expect("original");
    std::fs::hard_link(&original, &alias).expect("hard link");

    let protected =
        OpenedFileIdentity::from_owned_file(File::open(&original).expect("open original")).expect("protected identity");
    let hard_link =
        OpenedFileIdentity::from_owned_file(File::open(&alias).expect("open alias")).expect("alias identity");
    std::fs::remove_file(&original).expect("remove original name");
    std::fs::write(&original, b"replacement").expect("replacement");
    let replacement = OpenedFileIdentity::from_owned_file(File::open(&original).expect("open replacement"))
        .expect("replacement identity");

    assert!(protected.same_object(&hard_link));
    assert!(!protected.same_object(&replacement));
    assert_eq!(
        protected.protected_object_identity(),
        hard_link.protected_object_identity()
    );
    assert_ne!(
        protected.protected_object_identity(),
        replacement.protected_object_identity()
    );
}

#[test]
fn state_detects_same_size_rewrite_after_modified_time_is_restored() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("mutable");
    std::fs::write(&path, b"original!").expect("original");
    let original_modified = std::fs::metadata(&path)
        .and_then(|metadata| metadata.modified())
        .expect("original modified time");
    let identity =
        OpenedFileIdentity::from_owned_file(File::open(&path).expect("open original")).expect("opened identity");
    let before = identity.current_state().expect("state before rewrite");

    let mut writer = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .expect("open writer");
    writer.write_all(b"rewritten").expect("same-size rewrite");
    writer.sync_all().expect("sync rewrite");
    writer
        .set_times(FileTimes::new().set_modified(original_modified))
        .expect("restore modified time");
    drop(writer);

    let metadata = std::fs::metadata(&path).expect("rewritten metadata");
    assert_eq!(metadata.len(), 9);
    assert_eq!(metadata.modified().expect("rewritten modified time"), original_modified);
    assert_ne!(identity.current_state().expect("state after rewrite"), before);
}
