use std::os::unix::fs::PermissionsExt;

use super::{CA_CERTIFICATE_FILE_NAME, TlsAuthority, default_upstream_config};

#[test]
fn ephemeral_ca_file_contains_only_a_read_only_certificate_and_is_removed() {
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let authority = TlsAuthority::create(&socket, default_upstream_config().unwrap()).unwrap();
    let ca_path = authority.certificate_path().to_path_buf();
    let contents = std::fs::read_to_string(&ca_path).unwrap();
    let mode = std::fs::metadata(&ca_path).unwrap().permissions().mode();

    assert_eq!(ca_path.file_name().unwrap(), CA_CERTIFICATE_FILE_NAME);
    assert!(contents.contains("BEGIN CERTIFICATE"));
    assert!(!contents.contains("PRIVATE KEY"));
    assert_eq!(mode & 0o222, 0);
    authority.verify_certificate_file().unwrap();

    drop(authority);
    assert!(!ca_path.exists());
}

#[test]
fn changed_ca_path_identity_is_rejected_and_not_deleted() {
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let authority = TlsAuthority::create(&socket, default_upstream_config().unwrap()).unwrap();
    let ca_path = authority.certificate_path().to_path_buf();
    std::fs::remove_file(&ca_path).unwrap();
    std::fs::write(&ca_path, b"replacement").unwrap();

    assert!(authority.verify_certificate_file().is_err());
    drop(authority);
    assert_eq!(std::fs::read(&ca_path).unwrap(), b"replacement");
}

#[test]
fn preexisting_ca_file_is_not_deleted_when_authority_creation_fails() {
    let state = tempfile::tempdir().unwrap();
    let socket = state.path().join("proxy.sock");
    let ca_path = state.path().join(CA_CERTIFICATE_FILE_NAME);
    std::fs::write(&ca_path, b"preexisting").unwrap();

    let error = TlsAuthority::create(&socket, default_upstream_config().unwrap())
        .err()
        .expect("preexisting CA path must be rejected");

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&ca_path).unwrap(), b"preexisting");
}
