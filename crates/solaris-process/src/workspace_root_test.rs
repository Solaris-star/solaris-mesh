use super::*;

#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[test]
fn replacement_after_seal_does_not_change_the_capability_identity() {
    let parent = tempfile::tempdir().unwrap();
    let workspace = parent.path().join("workspace");
    let original = parent.path().join("original");
    std::fs::create_dir(&workspace).unwrap();
    let authority = WorkspaceRootAuthority::capture(&workspace).unwrap();
    let capability = authority.seal(&workspace).unwrap();

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;

        let expected = capability
            .duplicate_linux_directory()
            .unwrap()
            .metadata()
            .unwrap()
            .ino();
        std::fs::rename(&workspace, &original).unwrap();
        std::fs::create_dir(&workspace).unwrap();
        let retained = capability
            .duplicate_linux_directory()
            .unwrap()
            .metadata()
            .unwrap()
            .ino();
        assert_eq!(retained, expected);
        assert_ne!(std::fs::metadata(&workspace).unwrap().ino(), expected);
    }

    #[cfg(target_os = "macos")]
    {
        use std::os::unix::fs::MetadataExt;

        let expected = capability
            .duplicate_macos_directory()
            .unwrap()
            .metadata()
            .unwrap()
            .ino();
        std::fs::rename(&workspace, &original).unwrap();
        std::fs::create_dir(&workspace).unwrap();
        let retained = capability
            .duplicate_macos_directory()
            .unwrap()
            .metadata()
            .unwrap()
            .ino();
        assert_eq!(retained, expected);
        assert_ne!(std::fs::metadata(&workspace).unwrap().ino(), expected);
        assert_eq!(
            capability.verify_macos_path_identity().unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[cfg(windows)]
    {
        let error = std::fs::rename(&workspace, &original).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(32), "expected ERROR_SHARING_VIOLATION");
        drop(capability);
        drop(authority);
        std::fs::rename(&workspace, &original).unwrap();
    }
}

#[cfg(target_os = "macos")]
#[test]
fn macos_authority_seals_with_a_retained_directory_handle() {
    let workspace = tempfile::tempdir().unwrap();
    let authority = WorkspaceRootAuthority::capture(workspace.path()).unwrap();

    let capability = authority.seal(workspace.path()).unwrap();

    capability.verify_macos_path_identity().unwrap();
    assert_eq!(capability.launch_path(), workspace.path().canonicalize().unwrap());
    assert!(
        capability
            .duplicate_macos_directory()
            .unwrap()
            .metadata()
            .unwrap()
            .is_dir()
    );
}
