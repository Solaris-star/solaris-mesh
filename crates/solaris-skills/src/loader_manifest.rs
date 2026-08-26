use std::fs::File;
use std::io;

use solaris_config::file_identity::{OpenedFileIdentity, OpenedFileState, opened_file_link_count};

pub(super) fn verify_opened_manifest_state(
    file: &File,
    identity: &OpenedFileIdentity,
    expected: &OpenedFileState,
) -> io::Result<()> {
    if opened_file_link_count(file)? != 1 || identity.current_state()? != *expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "dynamic Skill manifest changed while reading",
        ));
    }
    Ok(())
}
