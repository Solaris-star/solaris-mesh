use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use solaris_config::file_identity::OpenedFileIdentity;
use solaris_tools::write::WorkspaceSearchPolicy;
use solaris_types::permission::PermissionMode;

use super::PermissionContext;

#[derive(Clone)]
pub(super) struct PermissionWorkspaceSearchPolicy {
    permissions: PermissionContext,
    execution_mode: Option<PermissionMode>,
}

impl PermissionWorkspaceSearchPolicy {
    pub(super) fn new(permissions: PermissionContext) -> Self {
        Self {
            permissions,
            execution_mode: None,
        }
    }

    fn mode(&self) -> PermissionMode {
        self.execution_mode.unwrap_or_else(|| self.permissions.mode())
    }
}

impl WorkspaceSearchPolicy for PermissionWorkspaceSearchPolicy {
    fn refresh(&self) -> Result<(), String> {
        if self.mode() == PermissionMode::Bypass {
            return Ok(());
        }
        if !self.permissions.boundary_identities_valid() {
            return Err("execution boundary root identity changed".to_owned());
        }
        self.permissions
            .protected_paths
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .refresh_file_identities()
    }

    fn allows_read(&self, path: &Path) -> bool {
        self.mode() == PermissionMode::Bypass
            || (self.permissions.boundary_identities_valid() && !self.permissions.is_protected_path(path))
    }

    fn requires_opened_file_identity(&self) -> bool {
        self.mode() != PermissionMode::Bypass
            && (self.permissions.boundary_requires_opened_identity()
                || !self
                    .permissions
                    .protected_paths
                    .read()
                    .unwrap_or_else(|error| error.into_inner())
                    .is_empty())
    }

    fn allows_file(&self, path: &Path) -> bool {
        self.mode() == PermissionMode::Bypass
            || (self.permissions.boundary_identities_valid() && !self.permissions.is_protected_traversed_file(path))
    }

    fn allows_opened_directory(&self, path: &Path, identity: Arc<OpenedFileIdentity>) -> bool {
        self.mode() == PermissionMode::Bypass
            || (self.permissions.boundary_identities_valid()
                && !self.permissions.is_protected_opened_directory(path, &identity))
    }

    fn allows_file_slot(&self, path: &Path, parent_identity: &OpenedFileIdentity, file_name: &OsStr) -> bool {
        self.mode() == PermissionMode::Bypass
            || (self.permissions.boundary_identities_valid()
                && !self
                    .permissions
                    .is_protected_file_slot(path, parent_identity, file_name))
    }

    fn allows_opened_file(
        &self,
        path: &Path,
        parent_identity: &OpenedFileIdentity,
        file_name: &OsStr,
        identity: Arc<OpenedFileIdentity>,
    ) -> bool {
        self.mode() == PermissionMode::Bypass
            || (self.permissions.boundary_identities_valid()
                && !self
                    .permissions
                    .is_protected_opened_file(path, parent_identity, file_name, identity))
    }

    fn allows_ambient_paths(&self) -> bool {
        self.mode() == PermissionMode::Bypass
    }

    fn scoped_to_permission_mode(&self, mode: PermissionMode) -> Option<Arc<dyn WorkspaceSearchPolicy>> {
        Some(Arc::new(Self {
            permissions: self.permissions.clone(),
            execution_mode: Some(mode),
        }))
    }
}
