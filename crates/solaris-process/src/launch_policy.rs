#[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use crate::{
    NetworkProxyPolicy, NetworkProxyPolicyError, ProtectedObjectIdentity, WorkspaceRootAuthority,
    WorkspaceRootLaunchCapability,
};

/// Filesystem policy applied to one process launch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ProcessLaunchPolicy {
    /// Preserve the caller's ambient filesystem access.
    #[default]
    Ambient,
    /// Restrict filesystem writes to the workspace and private process state.
    WorkspaceSandbox {
        workspace_root: PathBuf,
        workspace_capability: Option<WorkspaceRootLaunchCapability>,
        protected_roots: Vec<PathBuf>,
        protected_object_identities: Vec<ProtectedObjectIdentity>,
        network_proxy: NetworkProxyPolicy,
    },
    #[cfg(feature = "sandbox-test-fixtures")]
    #[doc(hidden)]
    WorkspaceSandboxTest {
        workspace_root: PathBuf,
        workspace_capability: Option<WorkspaceRootLaunchCapability>,
        protected_roots: Vec<PathBuf>,
        protected_object_identities: Vec<ProtectedObjectIdentity>,
        helper_path: PathBuf,
    },
}

impl ProcessLaunchPolicy {
    pub fn workspace_sandbox(
        workspace_root: impl Into<PathBuf>,
        protected_roots: impl IntoIterator<Item = PathBuf>,
    ) -> Self {
        Self::workspace_sandbox_with_identities(workspace_root, protected_roots, [])
    }

    pub fn workspace_sandbox_with_identities(
        workspace_root: impl Into<PathBuf>,
        protected_roots: impl IntoIterator<Item = PathBuf>,
        protected_object_identities: impl IntoIterator<Item = ProtectedObjectIdentity>,
    ) -> Self {
        let workspace_root = workspace_root.into();
        let workspace_capability = capture_workspace_capability(&workspace_root);
        Self::WorkspaceSandbox {
            workspace_root,
            workspace_capability,
            protected_roots: protected_roots.into_iter().collect(),
            protected_object_identities: protected_object_identities.into_iter().collect(),
            network_proxy: NetworkProxyPolicy::default(),
        }
    }

    pub fn workspace_sandbox_with_network(
        workspace_root: impl Into<PathBuf>,
        protected_roots: impl IntoIterator<Item = PathBuf>,
        protected_object_identities: impl IntoIterator<Item = ProtectedObjectIdentity>,
        permission_domains: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, NetworkProxyPolicyError> {
        let workspace_root = workspace_root.into();
        let workspace_capability = capture_workspace_capability(&workspace_root);
        Ok(Self::WorkspaceSandbox {
            workspace_root,
            workspace_capability,
            protected_roots: protected_roots.into_iter().collect(),
            protected_object_identities: protected_object_identities.into_iter().collect(),
            network_proxy: NetworkProxyPolicy::from_permission_domains(permission_domains)?,
        })
    }

    /// Build a strict sandbox policy from the object-bound authority sealed by
    /// the final process authorizer.
    pub fn workspace_sandbox_with_launch_capability_and_network(
        workspace_capability: WorkspaceRootLaunchCapability,
        protected_roots: impl IntoIterator<Item = PathBuf>,
        protected_object_identities: impl IntoIterator<Item = ProtectedObjectIdentity>,
        permission_domains: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<Self, NetworkProxyPolicyError> {
        Ok(Self::WorkspaceSandbox {
            workspace_root: workspace_capability.path().to_path_buf(),
            workspace_capability: Some(workspace_capability),
            protected_roots: protected_roots.into_iter().collect(),
            protected_object_identities: protected_object_identities.into_iter().collect(),
            network_proxy: NetworkProxyPolicy::from_permission_domains(permission_domains)?,
        })
    }

    #[cfg(all(feature = "sandbox-test-fixtures", any(unix, test)))]
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn workspace_sandbox_with_network_test_connector(
        workspace_root: impl Into<PathBuf>,
        protected_roots: impl IntoIterator<Item = PathBuf>,
        protected_object_identities: impl IntoIterator<Item = ProtectedObjectIdentity>,
        permission_domains: impl IntoIterator<Item = impl AsRef<str>>,
        approved_host: &str,
        approved_port: u16,
        resolved_ip: IpAddr,
        destination: SocketAddr,
    ) -> Result<Self, NetworkProxyPolicyError> {
        let network_proxy = NetworkProxyPolicy::from_permission_domains(permission_domains)?.with_test_connector(
            approved_host,
            approved_port,
            resolved_ip,
            destination,
        )?;
        let workspace_root = workspace_root.into();
        let workspace_capability = capture_workspace_capability(&workspace_root);
        Ok(Self::WorkspaceSandbox {
            workspace_root,
            workspace_capability,
            protected_roots: protected_roots.into_iter().collect(),
            protected_object_identities: protected_object_identities.into_iter().collect(),
            network_proxy,
        })
    }

    #[cfg(feature = "sandbox-test-fixtures")]
    #[doc(hidden)]
    pub fn workspace_sandbox_with_test_helper(
        workspace_root: impl Into<PathBuf>,
        protected_roots: impl IntoIterator<Item = PathBuf>,
        protected_object_identities: impl IntoIterator<Item = ProtectedObjectIdentity>,
        helper_path: impl Into<PathBuf>,
    ) -> Self {
        let workspace_root = workspace_root.into();
        let workspace_capability = capture_workspace_capability(&workspace_root);
        Self::WorkspaceSandboxTest {
            workspace_root,
            workspace_capability,
            protected_roots: protected_roots.into_iter().collect(),
            protected_object_identities: protected_object_identities.into_iter().collect(),
            helper_path: helper_path.into(),
        }
    }
}

fn capture_workspace_capability(workspace_root: &std::path::Path) -> Option<WorkspaceRootLaunchCapability> {
    WorkspaceRootAuthority::capture(workspace_root)
        .and_then(|authority| authority.seal(workspace_root))
        .ok()
}

#[cfg(test)]
#[path = "launch_policy_test.rs"]
mod launch_policy_test;
