use std::io;
use std::path::{Component, Path, PathBuf};

use super::{SandboxError, sandbox_io_error};
use crate::ProtectedObjectIdentity;
use crate::WorkspaceRootLaunchCapability;

#[cfg_attr(not(any(target_os = "linux", target_os = "macos")), allow(dead_code))]
pub(super) struct ResolvedSandboxLayout {
    pub(super) workspace_root: PathBuf,
    pub(super) workspace_capability: WorkspaceRootLaunchCapability,
    pub(super) protected_roots: Vec<PathBuf>,
    pub(super) protected_object_identities: Vec<ProtectedObjectIdentity>,
    pub(super) _test_helper_path: Option<PathBuf>,
}

#[cfg(test)]
pub(super) fn validate_layout(workspace_root: &Path, protected_roots: &[PathBuf]) -> io::Result<ResolvedSandboxLayout> {
    let authority = crate::WorkspaceRootAuthority::capture(workspace_root)?;
    let capability = authority.seal(workspace_root)?;
    validate_layout_with_identities(&capability, protected_roots, &[])
}

pub(super) fn validate_layout_with_identities(
    workspace_capability: &WorkspaceRootLaunchCapability,
    protected_roots: &[PathBuf],
    protected_object_identities: &[ProtectedObjectIdentity],
) -> io::Result<ResolvedSandboxLayout> {
    let workspace = workspace_capability.launch_path().to_path_buf();
    let mut protected_roots = protected_roots
        .iter()
        .map(|protected| resolve_path(protected))
        .collect::<io::Result<Vec<_>>>()?;
    for protected in &protected_roots {
        if protected == &workspace
            || workspace.starts_with(protected)
            || directory_identity(protected).is_ok_and(|identity| identity == workspace_capability.identity())
        {
            return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
        }
    }
    protected_roots.sort_by(|left, right| {
        left.components()
            .count()
            .cmp(&right.components().count())
            .then_with(|| left.cmp(right))
    });
    let mut outermost = Vec::with_capacity(protected_roots.len());
    for protected in protected_roots {
        if !outermost.iter().any(|root: &PathBuf| protected.starts_with(root)) {
            outermost.push(protected);
        }
    }
    Ok(ResolvedSandboxLayout {
        workspace_root: workspace,
        workspace_capability: workspace_capability.clone(),
        protected_roots: outermost,
        protected_object_identities: protected_object_identities.to_vec(),
        _test_helper_path: None,
    })
}

fn directory_identity(path: &Path) -> io::Result<ProtectedObjectIdentity> {
    crate::WorkspaceRootAuthority::capture(path).map(|authority| authority.identity())
}

pub(super) fn resolve_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?
            .join(path)
    };
    let normalized =
        normalize_absolute_path(&absolute).ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?;
    let mut existing = normalized.as_path();
    let mut missing = Vec::new();
    while !existing.exists() {
        missing.push(
            existing
                .file_name()
                .ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?
                .to_os_string(),
        );
        existing = existing
            .parent()
            .ok_or_else(|| sandbox_io_error(SandboxError::PreparationFailed))?;
    }
    let mut resolved = existing
        .canonicalize()
        .map_err(|_| sandbox_io_error(SandboxError::PreparationFailed))?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn normalize_absolute_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return None;
                }
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Some(normalized)
}

#[cfg(any(test, target_os = "linux", target_os = "macos"))]
pub(super) fn ensure_protected_roots_are_disjoint(
    allowed_roots: &[PathBuf],
    protected_roots: &[PathBuf],
) -> io::Result<()> {
    if protected_roots.iter().any(|protected| {
        allowed_roots
            .iter()
            .any(|allowed| protected.starts_with(allowed) || allowed.starts_with(protected))
    }) {
        return Err(sandbox_io_error(SandboxError::UnrepresentableSandboxLayout));
    }
    Ok(())
}
