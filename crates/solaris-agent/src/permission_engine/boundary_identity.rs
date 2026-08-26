use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use solaris_config::file_identity::OpenedFileIdentity;
use solaris_process::{WorkspaceRootAuthority, WorkspaceRootLaunchCapability};
use solaris_types::permission::ExecutionBoundary;

#[derive(Clone, Default)]
pub(super) struct BoundaryIdentityPolicy {
    roots: Vec<BoundaryRoot>,
    invalid: bool,
}

#[derive(Clone)]
struct BoundaryRoot {
    requested_path: PathBuf,
    anchor_path: PathBuf,
    anchor_identity: Option<Arc<OpenedFileIdentity>>,
    directory_authority: Option<WorkspaceRootAuthority>,
    exact_kind: Option<RootKind>,
}

#[derive(Clone, Copy)]
enum RootKind {
    Directory,
    File,
}

impl BoundaryIdentityPolicy {
    pub(super) fn capture(boundary: &ExecutionBoundary) -> Self {
        let mut policy = Self::default();
        let mut roots = Vec::new();
        // Explicit roots remain the process workspace authority even when the
        // file-operation side of the boundary is otherwise unrestricted.
        roots.extend(boundary.readable_roots.iter());
        roots.extend(boundary.writable_roots.iter());
        roots.sort();
        roots.dedup();
        for root in roots {
            match capture_root(Path::new(root)) {
                Ok(captured) => policy.roots.push(captured),
                Err(()) => policy.invalid = true,
            }
        }
        policy
    }

    pub(super) fn validates_current_paths(&self) -> bool {
        !self.invalid && self.roots.iter().all(BoundaryRoot::validates_current_path)
    }

    pub(super) fn is_empty(&self) -> bool {
        !self.invalid && self.roots.is_empty()
    }

    pub(super) fn seal_workspace_root(&self, path: &Path) -> std::io::Result<WorkspaceRootLaunchCapability> {
        if self.invalid {
            return Err(std::io::Error::new(
                ErrorKind::PermissionDenied,
                "execution boundary root identity is invalid",
            ));
        }
        let requested = normalize_absolute(path).ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::InvalidInput,
                "workspace root path is not a normalized absolute path",
            )
        })?;
        let root = self
            .roots
            .iter()
            .find(|root| {
                root.requested_path == requested
                    && matches!(root.exact_kind, Some(RootKind::Directory))
                    && root.directory_authority.is_some()
            })
            .ok_or_else(|| {
                std::io::Error::new(
                    ErrorKind::PermissionDenied,
                    "workspace root is not an exact opened directory boundary",
                )
            })?;
        let authority = root.directory_authority.as_ref().ok_or_else(|| {
            std::io::Error::new(
                ErrorKind::PermissionDenied,
                "workspace directory authority is unavailable",
            )
        })?;
        authority.seal(&root.anchor_path)
    }
}

impl BoundaryRoot {
    fn validates_current_path(&self) -> bool {
        let identity_valid = if let Some(authority) = &self.directory_authority {
            authority.validates_current_path()
        } else if let Some(expected) = &self.anchor_identity {
            open_identity(&self.anchor_path, self.exact_kind).is_ok_and(|actual| expected.same_object(&actual))
        } else {
            false
        };
        if !identity_valid {
            return false;
        }
        self.exact_kind.is_some() || missing_suffix_stays_beneath_anchor(self)
    }
}

fn capture_root(path: &Path) -> Result<BoundaryRoot, ()> {
    let requested_path = normalize_absolute(path).ok_or(())?;
    if requested_path.exists() {
        let kind = root_kind(&requested_path).ok_or(())?;
        let (anchor_identity, directory_authority) = match kind {
            RootKind::Directory => capture_directory_root(&requested_path)?,
            RootKind::File => (Some(open_identity(&requested_path, Some(kind)).map_err(|_| ())?), None),
        };
        return Ok(BoundaryRoot {
            requested_path: requested_path.clone(),
            anchor_path: requested_path,
            anchor_identity,
            directory_authority,
            exact_kind: Some(kind),
        });
    }
    let mut anchor_path = requested_path.as_path();
    while !anchor_path.exists() {
        anchor_path = anchor_path.parent().ok_or(())?;
    }
    let anchor_path = anchor_path.to_path_buf();
    let kind = root_kind(&anchor_path).ok_or(())?;
    if !matches!(kind, RootKind::Directory) {
        return Err(());
    }
    let (anchor_identity, directory_authority) = capture_directory_root(&anchor_path)?;
    Ok(BoundaryRoot {
        requested_path,
        anchor_path,
        anchor_identity,
        directory_authority,
        exact_kind: None,
    })
}

fn capture_directory_root(
    path: &Path,
) -> Result<(Option<Arc<OpenedFileIdentity>>, Option<WorkspaceRootAuthority>), ()> {
    match WorkspaceRootAuthority::capture(path) {
        Ok(authority) => Ok((None, Some(authority))),
        Err(_) => Ok((
            Some(open_identity(path, Some(RootKind::Directory)).map_err(|_| ())?),
            None,
        )),
    }
}

fn missing_suffix_stays_beneath_anchor(root: &BoundaryRoot) -> bool {
    let Ok(suffix) = root.requested_path.strip_prefix(&root.anchor_path) else {
        return false;
    };
    let Ok(anchor_resolved) = root.anchor_path.canonicalize() else {
        return false;
    };
    let mut current = root.anchor_path.clone();
    for component in suffix.components() {
        let Component::Normal(name) = component else {
            return false;
        };
        current.push(name);
        match std::fs::symlink_metadata(&current) {
            Ok(_) => {
                let Ok(resolved) = current.canonicalize() else {
                    return false;
                };
                if !resolved.starts_with(&anchor_resolved) {
                    return false;
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => break,
            Err(_) => return false,
        }
    }
    true
}

fn root_kind(path: &Path) -> Option<RootKind> {
    let metadata = std::fs::metadata(path).ok()?;
    if metadata.is_dir() {
        Some(RootKind::Directory)
    } else if metadata.is_file() {
        Some(RootKind::File)
    } else {
        None
    }
}

fn open_identity(path: &Path, kind: Option<RootKind>) -> std::io::Result<Arc<OpenedFileIdentity>> {
    let identity = match kind {
        Some(RootKind::File) => OpenedFileIdentity::from_owned_file(std::fs::File::open(path)?)?,
        Some(RootKind::Directory) | None => {
            let directory = Dir::open_ambient_dir(path, ambient_authority())?;
            let file = directory.try_clone()?.into_std_file();
            OpenedFileIdentity::from_owned_file(file)?
        }
    };
    Ok(Arc::new(identity))
}

fn normalize_absolute(path: &Path) -> Option<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().ok()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
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
