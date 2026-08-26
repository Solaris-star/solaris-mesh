use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::Dir;
use solaris_config::file_identity::OpenedFileIdentity;
use solaris_process::ProtectedObjectIdentity;
use solaris_types::effect::EffectDescriptor;

// Runtime state normally uses only a root plus a few SQLite files. Each root,
// slot, and file-identity history is capped at 64. Since a slot owns a parent
// directory plus its identity handle, one policy retains at most 256 live OS
// handles. We never evict an old identity: capacity exhaustion fails closed so
// a previously protected, renamed object cannot become accessible again.
const MAX_RETAINED_IDENTITIES: usize = 64;
const RETAINED_IDENTITY_CAPACITY_ERROR: &str = "protected runtime retained identity capacity exhausted";

#[derive(Clone)]
struct ProtectedRoot {
    path: PathBuf,
    identity: Arc<OpenedFileIdentity>,
}

#[derive(Clone)]
struct ProtectedFileSlot {
    parent: Arc<Dir>,
    parent_identity: Arc<OpenedFileIdentity>,
    file_name: OsString,
}

#[derive(Clone, Default)]
pub(super) struct ProtectedPathPolicy {
    roots: Vec<ProtectedRoot>,
    files: Vec<PathBuf>,
    slots: Vec<ProtectedFileSlot>,
    file_identities: Vec<Arc<OpenedFileIdentity>>,
    retention_exhausted: bool,
}

impl ProtectedPathPolicy {
    pub(super) fn is_empty(&self) -> bool {
        !self.retention_exhausted && self.roots.is_empty() && self.files.is_empty()
    }

    pub(super) fn register(
        &mut self,
        root: impl AsRef<Path>,
        state_paths: impl IntoIterator<Item = PathBuf>,
    ) -> Result<(), String> {
        self.ensure_retention_available()?;
        let root_path =
            resolve_policy_path(root.as_ref()).ok_or_else(|| "failed to resolve protected runtime root".to_owned())?;
        let root_directory = Dir::open_ambient_dir(&root_path, ambient_authority())
            .map_err(|_| "failed to open protected runtime root".to_owned())?;
        let root_identity = directory_identity(&root_directory)?;
        let new_root = !self
            .roots
            .iter()
            .any(|registered| registered.identity.same_object(&root_identity));
        if new_root {
            self.ensure_capacity(self.roots.len())?;
            self.roots.push(ProtectedRoot {
                path: root_path,
                identity: root_identity,
            });
        }

        for requested in state_paths {
            let path = resolve_policy_path(&requested)
                .ok_or_else(|| "failed to resolve protected runtime state path".to_owned())?;
            let slot = open_file_slot(&path)?;
            let identity = open_slot_identity(&slot)?;
            let new_path = !self.files.contains(&path);
            let new_slot = !self.slots.iter().any(|registered| same_slot(registered, &slot));
            let new_identity = identity.as_ref().is_some_and(|candidate| {
                !self
                    .file_identities
                    .iter()
                    .any(|registered| registered.same_object(candidate))
            });
            if new_path {
                self.ensure_capacity(self.files.len())?;
            }
            if new_slot {
                self.ensure_capacity(self.slots.len())?;
            }
            if new_identity {
                self.ensure_capacity(self.file_identities.len())?;
            }
            if new_path {
                self.files.push(path);
            }
            if new_slot {
                self.slots.push(slot);
            }
            if let Some(identity) = identity
                && new_identity
            {
                self.file_identities.push(identity);
            }
        }
        self.roots.sort_by(|left, right| left.path.cmp(&right.path));
        self.files.sort();
        Ok(())
    }

    pub(super) fn refresh_file_identities(&mut self) -> Result<(), String> {
        self.ensure_retention_available()?;
        for index in 0..self.slots.len() {
            if let Some(identity) = open_slot_identity(&self.slots[index])? {
                self.remember_file_identity(identity)?;
            }
        }
        Ok(())
    }

    pub(super) fn merge(&mut self, other: &Self) {
        if self.retention_exhausted || other.retention_exhausted {
            self.retention_exhausted = true;
            return;
        }
        for root in &other.roots {
            if !self
                .roots
                .iter()
                .any(|registered| registered.identity.same_object(&root.identity))
            {
                if self.ensure_capacity(self.roots.len()).is_err() {
                    return;
                }
                self.roots.push(root.clone());
            }
        }
        for file in &other.files {
            if !self.files.contains(file) {
                if self.ensure_capacity(self.files.len()).is_err() {
                    return;
                }
                self.files.push(file.clone());
            }
        }
        for slot in &other.slots {
            if !self.slots.iter().any(|registered| same_slot(registered, slot)) {
                if self.ensure_capacity(self.slots.len()).is_err() {
                    return;
                }
                self.slots.push(slot.clone());
            }
        }
        for identity in &other.file_identities {
            if self.remember_file_identity(Arc::clone(identity)).is_err() {
                return;
            }
        }
        self.roots.sort_by(|left, right| left.path.cmp(&right.path));
        self.files.sort();
    }

    pub(super) fn protects_descriptor(&self, descriptor: &EffectDescriptor) -> bool {
        let paths = descriptor
            .resources
            .file_reads
            .iter()
            .chain(&descriptor.resources.file_writes);
        if self.retention_exhausted {
            return paths.count() > 0;
        }
        paths.into_iter().any(|path| self.protects(Path::new(path)))
    }

    pub(super) fn protects(&self, path: &Path) -> bool {
        if self.retention_exhausted {
            return true;
        }
        let Some(path) = resolve_policy_path(path) else {
            return false;
        };
        self.protects_resolved(&path)
    }

    pub(super) fn protects_traversed_file(&self, path: &Path) -> bool {
        if self.retention_exhausted {
            return true;
        }
        let Some(path) = normalize_absolute_path(path) else {
            return false;
        };
        self.protects_resolved(&path)
    }

    pub(super) fn protects_opened_directory(&self, path: &Path, identity: &OpenedFileIdentity) -> bool {
        self.retention_exhausted
            || self.protects_traversed_file(path)
            || self.roots.iter().any(|root| root.identity.same_object(identity))
    }

    pub(super) fn protects_file_slot(
        &self,
        path: &Path,
        parent_identity: &OpenedFileIdentity,
        file_name: &OsStr,
    ) -> bool {
        self.retention_exhausted
            || self.protects_traversed_file(path)
            || self
                .slots
                .iter()
                .any(|slot| slot.file_name == file_name && slot.parent_identity.same_object(parent_identity))
    }

    pub(super) fn protects_opened_file(
        &mut self,
        path: &Path,
        parent_identity: &OpenedFileIdentity,
        file_name: &OsStr,
        identity: Arc<OpenedFileIdentity>,
    ) -> bool {
        if self.retention_exhausted {
            return true;
        }
        let protected_slot = self.protects_file_slot(path, parent_identity, file_name);
        let protected_identity = self
            .file_identities
            .iter()
            .any(|registered| registered.same_object(&identity));
        if protected_slot && self.remember_file_identity(identity).is_err() {
            return true;
        }
        protected_slot || protected_identity
    }

    fn protects_resolved(&self, path: &Path) -> bool {
        self.retention_exhausted
            || self.roots.iter().any(|root| path.starts_with(&root.path))
            || self.files.iter().any(|file| file.as_path() == path)
    }

    fn ensure_retention_available(&self) -> Result<(), String> {
        if self.retention_exhausted {
            Err(RETAINED_IDENTITY_CAPACITY_ERROR.to_owned())
        } else {
            Ok(())
        }
    }

    fn ensure_capacity(&mut self, retained: usize) -> Result<(), String> {
        if retained < MAX_RETAINED_IDENTITIES {
            return Ok(());
        }
        if !self.retention_exhausted {
            tracing::error!(
                retained_identity_limit = MAX_RETAINED_IDENTITIES,
                "protected runtime identity retention exhausted; filesystem access will fail closed"
            );
        }
        self.retention_exhausted = true;
        Err(RETAINED_IDENTITY_CAPACITY_ERROR.to_owned())
    }

    fn remember_file_identity(&mut self, candidate: Arc<OpenedFileIdentity>) -> Result<(), String> {
        if self
            .file_identities
            .iter()
            .any(|identity| identity.same_object(&candidate))
        {
            return Ok(());
        }
        self.ensure_capacity(self.file_identities.len())?;
        self.file_identities.push(candidate);
        Ok(())
    }

    pub(super) fn fingerprint_material(&self) -> Vec<String> {
        let mut material = self
            .roots
            .iter()
            .map(|root| format!("root:{}", root.path.display()))
            .chain(self.files.iter().map(|path| format!("file:{}", path.display())))
            .collect::<Vec<_>>();
        if self.retention_exhausted {
            material.push("retained-identities:exhausted".to_owned());
        }
        material
    }

    pub(super) fn snapshot(&self) -> Vec<PathBuf> {
        let mut paths = self
            .roots
            .iter()
            .map(|root| root.path.clone())
            .chain(self.files.iter().cloned())
            .collect::<Vec<_>>();
        paths.sort();
        paths.dedup();
        paths
    }

    pub(super) fn process_snapshot(&mut self) -> Result<(Vec<PathBuf>, Vec<ProtectedObjectIdentity>), String> {
        self.refresh_file_identities()?;
        let mut identities = Vec::with_capacity(self.file_identities.len());
        for retained in &self.file_identities {
            let identity = retained
                .protected_object_identity()
                .ok_or_else(|| "protected runtime object identity is unavailable".to_owned())?;
            if !identities.contains(&identity) {
                identities.push(identity);
            }
        }
        Ok((self.snapshot(), identities))
    }
}

fn same_slot(left: &ProtectedFileSlot, right: &ProtectedFileSlot) -> bool {
    left.file_name == right.file_name && left.parent_identity.same_object(&right.parent_identity)
}

fn open_file_slot(path: &Path) -> Result<ProtectedFileSlot, String> {
    let parent_path = path
        .parent()
        .ok_or_else(|| "protected runtime state path has no parent".to_owned())?;
    let file_name = path
        .file_name()
        .ok_or_else(|| "protected runtime state path has no file name".to_owned())?
        .to_os_string();
    let parent = Dir::open_ambient_dir(parent_path, ambient_authority())
        .map_err(|_| "failed to open protected runtime state parent".to_owned())?;
    let parent_identity = directory_identity(&parent)?;
    Ok(ProtectedFileSlot {
        parent: Arc::new(parent),
        parent_identity,
        file_name,
    })
}

fn open_slot_identity(slot: &ProtectedFileSlot) -> Result<Option<Arc<OpenedFileIdentity>>, String> {
    match slot.parent.open(&slot.file_name) {
        Ok(file) => file_identity(file).map(Some),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(_) => Err("failed to refresh protected runtime state identity".to_owned()),
    }
}

fn file_identity(file: cap_std::fs::File) -> Result<Arc<OpenedFileIdentity>, String> {
    OpenedFileIdentity::from_owned_file(file.into_std())
        .map(Arc::new)
        .map_err(|_| "failed to identify protected runtime state".to_owned())
}

fn directory_identity(directory: &Dir) -> Result<Arc<OpenedFileIdentity>, String> {
    let file = directory
        .try_clone()
        .map_err(|_| "failed to retain protected directory handle".to_owned())?
        .into_std_file();
    OpenedFileIdentity::from_owned_file(file)
        .map(Arc::new)
        .map_err(|_| "failed to identify protected directory".to_owned())
}

pub(super) fn permission_path_matches(resource: &str, allowed: &str) -> bool {
    let resource_path = Path::new(resource);
    let allowed_path = Path::new(allowed);
    if resource_path.is_absolute() || allowed_path.is_absolute() {
        return resource_path.is_absolute() && allowed_path.is_absolute() && path_within(resource, allowed);
    }
    lexical_relative_path(resource_path)
        .zip(lexical_relative_path(allowed_path))
        .is_some_and(|(resource, allowed)| resource.starts_with(allowed))
}

fn lexical_relative_path(path: &Path) -> Option<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(value) => normalized.push(value),
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => return None,
        }
    }
    Some(normalized)
}

pub(super) fn path_within(resource: &str, root: &str) -> bool {
    let resource = resolve_boundary_path(Path::new(resource));
    let root = resolve_boundary_path(Path::new(root));
    resource
        .zip(root)
        .is_some_and(|(resource, root)| resource.starts_with(root))
}

fn resolve_policy_path(path: &Path) -> Option<PathBuf> {
    let normalized = normalize_absolute_path(path)?;
    resolve_existing_path(&normalized)
}

fn resolve_boundary_path(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let normalized = normalize_absolute_path(path)?;
    resolve_existing_path(&normalized)
}

fn resolve_existing_path(normalized: &Path) -> Option<PathBuf> {
    let mut existing = normalized;
    let mut missing = Vec::new();
    while !existing.exists() {
        missing.push(existing.file_name()?.to_os_string());
        existing = existing.parent()?;
    }
    let mut resolved = existing.canonicalize().ok()?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Some(resolved)
}

fn normalize_absolute_path(path: &Path) -> Option<PathBuf> {
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

#[cfg(test)]
#[path = "protected_paths_test.rs"]
mod protected_paths_test;
