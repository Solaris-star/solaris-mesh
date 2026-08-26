use std::io::{self, ErrorKind};
use std::path::{Component, Path, PathBuf};

use solaris_types::identity::{EffectId, RunId};

use super::stable_digest_bytes;
use crate::runtime_ledger::RuntimeLedger;

#[path = "output_store/secure_directory.rs"]
mod secure_directory;

use secure_directory::{SecureDirectory, paths_refer_to_same_directory};

pub(crate) fn write_protected_blob(root: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    SecureDirectory::open_or_create(root)?.write_atomically(name, bytes, |_| {})
}

pub(crate) fn read_protected_blob(root: &Path, name: &str, max_bytes: u64) -> io::Result<Vec<u8>> {
    SecureDirectory::open_existing(root)?.read_file_bounded(name, max_bytes)
}

pub(crate) fn delete_local_run_outputs(run_id: &RunId, ledger: &dyn RuntimeLedger) -> io::Result<()> {
    let root = ledger.effect_output_root().ok_or_else(|| {
        io::Error::new(
            ErrorKind::Unsupported,
            "runtime ledger has no local effect output directory",
        )
    })?;
    let directory = match SecureDirectory::open_existing(&root) {
        Ok(directory) => directory,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if directory.same_as_path(&effect_output_state_root())? {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "legacy global effect output directory is read-only",
        ));
    }
    directory.remove_child_directory(&stable_digest_bytes(run_id.as_str().as_bytes()))
}

pub(crate) struct EffectOutputStore {
    root: Option<PathBuf>,
    legacy_root: Option<PathBuf>,
}

impl EffectOutputStore {
    pub(crate) fn for_run_with_ledger(run_id: &RunId, ledger: &dyn RuntimeLedger) -> Self {
        let legacy_root = legacy_run_root(run_id);
        let root = ledger
            .effect_output_root()
            .map(|root| root.join(stable_digest_bytes(run_id.as_str().as_bytes())));
        let legacy_root = (root.as_ref() != Some(&legacy_root)).then_some(legacy_root);
        Self { root, legacy_root }
    }

    #[cfg(test)]
    pub(crate) fn for_legacy_run(run_id: &RunId) -> Self {
        Self {
            root: Some(legacy_run_root(run_id)),
            legacy_root: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn write_legacy_fixture(&self, output: &str) -> io::Result<String> {
        let root = self
            .root
            .as_ref()
            .ok_or_else(|| io::Error::new(ErrorKind::Unsupported, "legacy fixture has no output directory"))?;
        let digest = stable_digest_bytes(output.as_bytes());
        let reference = format!("sha256-{digest}.blob");
        SecureDirectory::open_or_create(root)?.write_atomically(&reference, output.as_bytes(), |_| {})?;
        Ok(reference)
    }

    pub(super) fn write(&self, effect_id: &EffectId, output: &str) -> io::Result<String> {
        self.write_named(effect_id.as_str(), output)
    }

    pub(crate) fn write_named(&self, _key: &str, output: &str) -> io::Result<String> {
        self.write_named_with_observer(output, |_| {})
    }

    fn write_named_with_observer(
        &self,
        output: &str,
        observer: impl FnMut(secure_directory::WriteStep),
    ) -> io::Result<String> {
        let root = self.root.as_ref().ok_or_else(|| {
            io::Error::new(
                ErrorKind::Unsupported,
                "runtime ledger has no local effect output directory",
            )
        })?;
        let output_root = root
            .parent()
            .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "effect output Run directory has no local root"))?;
        if paths_refer_to_same_directory(output_root, &effect_output_state_root())? {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "legacy global effect output directory is read-only",
            ));
        }
        let digest = stable_digest_bytes(output.as_bytes());
        let reference = format!("sha256-{digest}.blob");
        SecureDirectory::open_or_create(root)?.write_atomically(&reference, output.as_bytes(), observer)?;
        Ok(reference)
    }

    pub(crate) fn read(&self, reference: &str) -> io::Result<String> {
        let expected_digest = reference_digest(reference)?;
        let primary_result = self.root.as_ref().map_or_else(
            || {
                Err(io::Error::new(
                    ErrorKind::NotFound,
                    "effect output has no local directory",
                ))
            },
            |root| read_utf8(root, reference, expected_digest),
        );
        match primary_result {
            Err(error) if error.kind() == ErrorKind::NotFound => self
                .legacy_root
                .as_ref()
                .map_or(Err(error), |root| read_utf8(root, reference, expected_digest)),
            result => result,
        }
    }
}

pub(crate) fn effect_output_state_root() -> PathBuf {
    #[cfg(test)]
    return std::env::temp_dir().join("solaris-mesh-test-legacy-effect-outcomes");

    #[cfg(not(test))]
    solaris_config::config::app_config_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("effect-outcomes")
}

fn legacy_run_root(run_id: &RunId) -> PathBuf {
    effect_output_state_root().join(stable_digest_bytes(run_id.as_str().as_bytes()))
}

fn reference_digest(reference: &str) -> io::Result<&str> {
    validate_reference(reference)?;
    let stem = reference.strip_suffix(".blob").ok_or_else(invalid_reference)?;
    let digest = if let Some(digest) = stem.strip_prefix("sha256-") {
        digest
    } else {
        let (key_digest, output_digest) = stem.rsplit_once('-').ok_or_else(invalid_reference)?;
        if !is_sha256_hex(key_digest) {
            return Err(invalid_reference());
        }
        output_digest
    };
    if !is_sha256_hex(digest) {
        return Err(invalid_reference());
    }
    Ok(digest)
}

fn validate_reference(reference: &str) -> io::Result<()> {
    let mut components = Path::new(reference).components();
    if matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    ) {
        Ok(())
    } else {
        Err(invalid_reference())
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn invalid_reference() -> io::Error {
    io::Error::other("invalid effect output reference")
}

fn read_utf8(root: &Path, reference: &str, expected_digest: &str) -> io::Result<String> {
    let bytes = SecureDirectory::open_existing(root)?.read_file(reference)?;
    if stable_digest_bytes(&bytes) != expected_digest {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "effect output digest does not match its reference",
        ));
    }
    String::from_utf8(bytes).map_err(|error| io::Error::new(ErrorKind::InvalidData, error))
}

#[cfg(test)]
#[path = "output_store_test.rs"]
mod output_store_test;
