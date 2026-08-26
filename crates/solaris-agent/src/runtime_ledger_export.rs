use std::ffi::OsStr;
use std::fmt;
use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cap_std::ambient_authority;
use cap_std::fs::{Dir, File, OpenOptions};
use rusqlite::{Connection, TransactionBehavior};
use serde::Serialize;
use solaris_config::file_identity::OpenedFileIdentity;
use uuid::Uuid;

#[cfg(test)]
use super::LedgerRecord;
use super::{decode_sqlite_record, sqlite_error};

struct ExportParent {
    path: PathBuf,
    directory: Dir,
    identity: Arc<OpenedFileIdentity>,
}

pub(super) struct ProtectedExportPath {
    path: PathBuf,
    parent: ExportParent,
    file_name: std::ffi::OsString,
    opened_identity: Option<Arc<OpenedFileIdentity>>,
}

impl ProtectedExportPath {
    pub(super) fn verify_opened_slot(&self) -> io::Result<()> {
        let expected = self
            .opened_identity
            .as_ref()
            .ok_or_else(|| io::Error::other("protected runtime state was not opened"))?;
        let current = self
            .parent
            .directory
            .open(&self.file_name)
            .map_err(|error| io::Error::new(error.kind(), "runtime ledger database path changed while opening"))?;
        let current = file_identity(current)?;
        if expected.same_object(&current) {
            Ok(())
        } else {
            Err(io::Error::other("runtime ledger database path changed while opening"))
        }
    }
}

type ReplaceStep<'a> = Box<dyn FnOnce(&ExportParent, &mut File, &OsStr, &OsStr, &Path, &Path) -> io::Result<()> + 'a>;
type SyncStep<'a> = Box<dyn FnOnce(&ExportParent) -> io::Result<()> + 'a>;
type RevalidationStep<'a> = Box<dyn FnOnce(&ExportParent) -> io::Result<()> + 'a>;
type WriteRecordsStep<'a> = Box<dyn FnOnce(&mut dyn Write) -> io::Result<usize> + 'a>;
type WriteTemporaryStep<'a> = Box<dyn FnOnce(&mut File, WriteRecordsStep<'a>) -> io::Result<usize> + 'a>;
type SyncTemporaryStep<'a> = Box<dyn FnOnce(&File) -> io::Result<()> + 'a>;

struct ExportIoSteps<'a> {
    write_temporary: WriteTemporaryStep<'a>,
    sync_temporary: SyncTemporaryStep<'a>,
    replace: ReplaceStep<'a>,
    sync_parent: SyncStep<'a>,
    before_revalidation: RevalidationStep<'a>,
}

pub(super) fn export_sqlite_records_atomically(
    connection: &mut Connection,
    target: &Path,
    protected_paths: &[ProtectedExportPath],
) -> io::Result<usize> {
    export_records_atomically_impl(
        target,
        protected_paths,
        Box::new(move |writer| write_sqlite_snapshot(connection, writer)),
        Box::new(|parent, source, temporary_name, target_name, _, _| {
            replace_file_atomically(parent, source, temporary_name, target_name)
        }),
        Box::new(sync_export_parent_directory),
        Box::new(|_| Ok(())),
    )
}

#[cfg(test)]
pub(super) fn export_records_atomically_with(
    records: &[LedgerRecord],
    target: &Path,
    protected_paths: &[PathBuf],
    replace: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<usize> {
    let protected_paths = retain_protected_export_paths(protected_paths)?;
    export_records_atomically_impl(
        target,
        &protected_paths,
        Box::new(move |writer| write_record_slice(writer, records)),
        Box::new(move |_, _, _, _, temporary_path, target_path| replace(temporary_path, target_path)),
        Box::new(sync_export_parent_directory),
        Box::new(|_| Ok(())),
    )
}

#[cfg(test)]
pub(super) fn export_records_atomically_with_steps(
    records: &[LedgerRecord],
    target: &Path,
    protected_paths: &[PathBuf],
    replace: impl FnOnce(&Path, &Path) -> io::Result<()>,
    sync_parent: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<usize> {
    let protected_paths = retain_protected_export_paths(protected_paths)?;
    export_records_atomically_impl(
        target,
        &protected_paths,
        Box::new(move |writer| write_record_slice(writer, records)),
        Box::new(move |_, _, _, _, temporary_path, target_path| replace(temporary_path, target_path)),
        Box::new(move |parent| sync_parent(&parent.path)),
        Box::new(|_| Ok(())),
    )
}

#[cfg(test)]
pub(super) fn export_records_atomically_with_revalidation_hook(
    records: &[LedgerRecord],
    target: &Path,
    protected_paths: &[PathBuf],
    before_revalidation: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<usize> {
    let protected_paths = retain_protected_export_paths(protected_paths)?;
    export_records_atomically_impl(
        target,
        &protected_paths,
        Box::new(move |writer| write_record_slice(writer, records)),
        Box::new(|parent, source, temporary_name, target_name, _, _| {
            replace_file_atomically(parent, source, temporary_name, target_name)
        }),
        Box::new(sync_export_parent_directory),
        Box::new(move |parent| before_revalidation(&parent.path)),
    )
}

#[cfg(test)]
fn export_with_write_step_for_test(
    target: &Path,
    write_records: impl FnOnce(&mut dyn Write) -> io::Result<usize>,
) -> io::Result<usize> {
    export_records_atomically_impl(
        target,
        &[],
        Box::new(move |writer| write_records(writer).map_err(sanitize_test_write_error)),
        Box::new(|parent, source, temporary_name, target_name, _, _| {
            replace_file_atomically(parent, source, temporary_name, target_name)
        }),
        Box::new(sync_export_parent_directory),
        Box::new(|_| Ok(())),
    )
}

fn export_records_atomically_impl<'a>(
    target: &Path,
    protected_paths: &[ProtectedExportPath],
    write_records: WriteRecordsStep<'a>,
    replace: ReplaceStep<'a>,
    sync_parent: SyncStep<'a>,
    before_revalidation: RevalidationStep<'a>,
) -> io::Result<usize> {
    export_records_atomically_with_io_impl(
        target,
        protected_paths,
        write_records,
        ExportIoSteps {
            write_temporary: Box::new(write_records_to_temporary),
            sync_temporary: Box::new(File::sync_all),
            replace,
            sync_parent,
            before_revalidation,
        },
    )
}

fn export_records_atomically_with_io_impl<'a>(
    target: &Path,
    protected_paths: &[ProtectedExportPath],
    write_records: WriteRecordsStep<'a>,
    steps: ExportIoSteps<'a>,
) -> io::Result<usize> {
    let ExportIoSteps {
        write_temporary,
        sync_temporary,
        replace,
        sync_parent,
        before_revalidation,
    } = steps;
    let (parent, target_name) = open_export_parent(target)?;
    let target_path = parent.path.join(&target_name);
    let before = inspect_export_target(&parent, &target_name, &target_path, protected_paths)?;
    let (temporary_name, temporary_path, mut file, temporary_identity) = create_export_temporary_file(&parent)?;
    let result = (|| {
        let record_count = write_temporary(&mut file, write_records)?;
        sync_temporary(&file)
            .map_err(|error| io::Error::new(error.kind(), "sync runtime ledger export temporary file"))?;
        before_revalidation(&parent)?;
        let current = inspect_export_target(&parent, &target_name, &target_path, protected_paths)?;
        let target_is_unchanged = optional_identity_matches(before.as_ref(), current.as_ref());
        drop(current);
        if !target_is_unchanged {
            return Err(io::Error::other("runtime ledger export target changed during export"));
        }
        inspect_export_temporary(&parent, &temporary_name, &temporary_identity)?;
        // Windows requires the destination identity handles to be closed before
        // SetFileInformationByHandle can replace the directory slot.
        drop(before);
        replace(
            &parent,
            &mut file,
            &temporary_name,
            &target_name,
            &temporary_path,
            &target_path,
        )?;
        sync_parent(&parent)?;
        Ok(record_count)
    })();
    if result.is_err() {
        let _ = parent.directory.remove_file(&temporary_name);
    }
    result
}

fn write_records_to_temporary(file: &mut File, write_records: WriteRecordsStep<'_>) -> io::Result<usize> {
    let mut writer = BufWriter::new(file);
    let record_count = write_records(&mut writer)?;
    writer.flush().map_err(export_write_error)?;
    Ok(record_count)
}

fn write_sqlite_snapshot(connection: &mut Connection, writer: &mut dyn Write) -> io::Result<usize> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| sqlite_error("begin runtime ledger export snapshot", error))?;
    let record_count = {
        let mut statement = transaction
            .prepare(
                "SELECT sequence, schema_version, run_id, timestamp_unix_ms, durability, record_type, payload
                 FROM runtime_ledger_records ORDER BY sequence",
            )
            .map_err(|error| sqlite_error("prepare runtime ledger export query", error))?;
        let mut rows = statement
            .query([])
            .map_err(|error| sqlite_error("query runtime ledger export records", error))?;
        let mut record_count = 0_usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| sqlite_error("query runtime ledger export records", error))?
        {
            let record = decode_sqlite_record(row)
                .map_err(|error| sqlite_error("decode runtime ledger export record", error))?;
            write_jsonl_record(writer, &record)?;
            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| io::Error::other("runtime ledger export count overflow"))?;
        }
        record_count
    };
    transaction
        .commit()
        .map_err(|error| sqlite_error("commit runtime ledger export snapshot", error))?;
    Ok(record_count)
}

#[cfg(test)]
fn write_record_slice(writer: &mut dyn Write, records: &[LedgerRecord]) -> io::Result<usize> {
    for record in records {
        write_jsonl_record(writer, record)?;
    }
    Ok(records.len())
}

fn write_jsonl_record<T>(writer: &mut dyn Write, record: &T) -> io::Result<()>
where
    T: Serialize + ?Sized,
{
    serde_json::to_writer(&mut *writer, record).map_err(|_| export_encode_error())?;
    writer.write_all(b"\n").map_err(export_write_error)
}

#[derive(Debug)]
enum ExportStreamFailure {
    Encode,
    Write,
}

impl fmt::Display for ExportStreamFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode => formatter.write_str("encode runtime ledger export record"),
            Self::Write => formatter.write_str("write runtime ledger export"),
        }
    }
}

impl std::error::Error for ExportStreamFailure {}

fn export_encode_error() -> io::Error {
    io::Error::new(ErrorKind::InvalidData, ExportStreamFailure::Encode)
}

fn export_write_error(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), ExportStreamFailure::Write)
}

#[cfg(test)]
fn sanitize_test_write_error(error: io::Error) -> io::Error {
    if error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ExportStreamFailure>())
        .is_some()
    {
        error
    } else {
        export_write_error(error)
    }
}

#[cfg(test)]
enum TestExportIoFailure {
    PartialWrite { after_bytes: usize },
    Flush,
    SyncAll,
}

#[cfg(test)]
fn export_with_io_failure_for_test(
    target: &Path,
    failure: TestExportIoFailure,
    write_records: impl FnOnce(&mut dyn Write) -> io::Result<usize>,
) -> io::Result<usize> {
    let write_records: WriteRecordsStep<'_> =
        Box::new(move |writer| write_records(writer).map_err(sanitize_test_write_error));
    let (write_temporary, sync_temporary): (WriteTemporaryStep<'_>, SyncTemporaryStep<'_>) = match failure {
        TestExportIoFailure::PartialWrite { after_bytes } => (
            Box::new(move |file, write_records| {
                let writer = PartialFailureWriter {
                    file,
                    remaining: after_bytes,
                };
                write_records_with_test_writer(writer, write_records)
            }),
            Box::new(File::sync_all),
        ),
        TestExportIoFailure::Flush => (
            Box::new(move |file, write_records| {
                let writer = FlushFailureWriter { file };
                write_records_with_test_writer(writer, write_records)
            }),
            Box::new(File::sync_all),
        ),
        TestExportIoFailure::SyncAll => (
            Box::new(write_records_to_temporary),
            Box::new(|_| Err(io::Error::other("secret injected sync_all failure"))),
        ),
    };
    export_records_atomically_with_io_impl(
        target,
        &[],
        write_records,
        ExportIoSteps {
            write_temporary,
            sync_temporary,
            replace: Box::new(|parent, source, temporary_name, target_name, _, _| {
                replace_file_atomically(parent, source, temporary_name, target_name)
            }),
            sync_parent: Box::new(sync_export_parent_directory),
            before_revalidation: Box::new(|_| Ok(())),
        },
    )
}

#[cfg(test)]
fn write_records_with_test_writer(writer: impl Write, write_records: WriteRecordsStep<'_>) -> io::Result<usize> {
    let mut writer = BufWriter::with_capacity(8, writer);
    let record_count = write_records(&mut writer)?;
    writer.flush().map_err(export_write_error)?;
    Ok(record_count)
}

#[cfg(test)]
struct PartialFailureWriter<'a> {
    file: &'a mut File,
    remaining: usize,
}

#[cfg(test)]
impl Write for PartialFailureWriter<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("secret injected partial write failure"));
        }
        let allowed = input.len().min(self.remaining);
        let written = self.file.write(&input[..allowed])?;
        self.remaining = self.remaining.saturating_sub(written);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
struct FlushFailureWriter<'a> {
    file: &'a mut File,
}

#[cfg(test)]
impl Write for FlushFailureWriter<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.file.write(input)
    }

    fn flush(&mut self) -> io::Result<()> {
        Err(io::Error::other("secret injected flush failure"))
    }
}

fn open_export_parent(target: &Path) -> io::Result<(ExportParent, std::ffi::OsString)> {
    let requested_parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(requested_parent)?;
    let path = requested_parent
        .canonicalize()
        .map_err(|error| io::Error::new(error.kind(), "resolve runtime ledger export parent"))?;
    let directory = Dir::open_ambient_dir(&path, ambient_authority())
        .map_err(|error| io::Error::new(error.kind(), "open runtime ledger export parent"))?;
    let identity = directory_identity(&directory)?;
    let target_name = target
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "runtime ledger export path has no file name"))?
        .to_os_string();
    Ok((
        ExportParent {
            path,
            directory,
            identity,
        },
        target_name,
    ))
}

fn inspect_export_target(
    parent: &ExportParent,
    target_name: &OsStr,
    target_path: &Path,
    protected_paths: &[ProtectedExportPath],
) -> io::Result<Option<Arc<OpenedFileIdentity>>> {
    if target_path.canonicalize().ok().is_some_and(|resolved_target| {
        protected_paths
            .iter()
            .any(|protected| resolved_target == protected.path)
    }) {
        return Err(protected_export_error());
    }
    let target_identity = match parent.directory.open(target_name) {
        Ok(file) => Some(file_identity(file)?),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(io::Error::new(error.kind(), "inspect runtime ledger export target")),
    };
    for protected in protected_paths {
        if protected.file_name == target_name && protected.parent.identity.same_object(&parent.identity) {
            return Err(protected_export_error());
        }
        let current_identity = match protected.parent.directory.open(&protected.file_name) {
            Ok(file) => Some(file_identity(file)?),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => {
                return Err(io::Error::new(error.kind(), "inspect protected runtime ledger state"));
            }
        };
        if target_identity.as_ref().is_some_and(|target| {
            protected
                .opened_identity
                .as_ref()
                .into_iter()
                .chain(current_identity.as_ref())
                .any(|protected| target.same_object(protected))
        }) {
            return Err(protected_export_error());
        }
    }
    let resolved_target = parent.path.join(target_name);
    if resolved_target != target_path {
        return Err(io::Error::other(
            "runtime ledger export target changed during validation",
        ));
    }
    Ok(target_identity)
}

pub(super) fn retain_protected_export_path(path: &Path, create: bool) -> io::Result<ProtectedExportPath> {
    let resolved = resolve_ledger_path(path)?;
    let parent_path = resolved
        .parent()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "protected runtime state has no parent"))?
        .to_path_buf();
    let file_name = resolved
        .file_name()
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "protected runtime state has no file name"))?
        .to_os_string();
    let directory = Dir::open_ambient_dir(&parent_path, ambient_authority())
        .map_err(|error| io::Error::new(error.kind(), "open protected runtime state parent"))?;
    let identity = directory_identity(&directory)?;
    let opened_identity = if create {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        Some(file_identity(directory.open_with(&file_name, &options)?)?)
    } else {
        match directory.open(&file_name) {
            Ok(file) => Some(file_identity(file)?),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        }
    };
    Ok(ProtectedExportPath {
        path: resolved,
        parent: ExportParent {
            path: parent_path,
            directory,
            identity,
        },
        file_name,
        opened_identity,
    })
}

pub(super) fn retain_protected_export_paths(paths: &[PathBuf]) -> io::Result<Vec<ProtectedExportPath>> {
    paths
        .iter()
        .map(|path| retain_protected_export_path(path, false))
        .collect()
}

pub(super) fn resolve_ledger_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    match absolute.canonicalize() {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let parent = absolute
                .parent()
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "runtime ledger path has no parent"))?;
            let file_name = absolute
                .file_name()
                .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "runtime ledger path has no file name"))?;
            let parent = parent
                .canonicalize()
                .map_err(|error| io::Error::new(error.kind(), "resolve runtime ledger parent"))?;
            Ok(parent.join(file_name))
        }
        Err(error) => Err(io::Error::new(error.kind(), "resolve runtime ledger path")),
    }
}

fn create_export_temporary_file(
    parent: &ExportParent,
) -> io::Result<(std::ffi::OsString, PathBuf, File, Arc<OpenedFileIdentity>)> {
    for _ in 0..16 {
        let name = format!(".solaris-ledger-export-{}.tmp", Uuid::now_v7());
        let options = export_open_options();
        match parent.directory.open_with(&name, &options) {
            Ok(file) => {
                let identity = file_identity(file.try_clone()?)?;
                let name = std::ffi::OsString::from(name);
                let path = parent.path.join(&name);
                return Ok((name, path, file, identity));
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        ErrorKind::AlreadyExists,
        "could not allocate runtime ledger export temporary file",
    ))
}

fn inspect_export_temporary(
    parent: &ExportParent,
    temporary_name: &OsStr,
    expected: &OpenedFileIdentity,
) -> io::Result<()> {
    let file = parent
        .directory
        .open(temporary_name)
        .map_err(|error| io::Error::new(error.kind(), "runtime ledger export temporary file changed"))?;
    let metadata = file
        .metadata()
        .map_err(|error| io::Error::new(error.kind(), "runtime ledger export temporary file changed"))?;
    if !metadata.is_file() {
        return Err(io::Error::other("runtime ledger export temporary file changed"));
    }
    let current = file_identity(file)?;
    if !expected.same_object(&current) {
        return Err(io::Error::other("runtime ledger export temporary file changed"));
    }
    Ok(())
}

fn export_open_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(windows)]
    {
        use cap_std::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{DELETE, FILE_WRITE_DATA, SYNCHRONIZE};

        options.access_mode(FILE_WRITE_DATA | DELETE | SYNCHRONIZE);
    }
    options
}

fn protected_export_error() -> io::Error {
    io::Error::new(
        ErrorKind::InvalidInput,
        "runtime ledger export path is protected internal state",
    )
}

fn file_identity(file: File) -> io::Result<Arc<OpenedFileIdentity>> {
    OpenedFileIdentity::from_owned_file(file.into_std()).map(Arc::new)
}

fn directory_identity(directory: &Dir) -> io::Result<Arc<OpenedFileIdentity>> {
    let file = directory.try_clone()?.into_std_file();
    OpenedFileIdentity::from_owned_file(file).map(Arc::new)
}

fn optional_identity_matches(
    before: Option<&Arc<OpenedFileIdentity>>,
    after: Option<&Arc<OpenedFileIdentity>>,
) -> bool {
    match (before, after) {
        (Some(before), Some(after)) => before.same_object(after),
        (None, None) => true,
        _ => false,
    }
}

#[cfg(unix)]
fn replace_file_atomically(
    parent: &ExportParent,
    _: &mut File,
    temporary_name: &OsStr,
    target_name: &OsStr,
) -> io::Result<()> {
    parent.directory.rename(temporary_name, &parent.directory, target_name)
}

#[cfg(unix)]
fn sync_export_parent_directory(parent: &ExportParent) -> io::Result<()> {
    parent.directory.try_clone()?.into_std_file().sync_all()
}

#[cfg(windows)]
fn replace_file_atomically(parent: &ExportParent, source: &mut File, _: &OsStr, target_name: &OsStr) -> io::Result<()> {
    use std::mem::{offset_of, size_of};
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::{FILE_RENAME_INFO, FileRenameInfo, SetFileInformationByHandle};

    let target = parent.path.join(target_name);
    let name = windows_rename_target_name(&target);
    let name_bytes = name
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "runtime ledger export name is too long"))?;
    let header_bytes = offset_of!(FILE_RENAME_INFO, FileName);
    let total_bytes = size_of::<FILE_RENAME_INFO>()
        .checked_add(name_bytes as usize)
        .and_then(|length| length.checked_add(size_of::<u16>()))
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidInput, "runtime ledger export name is too long"))?;
    let word_count = total_bytes.div_ceil(size_of::<usize>());
    let mut storage = vec![0usize; word_count];
    let information = storage.as_mut_ptr().cast::<FILE_RENAME_INFO>();
    unsafe {
        (*information).Anonymous.ReplaceIfExists = true;
        // FILE_RENAME_INFO documents a parent handle plus relative name, but
        // current Windows/NTFS rejects that form with ERROR_INVALID_PARAMETER.
        // The retained cap-std parent handle denies parent rename, so a full
        // name remains bound to the validated directory for this operation.
        (*information).RootDirectory = std::ptr::null_mut();
        (*information).FileNameLength = name_bytes;
        std::ptr::copy_nonoverlapping(
            name.as_ptr().cast::<u8>(),
            storage.as_mut_ptr().cast::<u8>().add(header_bytes),
            name_bytes as usize,
        );
    }
    let result = unsafe {
        SetFileInformationByHandle(
            source.as_raw_handle(),
            FileRenameInfo,
            information.cast(),
            u32::try_from(total_bytes)
                .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "runtime ledger export name is too long"))?,
        )
    };
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(any(windows, test))]
fn windows_rename_target_name(path: &Path) -> Vec<u16> {
    let mut name = windows_path_utf16(path);
    let verbatim_prefix = ['\\' as u16, '\\' as u16, '?' as u16, '\\' as u16];
    let verbatim_unc_prefix = [
        '\\' as u16,
        '\\' as u16,
        '?' as u16,
        '\\' as u16,
        'U' as u16,
        'N' as u16,
        'C' as u16,
        '\\' as u16,
    ];
    if name.starts_with(&verbatim_unc_prefix) {
        name.drain(2..verbatim_unc_prefix.len());
    } else if name.starts_with(&verbatim_prefix) {
        name.drain(..verbatim_prefix.len());
    }
    name
}

#[cfg(windows)]
fn windows_path_utf16(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;

    path.as_os_str().encode_wide().collect()
}

#[cfg(all(test, not(windows)))]
fn windows_path_utf16(path: &Path) -> Vec<u16> {
    path.to_string_lossy().encode_utf16().collect()
}

#[cfg(windows)]
fn sync_export_parent_directory(parent: &ExportParent) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;

    use windows_sys::Win32::Storage::FileSystem::FlushFileBuffers;

    let parent_file = parent.directory.try_clone()?.into_std_file();
    let result = unsafe { FlushFileBuffers(parent_file.as_raw_handle()) };
    if result == 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(5) {
            // Windows does not allow FlushFileBuffers on directory handles.
            // The temporary file itself was synced before the atomic rename.
            Ok(())
        } else {
            Err(error)
        }
    } else {
        Ok(())
    }
}

#[cfg(not(any(unix, windows)))]
fn replace_file_atomically(
    parent: &ExportParent,
    _: &mut File,
    temporary_name: &OsStr,
    target_name: &OsStr,
) -> io::Result<()> {
    parent.directory.rename(temporary_name, &parent.directory, target_name)
}

#[cfg(not(any(unix, windows)))]
fn sync_export_parent_directory(_: &ExportParent) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
#[path = "runtime_ledger_export_streaming_test.rs"]
mod runtime_ledger_export_streaming_test;
