//! Detection-only probe for the Windows process security-environment (PSEC) API.
//!
//! The ABI and functional-proof boundary follow Microsoft `mxc` commit
//! `b497fd48653e01c846c6ef225e0af1c859b70122`, especially
//! `src/backends/learning_mode/windows/src/secenv.rs`. This module does not copy
//! either Microsoft FlatBuffer schema. Export presence and OS build numbers are
//! diagnostic evidence only; neither is sufficient for strict network enforcement.

use std::ffi::{CStr, OsString, c_void};
use std::mem::transmute;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::ptr::NonNull;

const LOAD_LIBRARY_SEARCH_SYSTEM32: u32 = 0x0000_0800;
const E_NOTIMPL: i32 = 0x8000_4001_u32 as i32;
const HRESULT_FROM_WIN32_NOT_SUPPORTED: i32 = 0x8007_0032_u32 as i32;
const HRESULT_FROM_WIN32_CALL_NOT_IMPLEMENTED: i32 = 0x8007_0078_u32 as i32;

type QueryProcessSecurityEnvironmentSupport = unsafe extern "system" fn(*mut u64) -> i32;

#[link(name = "kernel32")]
unsafe extern "system" {
    #[link_name = "GetSystemDirectoryW"]
    fn get_system_directory(buffer: *mut u16, size: u32) -> u32;
    #[link_name = "LoadLibraryExW"]
    fn load_library_ex(path: *const u16, file: *mut c_void, flags: u32) -> *mut c_void;
    #[link_name = "GetProcAddress"]
    fn get_proc_address(module: *mut c_void, name: *const u8) -> *mut c_void;
    #[link_name = "FreeLibrary"]
    fn free_library(module: *mut c_void) -> i32;
    #[link_name = "GetLastError"]
    fn get_last_error() -> u32;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DllProbeOperation {
    ResolveSystemDirectory,
    LoadProcessModel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PsecExport {
    Create,
    QuerySupport,
    Close,
}

impl PsecExport {
    const fn symbol(self) -> &'static CStr {
        match self {
            Self::Create => c"CreateProcessSecurityEnvironment",
            Self::QuerySupport => c"QueryProcessSecurityEnvironmentSupport",
            Self::Close => c"CloseProcessSecurityEnvironment",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryUnsupportedKind {
    NotSupported,
    NotImplemented,
    Win32CallNotImplemented,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FunctionalProbeState {
    NotImplemented,
    Passed,
    // The first-stage real adapter does not execute functional steps yet. This
    // variant is retained so an injected or future adapter can classify a safe
    // numeric failure without changing the probe contract.
    #[allow(dead_code)]
    Failed {
        code: u32,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FunctionalProbeEvidence {
    create: FunctionalProbeState,
    close: FunctionalProbeState,
    process_attribute: FunctionalProbeState,
    packaged_proxy_peer: FunctionalProbeState,
    wfp_filtering: FunctionalProbeState,
}

impl FunctionalProbeEvidence {
    const fn not_implemented() -> Self {
        Self {
            create: FunctionalProbeState::NotImplemented,
            close: FunctionalProbeState::NotImplemented,
            process_attribute: FunctionalProbeState::NotImplemented,
            packaged_proxy_peer: FunctionalProbeState::NotImplemented,
            wfp_filtering: FunctionalProbeState::NotImplemented,
        }
    }

    fn has_failure(self) -> bool {
        self.steps()
            .into_iter()
            .any(|step| matches!(step, FunctionalProbeState::Failed { .. }))
    }

    fn is_complete(self) -> bool {
        self.steps()
            .into_iter()
            .all(|step| step == FunctionalProbeState::Passed)
    }

    const fn steps(self) -> [FunctionalProbeState; 5] {
        [
            self.create,
            self.close,
            self.process_attribute,
            self.packaged_proxy_peer,
            self.wfp_filtering,
        ]
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueryOutcome {
    Supported { flags: u64 },
    Unsupported { kind: QueryUnsupportedKind, hresult: i32 },
    Failed { hresult: i32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PsecProbeStatus {
    DllUnavailable {
        operation: DllProbeOperation,
        code: u32,
    },
    ExportMissing {
        export: PsecExport,
        code: u32,
    },
    QueryUnsupported {
        kind: QueryUnsupportedKind,
        hresult: i32,
    },
    QueryFailed {
        hresult: i32,
    },
    FunctionalProbeIncomplete {
        support_flags: u64,
        evidence: FunctionalProbeEvidence,
    },
    FunctionalProbeFailed {
        support_flags: u64,
        evidence: FunctionalProbeEvidence,
    },
    FunctionallyProven {
        support_flags: u64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PsecCapability {
    status: PsecProbeStatus,
}

impl PsecCapability {
    pub(super) const fn is_fully_proven(self) -> bool {
        matches!(self.status, PsecProbeStatus::FunctionallyProven { .. })
    }
}

struct LoadedModule(*mut c_void);

impl LoadedModule {
    fn resolve(&self, export: PsecExport) -> Result<NonNull<c_void>, PsecProbeStatus> {
        // SAFETY: the module handle is valid until `Drop`. `PsecExport` owns
        // the exact static NUL-terminated symbol passed to `GetProcAddress`.
        let address = unsafe { get_proc_address(self.0, export.symbol().as_ptr().cast()) };
        NonNull::new(address).ok_or_else(|| PsecProbeStatus::ExportMissing {
            export,
            code: last_error(),
        })
    }
}

impl Drop for LoadedModule {
    fn drop(&mut self) {
        // SAFETY: the handle came from a successful `LoadLibraryExW` call and
        // no resolved function pointer is retained beyond this owner.
        let _ = unsafe { free_library(self.0) };
    }
}

struct CreateExportHandle {
    _address: NonNull<c_void>,
}

struct QuerySupportExport(QueryProcessSecurityEnvironmentSupport);

impl QuerySupportExport {
    fn invoke(self) -> (i32, u64) {
        let mut flags = 0_u64;
        // SAFETY: the typed function pointer was constructed only from the
        // exact QueryProcessSecurityEnvironmentSupport export, and `flags` is
        // a live writable out-parameter for the duration of the call.
        let hresult = unsafe { self.0(&mut flags) };
        (hresult, flags)
    }
}

struct CloseExportHandle {
    _address: NonNull<c_void>,
}

trait PsecProbeAdapter {
    type Module;
    type CreateExport;
    type QuerySupportExport;
    type CloseExport;

    fn load_processmodel_from_system32(&self) -> Result<Self::Module, PsecProbeStatus>;

    fn resolve_create(&self, module: &Self::Module) -> Result<Self::CreateExport, PsecProbeStatus>;

    fn resolve_query_support(&self, module: &Self::Module) -> Result<Self::QuerySupportExport, PsecProbeStatus>;

    fn resolve_close(&self, module: &Self::Module) -> Result<Self::CloseExport, PsecProbeStatus>;

    fn query_support(&self, query: Self::QuerySupportExport) -> (i32, u64);
}

struct SystemPsecProbe;

impl PsecProbeAdapter for SystemPsecProbe {
    type Module = LoadedModule;
    type CreateExport = CreateExportHandle;
    type QuerySupportExport = QuerySupportExport;
    type CloseExport = CloseExportHandle;

    fn load_processmodel_from_system32(&self) -> Result<Self::Module, PsecProbeStatus> {
        load_processmodel_from_system32()
    }

    fn resolve_create(&self, module: &Self::Module) -> Result<Self::CreateExport, PsecProbeStatus> {
        module
            .resolve(PsecExport::Create)
            .map(|address| CreateExportHandle { _address: address })
    }

    fn resolve_query_support(&self, module: &Self::Module) -> Result<Self::QuerySupportExport, PsecProbeStatus> {
        let address = module.resolve(PsecExport::QuerySupport)?;
        // SAFETY: `address` came from the exact symbol owned by
        // `PsecExport::QuerySupport`. Its ABI matches the pinned Microsoft PSEC
        // declaration, and the loaded module remains alive while it is invoked.
        let function = unsafe { transmute::<*mut c_void, QueryProcessSecurityEnvironmentSupport>(address.as_ptr()) };
        Ok(QuerySupportExport(function))
    }

    fn resolve_close(&self, module: &Self::Module) -> Result<Self::CloseExport, PsecProbeStatus> {
        module
            .resolve(PsecExport::Close)
            .map(|address| CloseExportHandle { _address: address })
    }

    fn query_support(&self, query: Self::QuerySupportExport) -> (i32, u64) {
        query.invoke()
    }
}

pub(super) fn probe_network_capability() -> PsecCapability {
    PsecCapability {
        status: probe_network_status(),
    }
}

fn probe_network_status() -> PsecProbeStatus {
    probe_network_status_with(&SystemPsecProbe)
}

fn probe_network_status_with<A: PsecProbeAdapter>(adapter: &A) -> PsecProbeStatus {
    let module = match adapter.load_processmodel_from_system32() {
        Ok(module) => module,
        Err(status) => return status,
    };
    let _create = match adapter.resolve_create(&module) {
        Ok(address) => address,
        Err(status) => return status,
    };
    let query = match adapter.resolve_query_support(&module) {
        Ok(address) => address,
        Err(status) => return status,
    };
    let _close = match adapter.resolve_close(&module) {
        Ok(address) => address,
        Err(status) => return status,
    };

    let (hresult, flags) = adapter.query_support(query);
    let query = classify_query(hresult, flags);

    // Export and query success are insufficient. The create/close lifecycle,
    // process attribute, packaged proxy peer, and WFP rules are not implemented
    // by Solaris yet and therefore cannot produce Full enforcement.
    classify_functional(query, FunctionalProbeEvidence::not_implemented())
}

fn classify_query(hresult: i32, flags: u64) -> QueryOutcome {
    match hresult {
        HRESULT_FROM_WIN32_NOT_SUPPORTED => QueryOutcome::Unsupported {
            kind: QueryUnsupportedKind::NotSupported,
            hresult,
        },
        E_NOTIMPL => QueryOutcome::Unsupported {
            kind: QueryUnsupportedKind::NotImplemented,
            hresult,
        },
        HRESULT_FROM_WIN32_CALL_NOT_IMPLEMENTED => QueryOutcome::Unsupported {
            kind: QueryUnsupportedKind::Win32CallNotImplemented,
            hresult,
        },
        failed if failed < 0 => QueryOutcome::Failed { hresult: failed },
        _ => QueryOutcome::Supported { flags },
    }
}

fn classify_functional(query: QueryOutcome, evidence: FunctionalProbeEvidence) -> PsecProbeStatus {
    match query {
        QueryOutcome::Unsupported { kind, hresult } => PsecProbeStatus::QueryUnsupported { kind, hresult },
        QueryOutcome::Failed { hresult } => PsecProbeStatus::QueryFailed { hresult },
        QueryOutcome::Supported { flags } if evidence.has_failure() => PsecProbeStatus::FunctionalProbeFailed {
            support_flags: flags,
            evidence,
        },
        QueryOutcome::Supported { flags } if evidence.is_complete() => {
            PsecProbeStatus::FunctionallyProven { support_flags: flags }
        }
        QueryOutcome::Supported { flags } => PsecProbeStatus::FunctionalProbeIncomplete {
            support_flags: flags,
            evidence,
        },
    }
}

fn load_processmodel_from_system32() -> Result<LoadedModule, PsecProbeStatus> {
    let path = system_processmodel_path()?;
    // SAFETY: `path` is an absolute, null-terminated System32 path. The search
    // flag also constrains any dependent DLL lookup to System32.
    let module = unsafe { load_library_ex(path.as_ptr(), std::ptr::null_mut(), LOAD_LIBRARY_SEARCH_SYSTEM32) };
    if module.is_null() {
        return Err(PsecProbeStatus::DllUnavailable {
            operation: DllProbeOperation::LoadProcessModel,
            code: last_error(),
        });
    }
    Ok(LoadedModule(module))
}

fn system_processmodel_path() -> Result<Vec<u16>, PsecProbeStatus> {
    let mut buffer = vec![0_u16; 260];
    let length = loop {
        // SAFETY: `buffer` is writable for `buffer.len()` UTF-16 code units.
        let length = unsafe { get_system_directory(buffer.as_mut_ptr(), buffer.len() as u32) };
        if length == 0 {
            return Err(PsecProbeStatus::DllUnavailable {
                operation: DllProbeOperation::ResolveSystemDirectory,
                code: last_error(),
            });
        }
        if (length as usize) < buffer.len() {
            break length as usize;
        }
        buffer.resize(length as usize + 1, 0);
    };
    let mut path = PathBuf::from(OsString::from_wide(&buffer[..length]));
    if !path.is_absolute() {
        return Err(PsecProbeStatus::DllUnavailable {
            operation: DllProbeOperation::ResolveSystemDirectory,
            code: 0,
        });
    }
    path.push("processmodel.dll");
    Ok(path.as_os_str().encode_wide().chain(std::iter::once(0)).collect())
}

fn last_error() -> u32 {
    // SAFETY: `GetLastError` reads the current thread's error slot.
    unsafe { get_last_error() }
}

#[cfg(test)]
#[path = "psec_test.rs"]
mod psec_test;
