use std::ffi::{OsString, c_void};
use std::io;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::ptr::NonNull;

use windows_sys::Win32::Foundation::HANDLE;

const LOAD_LIBRARY_SEARCH_SYSTEM32: u32 = 0x0000_0800;
pub(crate) const PROCESS_SECURITY_ENVIRONMENT_FLAG_NONE: u32 = 0;
#[allow(
    dead_code,
    reason = "used by the sandbox-helper path include; the library copy owns the same ABI"
)]
pub(crate) const PROC_THREAD_ATTRIBUTE_SECURITY_ENVIRONMENT: usize = 35 | 0x0002_0000;

const CREATE_SYMBOL: &[u8] = b"CreateProcessSecurityEnvironment\0";
const QUERY_SYMBOL: &[u8] = b"QueryProcessSecurityEnvironmentSupport\0";
const CLOSE_SYMBOL: &[u8] = b"CloseProcessSecurityEnvironment\0";

type CreateProcessSecurityEnvironment = unsafe extern "system" fn(
    sandbox_specification: *const c_void,
    sandbox_specification_size: u32,
    flags: u32,
    process_security_environment: *mut HANDLE,
) -> i32;
type QueryProcessSecurityEnvironmentSupport = unsafe extern "system" fn(support_flags: *mut u64) -> i32;
type CloseProcessSecurityEnvironment = unsafe extern "system" fn(process_security_environment: HANDLE);

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
}

pub(crate) struct SecurityEnvironmentApi {
    module: NonNull<c_void>,
    create: CreateProcessSecurityEnvironment,
    query_support: QueryProcessSecurityEnvironmentSupport,
    close: CloseProcessSecurityEnvironment,
}

impl SecurityEnvironmentApi {
    pub(crate) fn load() -> io::Result<Self> {
        let path = system_processmodel_path()?;
        // SAFETY: `path` is an absolute, null-terminated System32 path. The
        // search flag also constrains dependent DLL lookup to System32.
        let module = unsafe { load_library_ex(path.as_ptr(), std::ptr::null_mut(), LOAD_LIBRARY_SEARCH_SYSTEM32) };
        let module = NonNull::new(module).ok_or_else(io::Error::last_os_error)?;
        let create = match resolve(module, CREATE_SYMBOL) {
            Ok(value) => value,
            Err(error) => {
                // SAFETY: `module` came from a successful LoadLibraryExW call.
                unsafe { free_library(module.as_ptr()) };
                return Err(error);
            }
        };
        let query_support = match resolve(module, QUERY_SYMBOL) {
            Ok(value) => value,
            Err(error) => {
                // SAFETY: `module` came from a successful LoadLibraryExW call.
                unsafe { free_library(module.as_ptr()) };
                return Err(error);
            }
        };
        let close = match resolve(module, CLOSE_SYMBOL) {
            Ok(value) => value,
            Err(error) => {
                // SAFETY: `module` came from a successful LoadLibraryExW call.
                unsafe { free_library(module.as_ptr()) };
                return Err(error);
            }
        };
        // SAFETY: each address came from its exact processmodel.dll export and
        // the signatures match the Windows declarations used by Microsoft MXC.
        Ok(Self {
            module,
            create: unsafe { std::mem::transmute::<*mut c_void, CreateProcessSecurityEnvironment>(create.as_ptr()) },
            query_support: unsafe {
                std::mem::transmute::<*mut c_void, QueryProcessSecurityEnvironmentSupport>(query_support.as_ptr())
            },
            close: unsafe { std::mem::transmute::<*mut c_void, CloseProcessSecurityEnvironment>(close.as_ptr()) },
        })
    }

    pub(crate) fn query_support(&self) -> io::Result<u64> {
        let mut flags = 0_u64;
        // SAFETY: the function pointer was resolved from the exact query export
        // and `flags` is a live writable out-parameter.
        let hresult = unsafe { (self.query_support)(&mut flags) };
        hresult_result("QueryProcessSecurityEnvironmentSupport", hresult)?;
        Ok(flags)
    }

    pub(crate) fn create(self, specification: &[u8]) -> io::Result<SecurityEnvironment> {
        if specification.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process security environment specification is empty",
            ));
        }
        let specification_size = u32::try_from(specification.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "process security environment specification is too large",
            )
        })?;
        let mut handle: HANDLE = std::ptr::null_mut();
        // SAFETY: `specification` is contiguous and live for the complete call;
        // `handle` is a live out-parameter; the function pointer was resolved
        // from the exact create export.
        let hresult = unsafe {
            (self.create)(
                specification.as_ptr().cast(),
                specification_size,
                PROCESS_SECURITY_ENVIRONMENT_FLAG_NONE,
                &mut handle,
            )
        };
        hresult_result("CreateProcessSecurityEnvironment", hresult)?;
        if handle.is_null() {
            return Err(io::Error::other(
                "CreateProcessSecurityEnvironment succeeded without returning an environment handle",
            ));
        }
        let this = std::mem::ManuallyDrop::new(self);
        Ok(SecurityEnvironment {
            handle,
            module: this.module,
            close: this.close,
        })
    }
}

impl Drop for SecurityEnvironmentApi {
    fn drop(&mut self) {
        // SAFETY: this object exclusively owns the LoadLibraryExW reference.
        unsafe {
            free_library(self.module.as_ptr());
        }
    }
}

pub(crate) struct SecurityEnvironment {
    handle: HANDLE,
    module: NonNull<c_void>,
    close: CloseProcessSecurityEnvironment,
}

impl SecurityEnvironment {
    #[allow(
        dead_code,
        reason = "used by the sandbox-helper path include; the library copy owns the same ABI"
    )]
    pub(crate) fn raw(&self) -> HANDLE {
        self.handle
    }
}

impl Drop for SecurityEnvironment {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            // SAFETY: the handle came from this module's create export and is
            // closed exactly once before the DLL reference is released.
            unsafe { (self.close)(self.handle) };
            self.handle = std::ptr::null_mut();
        }
        // SAFETY: ownership of the LoadLibraryExW reference moved from the API
        // into this environment when create succeeded.
        unsafe {
            free_library(self.module.as_ptr());
        }
    }
}

fn resolve(module: NonNull<c_void>, name: &[u8]) -> io::Result<NonNull<c_void>> {
    debug_assert_eq!(name.last(), Some(&0));
    // SAFETY: `module` is a live module and `name` is static NUL-terminated
    // ASCII for the duration of the call.
    let address = unsafe { get_proc_address(module.as_ptr(), name.as_ptr()) };
    NonNull::new(address).ok_or_else(io::Error::last_os_error)
}

fn hresult_result(function: &str, hresult: i32) -> io::Result<()> {
    if hresult < 0 {
        Err(io::Error::other(format!(
            "{function} failed with HRESULT 0x{:08X}",
            hresult as u32
        )))
    } else {
        Ok(())
    }
}

fn system_processmodel_path() -> io::Result<Vec<u16>> {
    let mut buffer = vec![0_u16; 260];
    let length = loop {
        // SAFETY: buffer is writable for the advertised number of UTF-16 code
        // units and GetSystemDirectoryW returns the required length if larger.
        let length = unsafe { get_system_directory(buffer.as_mut_ptr(), buffer.len() as u32) };
        if length == 0 {
            return Err(io::Error::last_os_error());
        }
        if (length as usize) < buffer.len() {
            break length as usize;
        }
        buffer.resize(length as usize + 1, 0);
    };
    let directory = PathBuf::from(OsString::from_wide(&buffer[..length]));
    let path = directory.join("processmodel.dll");
    let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid System32 path"));
    }
    wide.push(0);
    Ok(wide)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_host_load_is_typed_and_never_panics() {
        match SecurityEnvironmentApi::load() {
            Ok(api) => {
                let _ = api.query_support();
            }
            Err(error) => {
                assert!(error.raw_os_error().is_some() || !error.to_string().is_empty());
            }
        }
    }

    #[test]
    fn security_environment_attribute_value_matches_windows_contract() {
        assert_eq!(PROC_THREAD_ATTRIBUTE_SECURITY_ENVIRONMENT, 35 | 0x0002_0000);
    }
}
