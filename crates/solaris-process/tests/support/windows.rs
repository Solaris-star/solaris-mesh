use std::ffi::OsStr;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, GENERIC_WRITE, INVALID_HANDLE_VALUE, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSecurityDescriptorToStringSecurityDescriptorW, ConvertStringSidToSidW, EXPLICIT_ACCESS_W, GRANT_ACCESS,
    GetNamedSecurityInfoW, SDDL_REVISION_1, SE_FILE_OBJECT, SetEntriesInAclW, SetNamedSecurityInfoW, TRUSTEE_IS_SID,
    TRUSTEE_IS_WELL_KNOWN_GROUP, TRUSTEE_W,
};
use windows_sys::Win32::Security::{ACL, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, OBJECT_INHERIT_ACE};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_ALL_ACCESS, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, OPEN_EXISTING,
};
use windows_sys::Win32::System::IO::DeviceIoControl;

const FSCTL_SET_REPARSE_POINT: u32 = 589_988;
const IO_REPARSE_TAG_MOUNT_POINT: u32 = 0xA000_0003;

pub(super) fn create_junction(link: &Path, target: &Path) -> io::Result<()> {
    std::fs::create_dir(link)?;
    let target = conventional_path(&target.canonicalize()?);
    let print_name = target.as_os_str().encode_wide().collect::<Vec<_>>();
    let substitute = OsStr::new(&format!(r"\??\{}", target.display()))
        .encode_wide()
        .collect::<Vec<_>>();
    let buffer = mount_point_buffer(&substitute, &print_name)?;
    let link = wide_nul(link.as_os_str())?;
    let handle = unsafe {
        CreateFileW(
            link.as_ptr(),
            GENERIC_WRITE,
            0,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    let mut returned = 0_u32;
    let result = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_REPARSE_POINT,
            buffer.as_ptr().cast(),
            u32::try_from(buffer.len()).map_err(|_| invalid_input())?,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
        )
    };
    unsafe {
        CloseHandle(handle);
    }
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn grant_everyone_full(path: &Path) -> io::Result<()> {
    let is_dir = path.is_dir();
    let mut everyone_text = wide_nul(OsStr::new("S-1-1-0"))?;
    let mut everyone = std::ptr::null_mut();
    if unsafe { ConvertStringSidToSidW(everyone_text.as_mut_ptr(), &mut everyone) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let everyone = LocalMemory(everyone);
    let mut dacl: *mut ACL = std::ptr::null_mut();
    let mut descriptor = std::ptr::null_mut();
    let mut path = wide_nul(path.as_os_str())?;
    let result = unsafe {
        GetNamedSecurityInfoW(
            path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if result != ERROR_SUCCESS {
        return Err(win32_error(result));
    }
    let _descriptor = LocalMemory(descriptor);
    let access = EXPLICIT_ACCESS_W {
        grfAccessPermissions: FILE_ALL_ACCESS,
        grfAccessMode: GRANT_ACCESS,
        grfInheritance: if is_dir {
            OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
        } else {
            0
        },
        Trustee: TRUSTEE_W {
            pMultipleTrustee: std::ptr::null_mut(),
            MultipleTrusteeOperation: 0,
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_WELL_KNOWN_GROUP,
            ptstrName: everyone.0.cast(),
        },
    };
    let mut updated: *mut ACL = std::ptr::null_mut();
    let result = unsafe { SetEntriesInAclW(1, &access, dacl, &mut updated) };
    if result != ERROR_SUCCESS {
        return Err(win32_error(result));
    }
    let _updated = LocalMemory(updated.cast());
    let result = unsafe {
        SetNamedSecurityInfoW(
            path.as_mut_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            updated,
            std::ptr::null(),
        )
    };
    if result != ERROR_SUCCESS {
        return Err(win32_error(result));
    }
    Ok(())
}

pub(super) fn dacl_sddl(path: &Path) -> io::Result<String> {
    let path = wide_nul(path.as_os_str())?;
    let mut descriptor = std::ptr::null_mut();
    let result = unsafe {
        GetNamedSecurityInfoW(
            path.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if result != ERROR_SUCCESS {
        return Err(win32_error(result));
    }
    let _descriptor = LocalMemory(descriptor);
    let mut text = std::ptr::null_mut();
    let mut length = 0_u32;
    if unsafe {
        ConvertSecurityDescriptorToStringSecurityDescriptorW(
            descriptor,
            SDDL_REVISION_1,
            DACL_SECURITY_INFORMATION,
            &mut text,
            &mut length,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let text_memory = LocalMemory(text.cast());
    let text = unsafe { std::slice::from_raw_parts(text_memory.0.cast::<u16>(), length as usize) };
    Ok(String::from_utf16_lossy(text).trim_end_matches('\0').to_owned())
}

fn mount_point_buffer(substitute: &[u16], print_name: &[u16]) -> io::Result<Vec<u8>> {
    let substitute_bytes =
        u16::try_from(substitute.len().checked_mul(2).ok_or_else(invalid_input)?).map_err(|_| invalid_input())?;
    let print_bytes =
        u16::try_from(print_name.len().checked_mul(2).ok_or_else(invalid_input)?).map_err(|_| invalid_input())?;
    let print_offset = substitute_bytes.checked_add(2).ok_or_else(invalid_input)?;
    let path_bytes = usize::from(substitute_bytes)
        .checked_add(2)
        .and_then(|size| size.checked_add(usize::from(print_bytes)))
        .and_then(|size| size.checked_add(2))
        .ok_or_else(invalid_input)?;
    let data_length =
        u16::try_from(8_usize.checked_add(path_bytes).ok_or_else(invalid_input)?).map_err(|_| invalid_input())?;
    let mut buffer = Vec::with_capacity(8 + usize::from(data_length));
    buffer.extend(IO_REPARSE_TAG_MOUNT_POINT.to_le_bytes());
    buffer.extend(data_length.to_le_bytes());
    buffer.extend(0_u16.to_le_bytes());
    buffer.extend(0_u16.to_le_bytes());
    buffer.extend(substitute_bytes.to_le_bytes());
    buffer.extend(print_offset.to_le_bytes());
    buffer.extend(print_bytes.to_le_bytes());
    extend_wide(&mut buffer, substitute);
    buffer.extend(0_u16.to_le_bytes());
    extend_wide(&mut buffer, print_name);
    buffer.extend(0_u16.to_le_bytes());
    Ok(buffer)
}

fn extend_wide(buffer: &mut Vec<u8>, value: &[u16]) {
    for unit in value {
        buffer.extend(unit.to_le_bytes());
    }
}

fn conventional_path(path: &Path) -> PathBuf {
    let value = path.as_os_str().to_string_lossy();
    if let Some(rest) = value.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{rest}"))
    } else if let Some(rest) = value.strip_prefix(r"\\?\") {
        PathBuf::from(rest)
    } else {
        path.to_path_buf()
    }
}

fn wide_nul(value: &OsStr) -> io::Result<Vec<u16>> {
    let mut result = value.encode_wide().collect::<Vec<_>>();
    if result.contains(&0) {
        return Err(invalid_input());
    }
    result.push(0);
    Ok(result)
}

fn invalid_input() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid junction target")
}

struct LocalMemory(*mut std::ffi::c_void);

impl Drop for LocalMemory {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.0);
        }
    }
}

fn win32_error(code: u32) -> io::Error {
    io::Error::from_raw_os_error(code as i32)
}
