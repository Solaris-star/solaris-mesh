use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::Path;

use tokio::process::Command;

const SAFE_ENVIRONMENT_KEYS: &[&str] = &[
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "SYSTEMDRIVE",
    "COMSPEC",
    "TEMP",
    "TMP",
    "TMPDIR",
    "HOME",
    "USERPROFILE",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TERM",
    "SOLARIS_MAX_ACTIVE_AGENTS",
    "SOLARIS_MAX_CONCURRENT_EFFECTS",
    "SOLARIS_MAX_SPAWN_DEPTH",
    "SOLARIS_MAX_TOTAL_DESCENDANTS",
    "SOLARIS_MAX_RUN_TURNS",
    "SOLARIS_MAX_RUN_TOKENS",
    "SOLARIS_MAX_RUN_WALL_TIME_MS",
    "SOLARIS_MAX_RUN_COST",
    "SOLARIS_MAX_PROCESS_OUTPUT_BYTES",
];

const RESOURCE_ENVIRONMENT_KEYS: &[&str] = &[
    "SOLARIS_MAX_ACTIVE_AGENTS",
    "SOLARIS_MAX_CONCURRENT_EFFECTS",
    "SOLARIS_MAX_SPAWN_DEPTH",
    "SOLARIS_MAX_TOTAL_DESCENDANTS",
    "SOLARIS_MAX_RUN_TURNS",
    "SOLARIS_MAX_RUN_TOKENS",
    "SOLARIS_MAX_RUN_WALL_TIME_MS",
    "SOLARIS_MAX_RUN_COST",
    "SOLARIS_MAX_PROCESS_OUTPUT_BYTES",
];

const NETWORK_PROXY_ENVIRONMENT_KEYS: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    "AWS_CA_BUNDLE",
    "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
    "GIT_SSL_CAINFO",
    "NIX_SSL_CERT_FILE",
    "PIP_CERT",
];

#[cfg(any(unix, windows))]
const NETWORK_PROXY_CA_ENVIRONMENT_KEYS: &[&str] = &[
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    "AWS_CA_BUNDLE",
    "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
    "GIT_SSL_CAINFO",
    "NIX_SSL_CERT_FILE",
    "PIP_CERT",
];

#[cfg(windows)]
const WINDOWS_PROTECTED_ENVIRONMENT_KEYS: &[&str] = &[
    "SYSTEMROOT",
    "WINDIR",
    "SYSTEMDRIVE",
    "COMSPEC",
    "PATHEXT",
    "PATH",
    "HOME",
    "USERPROFILE",
];

/// Keeps only supported process resource limits with valid numeric values.
pub fn filter_resource_environment(values: impl IntoIterator<Item = (String, String)>) -> Vec<(String, String)> {
    let mut environment = BTreeMap::new();
    for (key, value) in values {
        let Some(canonical) = canonical_resource_key(OsStr::new(&key)) else {
            continue;
        };
        if valid_resource_value(canonical, OsStr::new(&value)) {
            environment.insert(canonical.to_owned(), value);
        }
    }
    environment.into_iter().collect()
}

/// Clears ambient state and installs the minimal cross-platform process environment.
pub fn configure_safe_process_environment(command: &mut Command) {
    command.env_clear().envs(safe_process_environment());
}

pub(crate) fn configure_explicit_process_environment<I, K, V>(command: &mut Command, values: I)
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    for (key, value) in values {
        if !is_protected_explicit_environment_key(key.as_ref()) {
            command.env(key, value);
        }
    }
}

pub(crate) fn configure_safe_process_environment_with_overrides<I, K, V>(command: &mut Command, overrides: I)
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    let mut environment = safe_process_environment();
    environment.extend(filtered_override_environment(overrides));
    command.env_clear().envs(environment);
}

fn safe_process_environment() -> BTreeMap<OsString, OsString> {
    let mut environment = filtered_environment(std::env::vars_os());
    ensure_platform_environment(&mut environment);
    environment
}

fn filtered_environment<I, K, V>(values: I) -> BTreeMap<OsString, OsString>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    values
        .into_iter()
        .filter_map(|(key, value)| {
            let canonical = canonical_safe_key(key.as_ref())?;
            if canonical_resource_key(OsStr::new(canonical)).is_some()
                && !valid_resource_value(canonical, value.as_ref())
            {
                return None;
            }
            Some((OsString::from(canonical), value.as_ref().to_owned()))
        })
        .collect()
}

fn filtered_override_environment<I, K, V>(values: I) -> BTreeMap<OsString, OsString>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    filtered_environment(values)
        .into_iter()
        .filter(|(key, value)| canonical_resource_key(key).is_some() || (is_temp_key(key) && safe_absolute_path(value)))
        .collect()
}

fn is_temp_key(key: &OsStr) -> bool {
    ["TEMP", "TMP", "TMPDIR"]
        .iter()
        .any(|candidate| environment_keys_equal(&key.to_string_lossy(), candidate))
}

fn is_protected_explicit_environment_key(key: &OsStr) -> bool {
    if is_temp_key(key) || is_network_proxy_environment_key(key) {
        return true;
    }
    #[cfg(windows)]
    {
        let key = key.to_string_lossy();
        WINDOWS_PROTECTED_ENVIRONMENT_KEYS
            .iter()
            .any(|candidate| environment_keys_equal(&key, candidate))
    }
    #[cfg(not(windows))]
    {
        false
    }
}

pub(crate) fn is_network_proxy_environment_key(key: &OsStr) -> bool {
    let key = key.to_string_lossy();
    NETWORK_PROXY_ENVIRONMENT_KEYS
        .iter()
        .any(|candidate| key.eq_ignore_ascii_case(candidate))
}

#[cfg(any(unix, windows))]
pub(crate) fn append_network_proxy_ca_environment(
    environment: &mut Vec<(OsString, OsString)>,
    certificate_path: &Path,
) {
    let certificate_path = certificate_path.as_os_str().to_owned();
    environment.extend(
        NETWORK_PROXY_CA_ENVIRONMENT_KEYS
            .iter()
            .map(|key| (OsString::from(key), certificate_path.clone())),
    );
}

fn canonical_safe_key(key: &OsStr) -> Option<&'static str> {
    let key = key.to_string_lossy();
    SAFE_ENVIRONMENT_KEYS
        .iter()
        .copied()
        .find(|candidate| environment_keys_equal(&key, candidate))
}

fn canonical_resource_key(key: &OsStr) -> Option<&'static str> {
    let key = key.to_string_lossy();
    RESOURCE_ENVIRONMENT_KEYS
        .iter()
        .copied()
        .find(|candidate| environment_keys_equal(&key, candidate))
}

fn valid_resource_value(key: &str, value: &OsStr) -> bool {
    let Some(value) = value.to_str() else {
        return false;
    };
    if value.is_empty() || value.len() > 64 {
        return false;
    }
    if key == "SOLARIS_MAX_RUN_COST" {
        return value
            .parse::<f64>()
            .is_ok_and(|value| value.is_finite() && value >= 0.0);
    }
    value.bytes().all(|byte| byte.is_ascii_digit()) && value.parse::<u64>().is_ok()
}

#[cfg(windows)]
fn environment_keys_equal(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

#[cfg(not(windows))]
fn environment_keys_equal(left: &str, right: &str) -> bool {
    left == right
}

#[cfg(windows)]
fn ensure_platform_environment(environment: &mut BTreeMap<OsString, OsString>) {
    use std::path::{Component, Prefix};

    let windows_directory = windows_directory();
    let trusted_temp = trusted_windows_temp_directory().map(std::path::PathBuf::into_os_string);
    if let Some(windows_directory) = windows_directory {
        environment.insert(OsString::from("SYSTEMROOT"), windows_directory.as_os_str().to_owned());
        environment.insert(OsString::from("WINDIR"), windows_directory.as_os_str().to_owned());
        environment.insert(
            OsString::from("COMSPEC"),
            windows_directory.join("System32").join("cmd.exe").into_os_string(),
        );
        if let Some(Component::Prefix(prefix)) = windows_directory.components().next() {
            let drive = match prefix.kind() {
                Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => {
                    Some(OsString::from(format!("{}:", char::from(letter))))
                }
                _ => None,
            };
            if let Some(drive) = drive {
                environment.insert(OsString::from("SYSTEMDRIVE"), drive);
            }
        }
        if environment.get(OsStr::new("PATH")).is_none_or(|value| value.is_empty())
            && let Ok(path) = std::env::join_paths([windows_directory.join("System32"), windows_directory.clone()])
        {
            environment.insert(OsString::from("PATH"), path);
        }
    }

    if !environment.contains_key(OsStr::new("PATHEXT")) {
        environment.insert(OsString::from("PATHEXT"), OsString::from(".COM;.EXE;.BAT;.CMD"));
    }
    replace_temp_paths(environment, &["TEMP", "TMP", "TMPDIR"], trusted_temp);
    for key in ["HOME", "USERPROFILE"] {
        if environment
            .get(OsStr::new(key))
            .is_some_and(|value| !safe_absolute_path(value))
        {
            environment.remove(OsStr::new(key));
        }
    }
}

#[cfg(windows)]
fn trusted_windows_temp_directory() -> Option<std::path::PathBuf> {
    use std::io::Write;
    use std::sync::OnceLock;

    static TRUSTED_TEMP: OnceLock<Option<std::path::PathBuf>> = OnceLock::new();
    TRUSTED_TEMP
        .get_or_init(|| {
            let temp = local_app_data_directory()?.join("Temp");
            if !safe_absolute_path(temp.as_os_str()) {
                return None;
            }
            std::fs::create_dir_all(&temp).ok()?;
            let mut probe = tempfile::tempfile_in(&temp).ok()?;
            probe.write_all(b"solaris-temp-probe").ok()?;
            Some(temp)
        })
        .clone()
}

#[cfg(windows)]
fn local_app_data_directory() -> Option<std::path::PathBuf> {
    use std::os::windows::ffi::OsStringExt;

    use windows_sys::Win32::Foundation::S_OK;
    use windows_sys::Win32::System::Com::CoTaskMemFree;
    use windows_sys::Win32::UI::Shell::{FOLDERID_LocalAppData, SHGetKnownFolderPath};

    let mut raw_path = std::ptr::null_mut();
    // SAFETY: SHGetKnownFolderPath initializes raw_path with a null-terminated
    // CoTaskMem allocation, which is released below on every return path.
    let status = unsafe { SHGetKnownFolderPath(&FOLDERID_LocalAppData, 0, std::ptr::null_mut(), &mut raw_path) };
    if status != S_OK || raw_path.is_null() {
        // SAFETY: CoTaskMemFree accepts null and allocations returned on failure.
        unsafe { CoTaskMemFree(raw_path.cast()) };
        return None;
    }
    let mut length = 0_usize;
    // SAFETY: A successful SHGetKnownFolderPath call returns a null-terminated
    // string. The documented Windows path limit bounds a malformed result.
    while length <= 32_768 && unsafe { *raw_path.add(length) } != 0 {
        length += 1;
    }
    let path = if length <= 32_768 {
        // SAFETY: The loop found the terminator, so these code units belong to
        // the allocation and exclude its trailing null.
        let value = unsafe { std::slice::from_raw_parts(raw_path, length) };
        Some(std::path::PathBuf::from(OsString::from_wide(value)))
    } else {
        None
    };
    // SAFETY: raw_path came from SHGetKnownFolderPath and is no longer used.
    unsafe { CoTaskMemFree(raw_path.cast()) };
    path.filter(|path| safe_absolute_path(path.as_os_str()))
}

#[cfg(windows)]
fn windows_directory() -> Option<std::path::PathBuf> {
    use std::os::windows::ffi::OsStringExt;

    use windows_sys::Win32::System::SystemInformation::GetWindowsDirectoryW;

    let mut buffer = vec![0_u16; 260];
    loop {
        let length = unsafe { GetWindowsDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32) } as usize;
        if length == 0 || length > 32_768 {
            return None;
        }
        if length < buffer.len() {
            buffer.truncate(length);
            let path = std::path::PathBuf::from(OsString::from_wide(&buffer));
            return safe_absolute_path(path.as_os_str()).then_some(path);
        }
        buffer.resize(length + 1, 0);
    }
}

#[cfg(not(windows))]
fn ensure_platform_environment(environment: &mut BTreeMap<OsString, OsString>) {
    let fallback = std::env::temp_dir();
    let fallback = safe_absolute_path(fallback.as_os_str()).then(|| fallback.into_os_string());
    ensure_safe_temp_paths(environment, &["TMPDIR", "TMP", "TEMP"], fallback);
}

#[cfg(windows)]
fn replace_temp_paths(environment: &mut BTreeMap<OsString, OsString>, keys: &[&str], trusted_temp: Option<OsString>) {
    for key in keys {
        let key = OsStr::new(key);
        if let Some(trusted_temp) = trusted_temp.as_ref() {
            environment.insert(key.to_owned(), trusted_temp.clone());
        } else {
            environment.remove(key);
        }
    }
}

#[cfg(not(windows))]
fn ensure_safe_temp_paths(environment: &mut BTreeMap<OsString, OsString>, keys: &[&str], fallback: Option<OsString>) {
    for key in keys {
        let key = OsStr::new(key);
        if environment.get(key).is_some_and(|value| safe_absolute_path(value)) {
            continue;
        }
        if let Some(fallback) = fallback.as_ref() {
            environment.insert(key.to_owned(), fallback.clone());
        } else {
            environment.remove(key);
        }
    }
}

fn safe_absolute_path(value: &OsStr) -> bool {
    Path::new(value).is_absolute() && !value.to_string_lossy().contains('%')
}

#[cfg(test)]
#[path = "environment_test.rs"]
mod environment_test;
