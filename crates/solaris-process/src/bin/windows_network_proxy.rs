use std::ffi::{OsStr, OsString};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use windows_sys::Win32::Foundation::{CloseHandle, ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS, HANDLE};
use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TokenIsAppContainer};
use windows_sys::Win32::Storage::Packaging::Appx::GetCurrentPackageFamilyName;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use super::network_proxy::{HostNetworkProxy, NetworkProxyPolicy};

const CONTROL_PROTOCOL_VERSION: u32 = 1;
const APPLICATION_ID: &str = "Proxy";
const NONCE_HEX_LENGTH: usize = 64;
const DYNAMIC_PORT_MIN: u16 = 49_152;
const MAX_CONTROL_LINE_BYTES: usize = 4 * 1024;
const MAX_CA_CERTIFICATE_BYTES: usize = 64 * 1024;

pub(super) fn run() -> io::Result<()> {
    let invocation = Invocation::parse(std::env::args_os().skip(1))?;
    run_with(&SystemRuntimeIdentity, invocation, RuntimeLimits::production())
}

struct Invocation {
    control_port: u16,
    proxy_port: u16,
    nonce: String,
    domains: Vec<String>,
}

impl Invocation {
    fn parse(arguments: impl IntoIterator<Item = OsString>) -> io::Result<Self> {
        let mut arguments = arguments.into_iter();
        let control_port = parse_port(&mut arguments, "--control-port")?;
        let proxy_port = parse_port(&mut arguments, "--proxy-port")?;
        if control_port == proxy_port {
            return Err(invalid_input());
        }
        expect_flag(&mut arguments, "--nonce")?;
        let nonce = arguments
            .next()
            .and_then(|value| value.into_string().ok())
            .filter(|value| is_valid_nonce(value))
            .ok_or_else(invalid_input)?;
        let mut domains = Vec::new();
        while let Some(flag) = arguments.next() {
            if flag != OsStr::new("--domain") {
                return Err(invalid_input());
            }
            let domain = arguments
                .next()
                .and_then(|value| value.into_string().ok())
                .ok_or_else(invalid_input)?;
            domains.push(domain);
        }
        if domains.is_empty() {
            return Err(invalid_input());
        }
        Ok(Self {
            control_port,
            proxy_port,
            nonce,
            domains,
        })
    }
}

fn parse_port(arguments: &mut impl Iterator<Item = OsString>, flag: &str) -> io::Result<u16> {
    expect_flag(arguments, flag)?;
    arguments
        .next()
        .and_then(|value| value.to_str().and_then(|value| value.parse::<u16>().ok()))
        .filter(|port| *port >= DYNAMIC_PORT_MIN)
        .ok_or_else(invalid_input)
}

fn expect_flag(arguments: &mut impl Iterator<Item = OsString>, expected: &str) -> io::Result<()> {
    if arguments.next().as_deref() == Some(OsStr::new(expected)) {
        Ok(())
    } else {
        Err(invalid_input())
    }
}

fn is_valid_nonce(value: &str) -> bool {
    value.len() == NONCE_HEX_LENGTH && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[derive(Clone, Copy)]
struct RuntimeLimits {
    accept_timeout: Duration,
    handshake_timeout: Duration,
}

impl RuntimeLimits {
    const fn production() -> Self {
        Self {
            accept_timeout: Duration::from_secs(10),
            handshake_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PackageIdentity {
    family_name: String,
    is_app_container: bool,
}

trait RuntimeIdentity {
    fn current(&self) -> io::Result<PackageIdentity>;
}

struct SystemRuntimeIdentity;

impl RuntimeIdentity for SystemRuntimeIdentity {
    fn current(&self) -> io::Result<PackageIdentity> {
        Ok(PackageIdentity {
            family_name: current_package_family_name()?,
            is_app_container: current_process_is_app_container()?,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlHello {
    protocol: u32,
    nonce: String,
}

#[derive(Serialize)]
struct ControlReady<'a> {
    protocol: u32,
    nonce: &'a str,
    package_family_name: &'a str,
    application_user_model_id: String,
    proxy_url: String,
    ca_certificate_pem: &'a str,
}

fn run_with(identity_provider: &impl RuntimeIdentity, invocation: Invocation, limits: RuntimeLimits) -> io::Result<()> {
    let identity = identity_provider.current()?;
    if !identity.is_app_container || identity.family_name.is_empty() {
        return Err(permission_denied());
    }
    let policy = NetworkProxyPolicy::from_permission_domains(&invocation.domains).map_err(|_| invalid_input())?;
    if policy.is_empty() {
        return Err(invalid_input());
    }

    let control_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, invocation.control_port))?;
    control_listener.set_nonblocking(true)?;
    let private_state = tempfile::Builder::new().prefix("solaris-network-proxy-").tempdir()?;
    let state_path = private_state.path().join("proxy.state");
    let proxy = HostNetworkProxy::start_on_port(&state_path, invocation.proxy_port, policy)?;
    proxy.verify_ca_certificate()?;
    let ca_certificate = std::fs::read(proxy.ca_certificate_path())?;
    if ca_certificate.is_empty() || ca_certificate.len() > MAX_CA_CERTIFICATE_BYTES {
        return Err(permission_denied());
    }
    let ca_certificate = std::str::from_utf8(&ca_certificate).map_err(|_| permission_denied())?;

    let control = accept_control(&control_listener, limits.accept_timeout)?;
    control.set_read_timeout(Some(limits.handshake_timeout))?;
    control.set_write_timeout(Some(limits.handshake_timeout))?;
    let mut control = BufReader::new(control);
    let hello: ControlHello = read_json_line(&mut control, MAX_CONTROL_LINE_BYTES)?;
    if hello.protocol != CONTROL_PROTOCOL_VERSION || !constant_time_equal(&hello.nonce, &invocation.nonce) {
        return Err(permission_denied());
    }
    let ready = ControlReady {
        protocol: CONTROL_PROTOCOL_VERSION,
        nonce: &invocation.nonce,
        package_family_name: &identity.family_name,
        application_user_model_id: format!("{}!{APPLICATION_ID}", identity.family_name),
        proxy_url: proxy.proxy_url(),
        ca_certificate_pem: ca_certificate,
    };
    serde_json::to_writer(control.get_mut(), &ready).map_err(|_| io::Error::other("control encoding failed"))?;
    control.get_mut().write_all(b"\n")?;
    control.get_mut().flush()?;
    control.get_mut().set_read_timeout(None)?;
    control.get_mut().set_write_timeout(None)?;

    let mut buffer = [0_u8; 256];
    loop {
        match control.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_) => return Err(permission_denied()),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn accept_control(listener: &TcpListener, timeout: Duration) -> io::Result<TcpStream> {
    let deadline = Instant::now() + timeout;
    loop {
        match listener.accept() {
            Ok((stream, SocketAddr::V4(address))) if address.ip().is_loopback() => {
                stream.set_nonblocking(false)?;
                return Ok(stream);
            }
            Ok(_) => return Err(permission_denied()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Err(io::Error::from(io::ErrorKind::TimedOut));
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn read_json_line<T: for<'de> Deserialize<'de>>(reader: &mut impl BufRead, limit: usize) -> io::Result<T> {
    let mut line = Vec::new();
    reader.take((limit + 1) as u64).read_until(b'\n', &mut line)?;
    if line.is_empty() || line.len() > limit || line.last() != Some(&b'\n') {
        return Err(invalid_input());
    }
    serde_json::from_slice(&line).map_err(|_| invalid_input())
}

fn constant_time_equal(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

fn current_package_family_name() -> io::Result<String> {
    let mut length = 0_u32;
    // SAFETY: the first call supplies the documented null output buffer and a
    // live length out-parameter in order to query the required UTF-16 length.
    let result = unsafe { GetCurrentPackageFamilyName(&mut length, std::ptr::null_mut()) };
    if result != ERROR_INSUFFICIENT_BUFFER || length < 2 {
        return Err(permission_denied());
    }
    let mut buffer = vec![0_u16; length as usize];
    // SAFETY: `buffer` contains `length` writable UTF-16 code units and the
    // API receives the same live length value returned by the sizing call.
    let result = unsafe { GetCurrentPackageFamilyName(&mut length, buffer.as_mut_ptr()) };
    if result != ERROR_SUCCESS || length < 2 || length as usize > buffer.len() {
        return Err(permission_denied());
    }
    let value = &buffer[..length as usize];
    let value = value.strip_suffix(&[0]).ok_or_else(permission_denied)?;
    String::from_utf16(value).map_err(|_| permission_denied())
}

fn current_process_is_app_container() -> io::Result<bool> {
    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: the pseudo process handle is valid for the current process and
    // `token` is a live out-parameter which is closed by `TokenHandle`.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = TokenHandle(token);
    let mut value = 0_u32;
    let mut returned = 0_u32;
    // SAFETY: `value` is a writable u32 of the documented size for
    // TokenIsAppContainer and `token` remains valid for the complete call.
    if unsafe {
        GetTokenInformation(
            token.0,
            TokenIsAppContainer,
            (&mut value as *mut u32).cast(),
            size_of::<u32>() as u32,
            &mut returned,
        )
    } == 0
        || returned != size_of::<u32>() as u32
    {
        return Err(io::Error::last_os_error());
    }
    Ok(value != 0)
}

struct TokenHandle(HANDLE);

impl Drop for TokenHandle {
    fn drop(&mut self) {
        // SAFETY: this owner contains exactly one handle returned by a
        // successful OpenProcessToken call.
        let _ = unsafe { CloseHandle(self.0) };
    }
}

fn invalid_input() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid packaged proxy invocation")
}

fn permission_denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "packaged proxy identity or control rejected",
    )
}

#[cfg(test)]
#[path = "windows_network_proxy_test.rs"]
mod windows_network_proxy_test;
