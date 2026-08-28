use std::collections::BTreeSet;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, HANDLE, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::Networking::WinSock::{
    ADDRINFOEXW, AF_INET, AF_INET6, AF_UNSPEC, AI_BYPASS_DNS_CACHE, FreeAddrInfoExW, GetAddrInfoExCancel,
    GetAddrInfoExOverlappedResult, GetAddrInfoExW, IPPROTO_TCP, NS_DNS, SOCK_STREAM, WSA_IO_PENDING,
    WSA_OPERATION_ABORTED, WSADATA, WSAETIMEDOUT, WSAHOST_NOT_FOUND, WSANO_DATA, WSANO_RECOVERY, WSAStartup,
    WSATRY_AGAIN,
};
use windows_sys::Win32::System::IO::OVERLAPPED;
use windows_sys::Win32::System::Threading::{CreateEventW, INFINITE, WaitForSingleObject};

use super::super::is_public_destination;

pub(super) const MAX_RESOLVED_ADDRESSES: usize = 16;
static WINSOCK_READY: OnceLock<Result<(), i32>> = OnceLock::new();

pub(super) fn resolve_public_addresses(
    host: &str,
    port: u16,
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<Vec<SocketAddr>> {
    if stopped.load(Ordering::Acquire) {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "proxy DNS resolution timed out",
        ));
    }
    ensure_winsock()?;

    // A trailing dot makes this an absolute DNS name and prevents Windows
    // search-suffix expansion from turning an approved exact host into a
    // different intranet/private destination.
    let absolute_name = format!("{host}.");
    let wide_name = absolute_name
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let hints = ADDRINFOEXW {
        ai_flags: AI_BYPASS_DNS_CACHE as i32,
        ai_family: i32::from(AF_UNSPEC),
        ai_socktype: SOCK_STREAM,
        ai_protocol: IPPROTO_TCP,
        ..ADDRINFOEXW::default()
    };
    let event = EventHandle::new()?;
    let mut overlapped = OVERLAPPED {
        hEvent: event.0,
        ..OVERLAPPED::default()
    };
    let mut query_handle: HANDLE = ptr::null_mut();
    let mut result = ptr::null_mut();
    // SAFETY: all pointers refer to live, correctly initialized values. The
    // OVERLAPPED and event remain alive until an asynchronous query completes
    // (or cancellation has completed) before this function returns.
    let initial_status = unsafe {
        GetAddrInfoExW(
            wide_name.as_ptr(),
            ptr::null(),
            NS_DNS,
            ptr::null(),
            &hints,
            &mut result,
            ptr::null(),
            &overlapped,
            None,
            &mut query_handle,
        )
    };
    let status = if initial_status == WSA_IO_PENDING {
        wait_for_query(&mut overlapped, query_handle, event.0, stopped, deadline)?
    } else {
        initial_status
    };
    if status != 0 {
        if !result.is_null() {
            // SAFETY: result came from GetAddrInfoExW and has not been freed.
            unsafe { FreeAddrInfoExW(result) };
        }
        return Err(winsock_error(status));
    }

    let parsed = collect_addresses(result, port);
    if !result.is_null() {
        // SAFETY: result came from a successful GetAddrInfoExW and has not been freed.
        unsafe { FreeAddrInfoExW(result) };
    }
    parsed
}

fn ensure_winsock() -> io::Result<()> {
    let status = WINSOCK_READY.get_or_init(|| {
        let mut data = WSADATA::default();
        // SAFETY: `data` is writable for the duration of WSAStartup.
        let status = unsafe { WSAStartup(0x0202, &mut data) };
        if status == 0 { Ok(()) } else { Err(status) }
    });
    match status {
        Ok(()) => Ok(()),
        Err(code) => Err(io::Error::other(format!("WSAStartup failed with Winsock error {code}"))),
    }
}

struct EventHandle(HANDLE);

impl EventHandle {
    fn new() -> io::Result<Self> {
        // SAFETY: unnamed manual-reset event with no custom security descriptor.
        let handle = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::from_raw_os_error(unsafe { GetLastError() } as i32));
        }
        Ok(Self(handle))
    }
}

impl Drop for EventHandle {
    fn drop(&mut self) {
        // SAFETY: this wrapper exclusively owns the event handle.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

fn wait_for_query(
    overlapped: &mut OVERLAPPED,
    query_handle: HANDLE,
    event: HANDLE,
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<i32> {
    loop {
        if stopped.load(Ordering::Acquire) {
            cancel_query_and_wait(query_handle, event);
            return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            cancel_query_and_wait(query_handle, event);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "proxy DNS resolution timed out",
            ));
        }
        let wait_ms = duration_to_wait_ms(remaining.min(Duration::from_millis(50)));
        // SAFETY: event is a valid manual-reset event owned by EventHandle.
        match unsafe { WaitForSingleObject(event, wait_ms) } {
            WAIT_OBJECT_0 => {
                // SAFETY: GetAddrInfoExW initialized this OVERLAPPED and signaled
                // its event, so the operation has completed.
                return Ok(unsafe { GetAddrInfoExOverlappedResult(overlapped) });
            }
            WAIT_TIMEOUT => {}
            WAIT_FAILED => {
                cancel_query_and_wait(query_handle, event);
                return Err(io::Error::from_raw_os_error(unsafe { GetLastError() } as i32));
            }
            value => {
                cancel_query_and_wait(query_handle, event);
                return Err(io::Error::other(format!(
                    "proxy DNS wait returned unexpected status {value}"
                )));
            }
        }
    }
}

fn cancel_query_and_wait(query_handle: HANDLE, event: HANDLE) {
    if !query_handle.is_null() {
        // SAFETY: query_handle was produced by a pending GetAddrInfoExW call.
        unsafe {
            let _ = GetAddrInfoExCancel(&query_handle);
        }
    }
    // The OVERLAPPED/result pointers must remain alive until cancellation is
    // observed. Microsoft documents waiting for completion after cancellation.
    // This wait is confined to the packaged proxy process; process teardown is
    // the final containment boundary if the system namespace provider fails.
    // SAFETY: event remains valid until this function returns.
    unsafe {
        let _ = WaitForSingleObject(event, INFINITE);
    }
}

fn duration_to_wait_ms(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis().max(1).min(u128::from(u32::MAX))).unwrap_or(u32::MAX)
}

fn collect_addresses(mut entry: *mut ADDRINFOEXW, port: u16) -> io::Result<Vec<SocketAddr>> {
    let mut addresses = BTreeSet::new();
    while !entry.is_null() {
        // SAFETY: the linked list is owned by GetAddrInfoExW until it is freed by the caller.
        let current = unsafe { &*entry };
        let address = sockaddr_ip(current)?;
        if !addresses.insert(address) {
            entry = current.ai_next;
            continue;
        }
        if addresses.len() > MAX_RESOLVED_ADDRESSES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy DNS result exceeds the address limit",
            ));
        }
        entry = current.ai_next;
    }
    if addresses.is_empty() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "proxy DNS result is empty"));
    }
    if addresses.iter().any(|address| !is_public_destination(*address)) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "proxy DNS result is not public",
        ));
    }
    Ok(addresses
        .into_iter()
        .map(|address| SocketAddr::new(address, port))
        .collect())
}

fn sockaddr_ip(entry: &ADDRINFOEXW) -> io::Result<IpAddr> {
    if entry.ai_addr.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS result has no socket address",
        ));
    }
    let length = entry.ai_addrlen;
    // SAFETY: ai_addr is valid for ai_addrlen bytes for each ADDRINFOEXW entry.
    let bytes = unsafe { std::slice::from_raw_parts(entry.ai_addr.cast::<u8>(), length) };
    if bytes.len() < 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS socket address is truncated",
        ));
    }
    let family = u16::from_ne_bytes([bytes[0], bytes[1]]);
    match family {
        AF_INET => {
            if bytes.len() < 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy DNS IPv4 address is truncated",
                ));
            }
            Ok(IpAddr::V4(Ipv4Addr::new(bytes[4], bytes[5], bytes[6], bytes[7])))
        }
        AF_INET6 => {
            if bytes.len() < 24 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy DNS IPv6 address is truncated",
                ));
            }
            let octets = <[u8; 16]>::try_from(&bytes[8..24])
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS IPv6 address is invalid"))?;
            Ok(IpAddr::V6(Ipv6Addr::from(octets)))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS returned an unsupported address family",
        )),
    }
}

fn winsock_error(code: i32) -> io::Error {
    let kind = match code {
        WSAHOST_NOT_FOUND | WSANO_DATA => io::ErrorKind::NotFound,
        WSAETIMEDOUT | WSATRY_AGAIN => io::ErrorKind::TimedOut,
        WSA_OPERATION_ABORTED => io::ErrorKind::Interrupted,
        WSANO_RECOVERY => io::ErrorKind::InvalidData,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, format!("proxy DNS resolution failed with Winsock error {code}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_dns_wait_conversion_is_bounded_and_nonzero() {
        assert_eq!(duration_to_wait_ms(Duration::ZERO), 1);
        assert_eq!(duration_to_wait_ms(Duration::from_millis(1250)), 1250);
    }

    #[test]
    fn windows_dns_rejects_special_destination_after_resolution() {
        let address = build_ipv4_entry(Ipv4Addr::LOCALHOST);
        let error = collect_addresses(address.as_ptr(), 443).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn windows_dns_rejects_a_later_private_rebinding_result_instead_of_reusing_public_state() {
        let public = build_ipv4_entry(Ipv4Addr::new(93, 184, 216, 34));
        assert_eq!(
            collect_addresses(public.as_ptr(), 443).unwrap(),
            vec![SocketAddr::from((Ipv4Addr::new(93, 184, 216, 34), 443))]
        );

        let rebound = build_ipv4_entry(Ipv4Addr::new(10, 0, 0, 7));
        let error = collect_addresses(rebound.as_ptr(), 443).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn windows_dns_requires_every_address_in_one_resolution_to_be_public() {
        let private = build_ipv4_entry(Ipv4Addr::new(10, 0, 0, 8));
        let mut public = build_ipv4_entry(Ipv4Addr::new(93, 184, 216, 34));
        public.info.ai_next = private.as_ptr();
        let error = collect_addresses(public.as_ptr(), 443).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[ignore = "requires live external DNS"]
    #[test]
    fn windows_system_dns_resolves_only_public_addresses_for_real_host() {
        let stopped = AtomicBool::new(false);
        let addresses =
            resolve_public_addresses("example.com", 443, &stopped, Instant::now() + Duration::from_secs(5)).unwrap();
        assert!(!addresses.is_empty());
        assert!(addresses.iter().all(|address| is_public_destination(address.ip())));
        assert!(addresses.iter().all(|address| address.port() == 443));
    }

    struct TestAddrInfo {
        storage: Box<[u8; 16]>,
        info: Box<ADDRINFOEXW>,
    }

    impl TestAddrInfo {
        fn as_ptr(&self) -> *mut ADDRINFOEXW {
            let _keep_alive = &self.storage;
            (&*self.info as *const ADDRINFOEXW).cast_mut()
        }
    }

    fn build_ipv4_entry(address: Ipv4Addr) -> TestAddrInfo {
        let mut storage = Box::new([0_u8; 16]);
        storage[0..2].copy_from_slice(&AF_INET.to_ne_bytes());
        storage[4..8].copy_from_slice(&address.octets());
        let mut info = Box::new(ADDRINFOEXW::default());
        info.ai_family = i32::from(AF_INET);
        info.ai_addrlen = storage.len();
        info.ai_addr = storage.as_mut_ptr().cast();
        TestAddrInfo { storage, info }
    }
}
