use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use super::request::parse_authority;
use super::{MAX_PROXY_HEADER_BYTES, MAX_PROXY_HEADER_COUNT};

pub(super) const MAX_PROXY_BODY_BYTES: u64 = 64 * 1024 * 1024;

pub(super) struct PreparedRequest {
    pub(super) header: Vec<u8>,
    pub(super) body: RequestBody,
}

#[derive(Clone, Copy)]
pub(super) enum RequestBody {
    None,
    ContentLength(u64),
    Chunked,
}

enum RequestTarget<'a> {
    AbsoluteHttp,
    TlsOrigin { host: &'a str, port: u16 },
}

pub(super) fn validate_connect_header(header: &[u8]) -> io::Result<()> {
    let header = header_text(header)?;
    let mut lines = header[..header.len() - 4].split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| invalid_data("proxy CONNECT request line is missing"))?;
    if !request_line.ends_with(" HTTP/1.1") {
        return Err(invalid_data("proxy CONNECT requires HTTP/1.1"));
    }
    let mut host_header = false;
    let mut content_length = false;
    for line in lines {
        let (name, value) = parse_header_line(line)?;
        if name.eq_ignore_ascii_case("host") && std::mem::replace(&mut host_header, true) {
            return Err(invalid_data("proxy CONNECT Host header is duplicated"));
        }
        if name.eq_ignore_ascii_case("content-length") {
            if std::mem::replace(&mut content_length, true) || value.trim() != "0" {
                return Err(invalid_data("proxy CONNECT request body is unsupported"));
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("expect")
            || name.eq_ignore_ascii_case("upgrade")
        {
            return Err(invalid_data("proxy CONNECT request framing is unsupported"));
        }
    }
    if host_header {
        Ok(())
    } else {
        Err(invalid_data("proxy CONNECT Host header is missing"))
    }
}

pub(super) fn prepare_plain_request(header: &[u8]) -> io::Result<PreparedRequest> {
    prepare_request(header, RequestTarget::AbsoluteHttp)
}

pub(super) fn prepare_tls_request(header: &[u8], host: &str, port: u16) -> io::Result<PreparedRequest> {
    prepare_request(header, RequestTarget::TlsOrigin { host, port })
}

fn prepare_request(header: &[u8], target_kind: RequestTarget<'_>) -> io::Result<PreparedRequest> {
    let header = header_text(header)?;
    let mut lines = header[..header.len() - 4].split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| invalid_data("proxy HTTP request line is missing"))?;
    let mut parts = request_line.split(' ');
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || method.is_empty()
        || !method.bytes().all(|byte| byte.is_ascii_uppercase())
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
    {
        return Err(invalid_data("proxy HTTP request line is invalid"));
    }
    let output_target = match target_kind {
        RequestTarget::AbsoluteHttp => origin_target(target)?,
        RequestTarget::TlsOrigin { .. } => {
            if version != "HTTP/1.1" || method == "CONNECT" || !valid_origin_target(target) {
                return Err(invalid_data("proxy TLS request target is invalid"));
            }
            target
        }
    };
    let mut output = format!("{method} {output_target} {version}\r\n").into_bytes();
    let mut content_length = None;
    let mut transfer_encoding = None;
    let mut host_header = None;
    let mut header_count = 0;
    for line in lines {
        header_count += 1;
        if header_count > MAX_PROXY_HEADER_COUNT {
            return Err(invalid_data("proxy HTTP header count is too large"));
        }
        let (name, value) = parse_header_line(line)?;
        if name.eq_ignore_ascii_case("host") && host_header.replace(value.trim()).is_some() {
            return Err(invalid_data("proxy Host header is duplicated"));
        }
        if name.eq_ignore_ascii_case("connection")
            || name.eq_ignore_ascii_case("proxy-connection")
            || name.eq_ignore_ascii_case("proxy-authorization")
        {
            continue;
        }
        if name.eq_ignore_ascii_case("expect") || name.eq_ignore_ascii_case("upgrade") {
            return Err(invalid_data("proxy HTTP protocol switch is unsupported"));
        }
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some()
                || value.trim().is_empty()
                || !value.trim().bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(invalid_data("proxy Content-Length is invalid"));
            }
            let length = value
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|length| *length <= MAX_PROXY_BODY_BYTES)
                .ok_or_else(|| invalid_data("proxy Content-Length exceeds the limit"))?;
            content_length = Some(length);
        }
        if name.eq_ignore_ascii_case("transfer-encoding")
            && (transfer_encoding.replace(value.trim()).is_some() || !value.trim().eq_ignore_ascii_case("chunked"))
        {
            return Err(invalid_data("proxy Transfer-Encoding is unsupported"));
        }
        output.extend_from_slice(line.as_bytes());
        output.extend_from_slice(b"\r\n");
    }
    if content_length.is_some() && transfer_encoding.is_some() {
        return Err(invalid_data("proxy request has conflicting body framing"));
    }
    if let RequestTarget::TlsOrigin { host, port } = target_kind {
        let authority = host_header.ok_or_else(|| invalid_data("proxy TLS Host header is missing"))?;
        let (header_host, header_port) =
            parse_authority(authority, Some(443)).map_err(|_| invalid_data("proxy TLS Host header is invalid"))?;
        if header_host != host || header_port != port {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "proxy TLS Host differs from CONNECT authority",
            ));
        }
    }
    output.extend_from_slice(b"Connection: close\r\n\r\n");
    let body = if transfer_encoding.is_some() {
        RequestBody::Chunked
    } else if let Some(length) = content_length {
        RequestBody::ContentLength(length)
    } else {
        RequestBody::None
    };
    Ok(PreparedRequest { header: output, body })
}

fn header_text(header: &[u8]) -> io::Result<&str> {
    if header.len() > MAX_PROXY_HEADER_BYTES || !header.ends_with(b"\r\n\r\n") {
        return Err(invalid_data("proxy HTTP header is invalid"));
    }
    let header = std::str::from_utf8(header).map_err(|_| invalid_data("proxy HTTP header is not UTF-8"))?;
    if header.contains('\0') {
        return Err(invalid_data("proxy HTTP header contains a null byte"));
    }
    Ok(header)
}

fn parse_header_line(line: &str) -> io::Result<(&str, &str)> {
    if line.starts_with([' ', '\t']) {
        return Err(invalid_data("proxy folded HTTP header is unsupported"));
    }
    let (name, value) = line
        .split_once(':')
        .ok_or_else(|| invalid_data("proxy HTTP header is invalid"))?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || value.bytes().any(|byte| byte < b' ' && byte != b'\t')
    {
        return Err(invalid_data("proxy HTTP header is invalid"));
    }
    Ok((name, value))
}

fn valid_origin_target(target: &str) -> bool {
    (target == "*" || (target.starts_with('/') && !target.starts_with("//")))
        && !target.contains('#')
        && !target.bytes().any(|byte| byte.is_ascii_control() || byte == b' ')
}

fn origin_target(target: &str) -> io::Result<&str> {
    let rest = target
        .strip_prefix("http://")
        .ok_or_else(|| invalid_data("proxy request is not plaintext HTTP"))?;
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let target = &rest[authority_end..];
    if target.contains('#') {
        return Err(invalid_data("proxy request target contains a fragment"));
    }
    if target.is_empty() {
        Ok("/")
    } else if target.starts_with('?') {
        Err(invalid_data("proxy request target without a path is unsupported"))
    } else {
        Ok(target)
    }
}

pub(super) fn read_header<R: Read>(reader: &mut R, buffered: &mut Vec<u8>) -> io::Result<Option<Vec<u8>>> {
    loop {
        if let Some(end) = buffered.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_bytes = end + 4;
            if header_bytes > MAX_PROXY_HEADER_BYTES {
                return Err(invalid_data("proxy HTTP header is too large"));
            }
            return Ok(Some(buffered.drain(..header_bytes).collect()));
        }
        if buffered.len() >= MAX_PROXY_HEADER_BYTES {
            return Err(invalid_data("proxy HTTP header is too large"));
        }
        let mut bytes = [0_u8; 4096];
        let read = reader.read(&mut bytes)?;
        if read == 0 {
            if buffered.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "proxy request closed"));
        }
        buffered.extend_from_slice(&bytes[..read]);
    }
}

pub(super) fn forward_request_body<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    buffered: &mut Vec<u8>,
    body: RequestBody,
    stopped: &AtomicBool,
) -> io::Result<()> {
    match body {
        RequestBody::None => Ok(()),
        RequestBody::ContentLength(length) => forward_exact(reader, writer, buffered, length, stopped),
        RequestBody::Chunked => forward_chunked(reader, writer, buffered, stopped),
    }
}

fn forward_exact<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    buffered: &mut Vec<u8>,
    mut remaining: u64,
    stopped: &AtomicBool,
) -> io::Result<()> {
    while remaining != 0 {
        check_running(stopped)?;
        if !buffered.is_empty() {
            let count = buffered.len().min(usize::try_from(remaining).unwrap_or(usize::MAX));
            writer.write_all(&buffered[..count])?;
            buffered.drain(..count);
            remaining -= count as u64;
            continue;
        }
        let mut bytes = [0_u8; 16 * 1024];
        let limit = bytes.len().min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let read = reader.read(&mut bytes[..limit])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy request body closed",
            ));
        }
        writer.write_all(&bytes[..read])?;
        remaining -= read as u64;
    }
    Ok(())
}

fn forward_chunked<R: Read, W: Write>(
    reader: &mut R,
    writer: &mut W,
    buffered: &mut Vec<u8>,
    stopped: &AtomicBool,
) -> io::Result<()> {
    let mut total = 0_u64;
    loop {
        check_running(stopped)?;
        let line = read_bounded_line(reader, buffered)?;
        let size = parse_chunk_size(&line)?;
        total = total
            .checked_add(size)
            .filter(|total| *total <= MAX_PROXY_BODY_BYTES)
            .ok_or_else(|| invalid_data("proxy chunked body exceeds the limit"))?;
        writer.write_all(&line)?;
        if size == 0 {
            forward_trailers(reader, writer, buffered)?;
            return Ok(());
        }
        forward_exact(reader, writer, buffered, size, stopped)?;
        let ending = take_exact(reader, buffered, 2)?;
        if ending != b"\r\n" {
            return Err(invalid_data("proxy chunk ending is invalid"));
        }
        writer.write_all(&ending)?;
    }
}

fn parse_chunk_size(line: &[u8]) -> io::Result<u64> {
    let line = line
        .strip_suffix(b"\r\n")
        .ok_or_else(|| invalid_data("proxy chunk line is invalid"))?;
    let size = line.split(|byte| *byte == b';').next().unwrap_or_default();
    if size.is_empty() || !size.iter().all(u8::is_ascii_hexdigit) {
        return Err(invalid_data("proxy chunk size is invalid"));
    }
    std::str::from_utf8(size)
        .ok()
        .and_then(|size| u64::from_str_radix(size, 16).ok())
        .ok_or_else(|| invalid_data("proxy chunk size is invalid"))
}

fn forward_trailers<R: Read, W: Write>(reader: &mut R, writer: &mut W, buffered: &mut Vec<u8>) -> io::Result<()> {
    let mut count = 0;
    loop {
        let line = read_bounded_line(reader, buffered)?;
        writer.write_all(&line)?;
        if line == b"\r\n" {
            return Ok(());
        }
        count += 1;
        let line =
            std::str::from_utf8(&line[..line.len() - 2]).map_err(|_| invalid_data("proxy chunk trailer is invalid"))?;
        let (name, _) = parse_header_line(line)?;
        if count > MAX_PROXY_HEADER_COUNT
            || matches!(
                name.to_ascii_lowercase().as_str(),
                "host"
                    | "content-length"
                    | "transfer-encoding"
                    | "connection"
                    | "proxy-connection"
                    | "proxy-authorization"
                    | "expect"
                    | "upgrade"
            )
        {
            return Err(invalid_data("proxy chunk trailer is invalid"));
        }
    }
}

fn read_bounded_line<R: Read>(reader: &mut R, buffered: &mut Vec<u8>) -> io::Result<Vec<u8>> {
    loop {
        if let Some(end) = buffered.windows(2).position(|window| window == b"\r\n") {
            return Ok(buffered.drain(..end + 2).collect());
        }
        if buffered.len() >= MAX_PROXY_HEADER_BYTES {
            return Err(invalid_data("proxy HTTP line is too large"));
        }
        let mut bytes = [0_u8; 4096];
        let read = reader.read(&mut bytes)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy request body closed",
            ));
        }
        buffered.extend_from_slice(&bytes[..read]);
    }
}

fn take_exact<R: Read>(reader: &mut R, buffered: &mut Vec<u8>, count: usize) -> io::Result<Vec<u8>> {
    while buffered.len() < count {
        let mut bytes = [0_u8; 4096];
        let read = reader.read(&mut bytes)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy request body closed",
            ));
        }
        buffered.extend_from_slice(&bytes[..read]);
    }
    Ok(buffered.drain(..count).collect())
}

fn check_running(stopped: &AtomicBool) -> io::Result<()> {
    if stopped.load(Ordering::Acquire) {
        Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"))
    } else {
        Ok(())
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
#[path = "host_http_test.rs"]
mod host_http_test;
