use super::{MAX_PROXY_HEADER_BYTES, MAX_PROXY_HEADER_COUNT, NetworkProxyPolicy, normalize_host};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProxyRequest {
    pub(crate) connect: bool,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) header_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProxyRequestError {
    Incomplete,
    TooLarge,
    Invalid,
    Denied,
}

impl ProxyRequest {
    pub(crate) fn parse(bytes: &[u8], policy: &NetworkProxyPolicy) -> Result<Self, ProxyRequestError> {
        if bytes.len() > MAX_PROXY_HEADER_BYTES {
            return Err(ProxyRequestError::TooLarge);
        }
        let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
            return Err(ProxyRequestError::Incomplete);
        };
        let header_bytes = header_end + 4;
        let header = std::str::from_utf8(&bytes[..header_bytes]).map_err(|_| ProxyRequestError::Invalid)?;
        if header.chars().any(|character| character == '\0') {
            return Err(ProxyRequestError::Invalid);
        }
        let mut lines = header[..header.len() - 4].split("\r\n");
        let request_line = lines.next().ok_or(ProxyRequestError::Invalid)?;
        let mut parts = request_line.split(' ');
        let method = parts.next().ok_or(ProxyRequestError::Invalid)?;
        let target = parts.next().ok_or(ProxyRequestError::Invalid)?;
        let version = parts.next().ok_or(ProxyRequestError::Invalid)?;
        if parts.next().is_some()
            || method.is_empty()
            || !method.bytes().all(|byte| byte.is_ascii_uppercase())
            || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        {
            return Err(ProxyRequestError::Invalid);
        }

        let headers = lines.collect::<Vec<_>>();
        if headers.len() > MAX_PROXY_HEADER_COUNT {
            return Err(ProxyRequestError::TooLarge);
        }
        let mut host_header = None;
        for line in headers {
            if line.starts_with([' ', '\t']) {
                return Err(ProxyRequestError::Invalid);
            }
            let (name, value) = line.split_once(':').ok_or(ProxyRequestError::Invalid)?;
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                || value.bytes().any(|byte| byte < b' ' && byte != b'\t')
            {
                return Err(ProxyRequestError::Invalid);
            }
            if name.eq_ignore_ascii_case("host") && host_header.replace(value.trim()).is_some() {
                return Err(ProxyRequestError::Invalid);
            }
        }

        let connect = method == "CONNECT";
        let (host, port) = if connect {
            if target.contains(['/', '?', '#', '@']) {
                return Err(ProxyRequestError::Invalid);
            }
            parse_authority(target, None)?
        } else {
            let (default_port, rest) = if let Some(rest) = target.strip_prefix("http://") {
                (80, rest)
            } else if let Some(rest) = target.strip_prefix("https://") {
                (443, rest)
            } else {
                return Err(ProxyRequestError::Invalid);
            };
            let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
            let authority = &rest[..authority_end];
            if authority.contains('@') {
                return Err(ProxyRequestError::Invalid);
            }
            parse_authority(authority, Some(default_port))?
        };
        if let Some(host_header) = host_header {
            let (header_host, header_port) = parse_authority(host_header, Some(port))?;
            if header_host != host || header_port != port {
                return Err(ProxyRequestError::Invalid);
            }
        } else if !connect && version == "HTTP/1.1" {
            return Err(ProxyRequestError::Invalid);
        }
        if !policy.permits(&host, port) {
            return Err(ProxyRequestError::Denied);
        }
        Ok(Self {
            connect,
            host,
            port,
            header_bytes,
        })
    }
}

pub(super) fn parse_authority(value: &str, default_port: Option<u16>) -> Result<(String, u16), ProxyRequestError> {
    if value.is_empty() || value.starts_with('[') || value.ends_with(']') {
        return Err(ProxyRequestError::Invalid);
    }
    let (host, port) = match value.rsplit_once(':') {
        Some((host, port)) if !host.contains(':') => {
            let port = port
                .parse::<u16>()
                .ok()
                .filter(|port| *port != 0)
                .ok_or(ProxyRequestError::Invalid)?;
            (host, port)
        }
        Some(_) => return Err(ProxyRequestError::Invalid),
        None => (value, default_port.ok_or(ProxyRequestError::Invalid)?),
    };
    normalize_host(host)
        .map(|host| (host, port))
        .map_err(|_| ProxyRequestError::Invalid)
}

#[cfg(test)]
#[path = "request_test.rs"]
mod request_test;
