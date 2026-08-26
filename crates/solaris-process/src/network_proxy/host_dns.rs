use std::collections::BTreeSet;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream, UdpSocket};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::time::{Duration, Instant};

use super::super::is_public_destination;

const RESOLVER_CONFIG_PATH: &str = "/etc/resolv.conf";
const MAX_RESOLVER_CONFIG_BYTES: u64 = 64 * 1024;
const MAX_NAME_SERVERS: usize = 3;
const MAX_SEARCH_SUFFIXES: usize = 6;
const MAX_DNS_RECORDS: usize = 256;
const MAX_DNS_PACKET_BYTES: usize = u16::MAX as usize;
const MAX_DNS_UDP_PACKET_BYTES: usize = 4096;
const MAX_DNS_NAME_POINTERS: usize = 32;
const MAX_CNAME_DEPTH: usize = 8;
pub(super) const MAX_RESOLVED_ADDRESSES: usize = 16;
const DNS_POLL_INTERVAL: Duration = Duration::from_millis(50);
const DNS_SERVER_ATTEMPT: Duration = Duration::from_secs(1);
const DNS_TYPE_A: u16 = 1;
const DNS_TYPE_CNAME: u16 = 5;
const DNS_TYPE_AAAA: u16 = 28;
const DNS_CLASS_IN: u16 = 1;

static NEXT_TRANSACTION_ID: AtomicU16 = AtomicU16::new(1);

pub(super) fn resolve_public_addresses(
    host: &str,
    port: u16,
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<Vec<SocketAddr>> {
    let servers = read_resolver_config(Path::new(RESOLVER_CONFIG_PATH))?;
    resolve_with_servers(host, port, &servers, stopped, deadline)
}

fn resolve_with_servers(
    host: &str,
    port: u16,
    servers: &[SocketAddr],
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<Vec<SocketAddr>> {
    check_deadline(stopped, deadline, "proxy DNS resolution timed out")?;
    let absolute_name = format!("{host}.");
    let mut addresses = BTreeSet::new();
    for query_type in [DNS_TYPE_A, DNS_TYPE_AAAA] {
        for address in resolve_type(&absolute_name, query_type, servers, stopped, deadline)? {
            addresses.insert(address);
            if addresses.len() > MAX_RESOLVED_ADDRESSES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy DNS result exceeds the address limit",
                ));
            }
        }
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

fn resolve_type(
    absolute_name: &str,
    query_type: u16,
    servers: &[SocketAddr],
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<Vec<IpAddr>> {
    let mut current_name = canonical_dns_name(absolute_name)?;
    let mut visited = BTreeSet::new();
    for _ in 0..=MAX_CNAME_DEPTH {
        if !visited.insert(current_name.clone()) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "proxy DNS CNAME loop"));
        }
        let response = query_servers(&current_name, query_type, servers, stopped, deadline)?;
        let selection = select_response(&response, &current_name, query_type)?;
        match selection {
            ResponseSelection::Addresses(addresses) => return Ok(addresses),
            ResponseSelection::CanonicalName(name) => current_name = name,
            ResponseSelection::NoData => return Ok(Vec::new()),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "proxy DNS CNAME depth exceeds the limit",
    ))
}

fn query_servers(
    name: &str,
    query_type: u16,
    servers: &[SocketAddr],
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<DnsResponse> {
    let mut last_error = None;
    for server in servers {
        check_deadline(stopped, deadline, "proxy DNS resolution timed out")?;
        match query_server(name, query_type, *server, stopped, deadline) {
            Ok(response) => return Ok(response),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Err(error),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| io::Error::new(io::ErrorKind::NotFound, "proxy DNS server is unavailable")))
}

fn query_server(
    name: &str,
    query_type: u16,
    server: SocketAddr,
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<DnsResponse> {
    let transaction_id = NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed);
    let query = build_query(name, query_type, transaction_id)?;
    let attempt_deadline = deadline.min(Instant::now() + DNS_SERVER_ATTEMPT);
    let bind_address = match server {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    };
    let socket = UdpSocket::bind(bind_address)?;
    socket.connect(server)?;
    socket.set_write_timeout(Some(DNS_POLL_INTERVAL))?;
    socket.send(&query)?;
    let mut packet = vec![0_u8; MAX_DNS_UDP_PACKET_BYTES];
    loop {
        let timeout = io_timeout(stopped, deadline.min(attempt_deadline), "proxy DNS server timed out")?;
        socket.set_read_timeout(Some(timeout))?;
        match socket.recv(&mut packet) {
            Ok(received) => match parse_response(&packet[..received], transaction_id, name, query_type)? {
                ParsedResponse::Complete(response) => return Ok(response),
                ParsedResponse::Truncated => {
                    return query_server_tcp(
                        &query,
                        transaction_id,
                        name,
                        query_type,
                        server,
                        stopped,
                        deadline.min(attempt_deadline),
                    );
                }
            },
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn query_server_tcp(
    query: &[u8],
    transaction_id: u16,
    name: &str,
    query_type: u16,
    server: SocketAddr,
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<DnsResponse> {
    let mut stream = connect_with_deadline(server, stopped, deadline)?;
    let query_length = u16::try_from(query.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "proxy DNS query is too large"))?;
    write_all_with_deadline(&mut stream, &query_length.to_be_bytes(), stopped, deadline)?;
    write_all_with_deadline(&mut stream, query, stopped, deadline)?;
    let mut length = [0_u8; 2];
    read_exact_with_deadline(&mut stream, &mut length, stopped, deadline)?;
    let length = usize::from(u16::from_be_bytes(length));
    if !(12..=MAX_DNS_PACKET_BYTES).contains(&length) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS response size is invalid",
        ));
    }
    let mut packet = vec![0_u8; length];
    read_exact_with_deadline(&mut stream, &mut packet, stopped, deadline)?;
    match parse_response(&packet, transaction_id, name, query_type)? {
        ParsedResponse::Complete(response) => Ok(response),
        ParsedResponse::Truncated => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS TCP response is truncated",
        )),
    }
}

fn connect_with_deadline(server: SocketAddr, stopped: &AtomicBool, deadline: Instant) -> io::Result<TcpStream> {
    loop {
        let timeout = io_timeout(stopped, deadline, "proxy DNS TCP connection timed out")?;
        match TcpStream::connect_timeout(&server, timeout) {
            Ok(stream) => return Ok(stream),
            Err(error) if error.kind() == io::ErrorKind::TimedOut => {}
            Err(error) => return Err(error),
        }
    }
}

fn write_all_with_deadline(
    stream: &mut TcpStream,
    mut bytes: &[u8],
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_write_timeout(Some(io_timeout(stopped, deadline, "proxy DNS TCP write timed out")?))?;
        match stream.write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "proxy DNS TCP connection closed",
                ));
            }
            Ok(written) => bytes = &bytes[written..],
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn read_exact_with_deadline(
    stream: &mut TcpStream,
    mut bytes: &mut [u8],
    stopped: &AtomicBool,
    deadline: Instant,
) -> io::Result<()> {
    while !bytes.is_empty() {
        stream.set_read_timeout(Some(io_timeout(stopped, deadline, "proxy DNS TCP read timed out")?))?;
        match stream.read(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "proxy DNS TCP connection closed",
                ));
            }
            Ok(read) => bytes = &mut bytes[read..],
            Err(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn build_query(name: &str, query_type: u16, transaction_id: u16) -> io::Result<Vec<u8>> {
    let mut query = Vec::with_capacity(name.len() + 18);
    query.extend_from_slice(&transaction_id.to_be_bytes());
    query.extend_from_slice(&0x0100_u16.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    query.extend_from_slice(&0_u16.to_be_bytes());
    query.extend_from_slice(&0_u16.to_be_bytes());
    query.extend_from_slice(&0_u16.to_be_bytes());
    encode_name(name, &mut query)?;
    query.extend_from_slice(&query_type.to_be_bytes());
    query.extend_from_slice(&DNS_CLASS_IN.to_be_bytes());
    Ok(query)
}

fn encode_name(name: &str, output: &mut Vec<u8>) -> io::Result<()> {
    let name = canonical_dns_name(name)?;
    for label in name.split('.') {
        output.push(u8::try_from(label.len()).map_err(|_| invalid_dns_name())?);
        output.extend_from_slice(label.as_bytes());
    }
    output.push(0);
    Ok(())
}

fn canonical_dns_name(name: &str) -> io::Result<String> {
    let name = name.strip_suffix('.').unwrap_or(name).to_ascii_lowercase();
    if name.is_empty() || name.len() > 253 {
        return Err(invalid_dns_name());
    }
    for label in name.split('.') {
        if label.is_empty()
            || label.len() > 63
            || !label.as_bytes()[0].is_ascii_alphanumeric()
            || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
            || !label.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(invalid_dns_name());
        }
    }
    Ok(name)
}

fn invalid_dns_name() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "proxy DNS name is invalid")
}

#[derive(Debug)]
enum ParsedResponse {
    Complete(DnsResponse),
    Truncated,
}

#[derive(Debug, Default)]
struct DnsResponse {
    records: Vec<DnsRecord>,
    name_error: bool,
}

#[derive(Debug)]
enum DnsRecord {
    Address { owner: String, address: IpAddr },
    CanonicalName { owner: String, target: String },
}

fn parse_response(
    packet: &[u8],
    transaction_id: u16,
    question_name: &str,
    question_type: u16,
) -> io::Result<ParsedResponse> {
    if packet.len() < 12 || packet.len() > MAX_DNS_PACKET_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS response size is invalid",
        ));
    }
    let response_id = read_u16(packet, 0)?;
    let flags = read_u16(packet, 2)?;
    let question_count = usize::from(read_u16(packet, 4)?);
    let answer_count = usize::from(read_u16(packet, 6)?);
    let authority_count = usize::from(read_u16(packet, 8)?);
    let additional_count = usize::from(read_u16(packet, 10)?);
    let record_count = question_count
        .checked_add(answer_count)
        .and_then(|count| count.checked_add(authority_count))
        .and_then(|count| count.checked_add(additional_count))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS record count is invalid"))?;
    if response_id != transaction_id
        || flags & 0x8000 == 0
        || flags & 0x7800 != 0
        || question_count != 1
        || record_count > MAX_DNS_RECORDS
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS response header is invalid",
        ));
    }
    let response_code = flags & 0x000f;
    if !matches!(response_code, 0 | 3) {
        return Err(io::Error::other("proxy DNS server returned an error"));
    }
    let (name, consumed) = parse_name(packet, 12)?;
    let question_offset = 12 + consumed;
    if name != canonical_dns_name(question_name)?
        || read_u16(packet, question_offset)? != question_type
        || read_u16(packet, question_offset + 2)? != DNS_CLASS_IN
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS response question is invalid",
        ));
    }
    if flags & 0x0200 != 0 {
        return Ok(ParsedResponse::Truncated);
    }

    let mut offset = question_offset + 4;
    let mut response = DnsResponse {
        records: Vec::new(),
        name_error: response_code == 3,
    };
    for index in 0..answer_count + authority_count + additional_count {
        let (owner, consumed) = parse_name(packet, offset)?;
        offset += consumed;
        let record_type = read_u16(packet, offset)?;
        let record_class = read_u16(packet, offset + 2)?;
        let data_length = usize::from(read_u16(packet, offset + 8)?);
        offset = offset
            .checked_add(10)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS record is invalid"))?;
        let data_end = offset
            .checked_add(data_length)
            .filter(|end| *end <= packet.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS record is truncated"))?;
        if index < answer_count && record_class == DNS_CLASS_IN {
            match (record_type, data_length) {
                (DNS_TYPE_A, 4) => response.records.push(DnsRecord::Address {
                    owner,
                    address: IpAddr::V4(Ipv4Addr::new(
                        packet[offset],
                        packet[offset + 1],
                        packet[offset + 2],
                        packet[offset + 3],
                    )),
                }),
                (DNS_TYPE_AAAA, 16) => {
                    let octets = <[u8; 16]>::try_from(&packet[offset..data_end])
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS IPv6 record is invalid"))?;
                    response.records.push(DnsRecord::Address {
                        owner,
                        address: IpAddr::V6(Ipv6Addr::from(octets)),
                    });
                }
                (DNS_TYPE_CNAME, _) => {
                    let (target, consumed) = parse_name(packet, offset)?;
                    if consumed != data_length {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "proxy DNS CNAME record is invalid",
                        ));
                    }
                    response.records.push(DnsRecord::CanonicalName { owner, target });
                }
                _ => {}
            }
        }
        offset = data_end;
    }
    if offset != packet.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS response has trailing data",
        ));
    }
    Ok(ParsedResponse::Complete(response))
}

enum ResponseSelection {
    Addresses(Vec<IpAddr>),
    CanonicalName(String),
    NoData,
}

fn select_response(response: &DnsResponse, name: &str, query_type: u16) -> io::Result<ResponseSelection> {
    if response.name_error {
        return Ok(ResponseSelection::NoData);
    }
    let name = canonical_dns_name(name)?;
    let addresses = response
        .records
        .iter()
        .filter_map(|record| match record {
            DnsRecord::Address { owner, address }
                if owner == &name
                    && matches!(
                        (query_type, address),
                        (DNS_TYPE_A, IpAddr::V4(_)) | (DNS_TYPE_AAAA, IpAddr::V6(_))
                    ) =>
            {
                Some(*address)
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    if addresses.len() > MAX_RESOLVED_ADDRESSES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS result exceeds the address limit",
        ));
    }
    if !addresses.is_empty() {
        return Ok(ResponseSelection::Addresses(addresses.into_iter().collect()));
    }
    let targets = response
        .records
        .iter()
        .filter_map(|record| match record {
            DnsRecord::CanonicalName { owner, target } if owner == &name => Some(target.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    match targets.len() {
        0 => Ok(ResponseSelection::NoData),
        1 => targets
            .into_iter()
            .next()
            .map(ResponseSelection::CanonicalName)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS CNAME record is missing")),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy DNS response has conflicting CNAME records",
        )),
    }
}

fn parse_name(packet: &[u8], offset: usize) -> io::Result<(String, usize)> {
    let mut labels = Vec::new();
    let mut position = offset;
    let mut consumed = None;
    let mut pointers = BTreeSet::new();
    loop {
        let length = *packet
            .get(position)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS name is truncated"))?;
        if length & 0xc0 == 0xc0 {
            let next = *packet
                .get(position + 1)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS pointer is truncated"))?;
            let pointer = usize::from(length & 0x3f) << 8 | usize::from(next);
            if pointer >= packet.len() || !pointers.insert(pointer) || pointers.len() > MAX_DNS_NAME_POINTERS {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy DNS pointer is invalid",
                ));
            }
            consumed.get_or_insert(position + 2 - offset);
            position = pointer;
            continue;
        }
        if length & 0xc0 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy DNS label type is invalid",
            ));
        }
        position += 1;
        if length == 0 {
            let consumed = consumed.unwrap_or_else(|| position - offset);
            let name = labels.join(".");
            return canonical_dns_name(&name).map(|name| (name, consumed));
        }
        let end = position
            .checked_add(usize::from(length))
            .filter(|end| *end <= packet.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS label is truncated"))?;
        let label = std::str::from_utf8(&packet[position..end])
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS label is invalid"))?;
        labels.push(label.to_ascii_lowercase());
        position = end;
        if labels.iter().map(String::len).sum::<usize>() + labels.len().saturating_sub(1) > 253 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "proxy DNS name is too long"));
        }
    }
}

fn read_u16(packet: &[u8], offset: usize) -> io::Result<u16> {
    let bytes = packet
        .get(offset..offset + 2)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy DNS response is truncated"))?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_resolver_config(path: &Path) -> io::Result<Vec<SocketAddr>> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_RESOLVER_CONFIG_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "system DNS configuration is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    Read::by_ref(&mut file)
        .take(MAX_RESOLVER_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RESOLVER_CONFIG_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "system DNS configuration is too large",
        ));
    }
    let contents = std::str::from_utf8(&bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "system DNS configuration is not UTF-8"))?;
    parse_resolver_config(contents)
}

fn parse_resolver_config(contents: &str) -> io::Result<Vec<SocketAddr>> {
    let mut servers = Vec::new();
    let mut search_suffixes = 0_usize;
    for line in contents.lines() {
        let comment = line.find(['#', ';']).unwrap_or(line.len());
        let mut fields = line[..comment].split_whitespace();
        match fields.next() {
            Some("nameserver") => {
                let address = fields
                    .next()
                    .filter(|_| fields.next().is_none())
                    .and_then(|address| address.parse::<IpAddr>().ok())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "system DNS nameserver is invalid"))?;
                if !servers.contains(&SocketAddr::new(address, 53)) {
                    servers.push(SocketAddr::new(address, 53));
                    if servers.len() > MAX_NAME_SERVERS {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "system DNS nameserver count exceeds the limit",
                        ));
                    }
                }
            }
            Some("search") => {
                search_suffixes = search_suffixes.saturating_add(fields.count());
            }
            Some("domain") => {
                search_suffixes = search_suffixes.saturating_add(usize::from(fields.next().is_some()));
                if fields.next().is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "system DNS domain is invalid",
                    ));
                }
            }
            _ => {}
        }
        if search_suffixes > MAX_SEARCH_SUFFIXES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "system DNS search suffix count exceeds the limit",
            ));
        }
    }
    if servers.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "system DNS nameserver is unavailable",
        ));
    }
    Ok(servers)
}

fn io_timeout(stopped: &AtomicBool, deadline: Instant, message: &'static str) -> io::Result<Duration> {
    check_deadline(stopped, deadline, message).map(|remaining| remaining.min(DNS_POLL_INTERVAL))
}

fn check_deadline(stopped: &AtomicBool, deadline: Instant, message: &'static str) -> io::Result<Duration> {
    if stopped.load(Ordering::Acquire) {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "proxy is stopping"));
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(io::Error::new(io::ErrorKind::TimedOut, message));
    }
    Ok(remaining)
}

#[cfg(test)]
#[path = "host_dns_test.rs"]
mod host_dns_test;
