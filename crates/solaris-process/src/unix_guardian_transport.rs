use std::io;
use std::os::fd::RawFd;
use std::time::{Duration, Instant};

pub(super) const MAX_PACKET_BYTES: usize = 256 * 1024;
pub(super) const IO_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn set_nonblocking(descriptor: RawFd) -> io::Result<()> {
    // Darwin's send path can wait for buffer space despite MSG_DONTWAIT.
    // Set O_NONBLOCK before the first send so every I/O deadline stays bounded.
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    if flags == -1 || unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

// XNU does not implement AF_UNIX/SOCK_SEQPACKET. Keep the Linux packet
// transport, and use bounded, length-prefixed frames on macOS. Both processes
// compile this module so the framing and failure rules cannot diverge.
pub(super) struct PacketReader {
    stream: bool,
    header: [u8; 4],
    header_read: usize,
    payload: Vec<u8>,
    payload_read: usize,
    deadline: Option<Instant>,
}

impl PacketReader {
    pub(super) fn new() -> Self {
        Self {
            stream: cfg!(target_os = "macos"),
            header: [0; 4],
            header_read: 0,
            payload: Vec::new(),
            payload_read: 0,
            deadline: None,
        }
    }

    pub(super) fn pending_deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(super) fn recv_packet(&mut self, descriptor: RawFd, timeout: Duration) -> io::Result<Vec<u8>> {
        self.poll_packet(descriptor, timeout)?.ok_or_else(timed_out)
    }

    // An incomplete stream frame survives short polls. Its own deadline never
    // resets, so a stalled/dribbling peer cannot indefinitely delay cleanup.
    pub(super) fn poll_packet(&mut self, descriptor: RawFd, timeout: Duration) -> io::Result<Option<Vec<u8>>> {
        let deadline = Instant::now() + timeout;
        let operation_deadline = (!timeout.is_zero()).then_some(deadline);
        loop {
            match self.try_recv(descriptor, operation_deadline) {
                Ok(packet) => return Ok(Some(packet)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
            let wait_deadline = self
                .pending_deadline()
                .map_or(deadline, |pending| pending.min(deadline));
            if !wait_ready(descriptor, libc::POLLIN, wait_deadline)? {
                self.check_deadline()?;
                return Ok(None);
            }
            if Instant::now() >= deadline {
                self.check_deadline()?;
                return Ok(None);
            }
        }
    }

    fn check_deadline(&self) -> io::Result<()> {
        if self.deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            Err(timed_out())
        } else {
            Ok(())
        }
    }

    fn try_recv(&mut self, descriptor: RawFd, operation_deadline: Option<Instant>) -> io::Result<Vec<u8>> {
        if !self.stream {
            // The extra byte detects oversized records, including truncated
            // SOCK_SEQPACKET records, before a valid-looking prefix is used.
            // Retain this buffer across empty supervision polls.
            self.payload.resize(MAX_PACKET_BYTES + 1, 0);
            let read = recv_bytes(descriptor, &mut self.payload)?;
            if read == 0 {
                return Err(closed());
            }
            if read > MAX_PACKET_BYTES {
                return Err(invalid_frame());
            }
            return Ok(self.payload[..read].to_vec());
        }
        loop {
            self.check_deadline()?;
            if operation_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            if self.header_read == self.header.len() && self.payload_read == self.payload.len() {
                self.header_read = 0;
                self.payload_read = 0;
                self.deadline = None;
                return Ok(std::mem::take(&mut self.payload));
            }
            let read = if self.header_read < self.header.len() {
                recv_bytes(descriptor, &mut self.header[self.header_read..])?
            } else {
                recv_bytes(descriptor, &mut self.payload[self.payload_read..])?
            };
            if read == 0 {
                return Err(if self.header_read == 0 {
                    closed()
                } else {
                    io::Error::new(io::ErrorKind::UnexpectedEof, "guardian frame was truncated")
                });
            }
            self.deadline.get_or_insert_with(|| Instant::now() + IO_TIMEOUT);
            if self.header_read < self.header.len() {
                self.header_read += read;
                if self.header_read == self.header.len() {
                    let length = u32::from_be_bytes(self.header) as usize;
                    if length == 0 || length > MAX_PACKET_BYTES {
                        return Err(invalid_frame());
                    }
                    self.payload.resize(length, 0);
                }
            } else {
                self.payload_read += read;
            }
        }
    }
}

pub(super) fn send_packet(descriptor: RawFd, packet: &[u8]) -> io::Result<()> {
    send_packet_until(
        descriptor,
        packet,
        cfg!(target_os = "macos"),
        Instant::now() + IO_TIMEOUT,
    )
}

fn send_packet_until(descriptor: RawFd, packet: &[u8], stream: bool, deadline: Instant) -> io::Result<()> {
    if packet.is_empty() || packet.len() > MAX_PACKET_BYTES {
        return Err(invalid_frame());
    }
    let mut frame = Vec::new();
    let bytes = if stream {
        frame.extend_from_slice(&(packet.len() as u32).to_be_bytes());
        frame.extend_from_slice(packet);
        frame.as_slice()
    } else {
        packet
    };
    let mut written = 0;
    while written < bytes.len() {
        if Instant::now() >= deadline {
            return Err(timed_out());
        }
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MSG_DONTWAIT;
        // macOS sockets have SO_NOSIGPIPE set when the pair is created.
        let result = unsafe {
            libc::send(
                descriptor,
                bytes[written..].as_ptr().cast(),
                bytes.len() - written,
                flags,
            )
        };
        if result > 0 {
            written += result as usize;
            if !stream && written != bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "guardian packet write was incomplete",
                ));
            }
        } else if result == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "guardian control write made no progress",
            ));
        } else {
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => {}
                io::ErrorKind::WouldBlock => {
                    if !wait_ready(descriptor, libc::POLLOUT, deadline)? {
                        return Err(timed_out());
                    }
                }
                _ => return Err(error),
            }
        }
    }
    Ok(())
}

fn recv_bytes(descriptor: RawFd, bytes: &mut [u8]) -> io::Result<usize> {
    let read = unsafe { libc::recv(descriptor, bytes.as_mut_ptr().cast(), bytes.len(), libc::MSG_DONTWAIT) };
    if read >= 0 {
        Ok(read as usize)
    } else {
        let error = io::Error::last_os_error();
        // Return control to the deadline-aware polling loop after a signal.
        if error.kind() == io::ErrorKind::Interrupted {
            Err(io::ErrorKind::WouldBlock.into())
        } else {
            Err(error)
        }
    }
}

fn wait_ready(descriptor: RawFd, events: libc::c_short, deadline: Instant) -> io::Result<bool> {
    loop {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Ok(false);
        };
        // Round up: poll's millisecond resolution must not expire early.
        let millis = remaining
            .as_millis()
            .saturating_add(u128::from(remaining.subsec_nanos() % 1_000_000 != 0));
        let timeout = i32::try_from(millis).unwrap_or(i32::MAX);
        let mut poll = libc::pollfd {
            fd: descriptor,
            events,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut poll, 1, timeout) };
        if result > 0 {
            if poll.revents & libc::POLLNVAL != 0 {
                return Err(io::Error::from_raw_os_error(libc::EBADF));
            }
            // Read queued data even with POLLHUP, then observe EOF on the next
            // read. A complete frame immediately before EOF must not be lost.
            return Ok(true);
        }
        if result == -1 && io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return Err(io::Error::last_os_error());
        }
    }
}

fn timed_out() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "guardian control operation timed out")
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "guardian control channel closed")
}

fn invalid_frame() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid guardian frame length")
}

#[cfg(test)]
#[path = "unix_guardian_transport_test.rs"]
mod unix_guardian_transport_test;
