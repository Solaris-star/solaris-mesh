use std::io::{self, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use super::{IO_TIMEOUT, MAX_PACKET_BYTES, PacketReader, send_packet_until};

fn stream_reader() -> PacketReader {
    let mut reader = PacketReader::new();
    reader.stream = true;
    reader
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut bytes = (payload.len() as u32).to_be_bytes().to_vec();
    bytes.extend_from_slice(payload);
    bytes
}

fn small_send_buffer(socket: &UnixStream) {
    let size: libc::c_int = 1024;
    assert_eq!(
        unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as libc::socklen_t,
            )
        },
        0
    );
}

#[test]
fn stream_preserves_every_header_and_body_fragment_across_nonblocking_polls() {
    let payload = b"launch plan with arguments";
    let bytes = frame(payload);
    // Every possible split includes an incomplete header or body.
    for split in 1..bytes.len() {
        let (parent, mut peer) = UnixStream::pair().unwrap();
        let mut reader = stream_reader();
        peer.write_all(&bytes[..split]).unwrap();
        assert!(
            reader
                .poll_packet(parent.as_raw_fd(), Duration::ZERO)
                .unwrap()
                .is_none()
        );
        let deadline = reader.pending_deadline().unwrap();
        assert!(
            reader
                .poll_packet(parent.as_raw_fd(), Duration::ZERO)
                .unwrap()
                .is_none()
        );
        assert_eq!(reader.pending_deadline(), Some(deadline));
        peer.write_all(&bytes[split..]).unwrap();
        assert_eq!(reader.recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap(), payload);
        assert!(reader.pending_deadline().is_none());
    }
}

#[test]
fn coalesced_stream_frames_are_distinct_and_survive_peer_hangup() {
    let (parent, mut peer) = UnixStream::pair().unwrap();
    let mut reader = stream_reader();
    let mut bytes = frame(b"O");
    bytes.extend(frame(b"X"));
    bytes.extend(frame(b"D"));
    peer.write_all(&bytes).unwrap();
    drop(peer);

    for expected in [b"O", b"X", b"D"] {
        assert_eq!(reader.recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap(), expected);
    }
    assert_eq!(
        reader.recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn stream_rejects_empty_and_oversized_lengths_before_reading_or_allocating_a_body() {
    for length in [0, MAX_PACKET_BYTES as u32 + 1, u32::MAX] {
        let (parent, mut peer) = UnixStream::pair().unwrap();
        let mut reader = stream_reader();
        peer.write_all(&length.to_be_bytes()).unwrap();
        assert_eq!(
            reader.recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(reader.payload.is_empty());
    }
}

#[test]
fn stream_distinguishes_clean_eof_from_every_truncated_frame() {
    let bytes = frame(b"PLAN");
    for prefix in 0..bytes.len() {
        let (parent, mut peer) = UnixStream::pair().unwrap();
        let mut reader = stream_reader();
        peer.write_all(&bytes[..prefix]).unwrap();
        drop(peer);
        let expected = if prefix == 0 {
            io::ErrorKind::BrokenPipe
        } else {
            io::ErrorKind::UnexpectedEof
        };
        assert_eq!(
            reader.recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap_err().kind(),
            expected
        );
    }
}

#[test]
fn partial_frame_does_not_block_supervision_poll_or_extend_its_deadline() {
    let (parent, mut peer) = UnixStream::pair().unwrap();
    let mut reader = stream_reader();
    peer.write_all(&[0]).unwrap();
    assert!(
        reader
            .poll_packet(parent.as_raw_fd(), Duration::from_millis(10))
            .unwrap()
            .is_none()
    );
    let deadline = reader.pending_deadline().unwrap();
    peer.write_all(&[0]).unwrap();
    assert!(
        reader
            .poll_packet(parent.as_raw_fd(), Duration::ZERO)
            .unwrap()
            .is_none()
    );
    assert_eq!(reader.pending_deadline(), Some(deadline));
    reader.deadline = Some(Instant::now());
    assert_eq!(
        reader
            .poll_packet(parent.as_raw_fd(), Duration::ZERO)
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
}

#[test]
fn read_timeout_is_absolute_despite_continuing_body_fragments() {
    let (parent, mut peer) = UnixStream::pair().unwrap();
    let mut reader = stream_reader();
    peer.write_all(&100_u32.to_be_bytes()).unwrap();
    let writer = std::thread::spawn(move || {
        for _ in 0..100 {
            if peer.write_all(b"x").is_err() {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    let started = Instant::now();
    let error = reader
        .recv_packet(parent.as_raw_fd(), Duration::from_millis(50))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "each fragment must not renew the timeout"
    );
    drop(parent);
    writer.join().unwrap();
}

#[test]
fn expired_operation_deadline_preserves_readable_frame_for_next_poll() {
    let (parent, mut peer) = UnixStream::pair().unwrap();
    let mut reader = stream_reader();
    let bytes = frame(b"PLAN");
    peer.write_all(&bytes[..5]).unwrap();
    assert!(
        reader
            .poll_packet(parent.as_raw_fd(), Duration::ZERO)
            .unwrap()
            .is_none()
    );
    peer.write_all(&bytes[5..]).unwrap();
    assert_eq!(
        reader
            .try_recv(parent.as_raw_fd(), Some(Instant::now()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(reader.recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap(), b"PLAN");
}

#[cfg(target_os = "linux")]
#[test]
fn packet_transport_rejects_truncated_oversized_record_and_reuses_idle_buffer() {
    use std::os::fd::{FromRawFd, OwnedFd};

    let mut descriptors = [-1; 2];
    assert_eq!(
        unsafe {
            libc::socketpair(
                libc::AF_UNIX,
                libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
                0,
                descriptors.as_mut_ptr(),
            )
        },
        0
    );
    let parent = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
    let peer = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
    let size = (MAX_PACKET_BYTES * 2) as libc::c_int;
    assert_eq!(
        unsafe {
            libc::setsockopt(
                peer.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as libc::socklen_t,
            )
        },
        0
    );
    let mut reader = PacketReader::new();
    assert!(
        reader
            .poll_packet(parent.as_raw_fd(), Duration::ZERO)
            .unwrap()
            .is_none()
    );
    let buffer = reader.payload.as_ptr();
    assert!(
        reader
            .poll_packet(parent.as_raw_fd(), Duration::ZERO)
            .unwrap()
            .is_none()
    );
    assert_eq!(reader.payload.as_ptr(), buffer);

    let packet = vec![b'D'; MAX_PACKET_BYTES + 16];
    assert_eq!(
        unsafe {
            libc::send(
                peer.as_raw_fd(),
                packet.as_ptr().cast(),
                packet.len(),
                libc::MSG_NOSIGNAL,
            )
        },
        packet.len() as isize
    );
    assert_eq!(
        reader.recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn stream_writes_complete_maximum_frame_through_partial_writes() {
    let (parent, peer) = UnixStream::pair().unwrap();
    small_send_buffer(&peer);
    let payload = vec![0x5a; MAX_PACKET_BYTES];
    let expected = payload.clone();
    let writer = std::thread::spawn(move || {
        send_packet_until(peer.as_raw_fd(), &payload, true, Instant::now() + IO_TIMEOUT).unwrap();
    });
    let received = stream_reader().recv_packet(parent.as_raw_fd(), IO_TIMEOUT).unwrap();
    assert_eq!(received, expected);
    writer.join().unwrap();
}

#[test]
fn stalled_stream_writer_obeys_deadline_after_a_partial_frame() {
    let (parent, peer) = UnixStream::pair().unwrap();
    small_send_buffer(&peer);
    let started = Instant::now();
    let error = send_packet_until(
        peer.as_raw_fd(),
        &vec![0x5a; MAX_PACKET_BYTES],
        true,
        started + Duration::from_millis(30),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() < Duration::from_secs(1));
    // The incomplete message is never exposed as a valid packet.
    drop(peer);
    assert_eq!(
        stream_reader()
            .recv_packet(parent.as_raw_fd(), IO_TIMEOUT)
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn invalid_outgoing_frame_writes_nothing() {
    for payload in [Vec::new(), vec![0; MAX_PACKET_BYTES + 1]] {
        let (parent, peer) = UnixStream::pair().unwrap();
        let error = send_packet_until(peer.as_raw_fd(), &payload, true, Instant::now() + IO_TIMEOUT).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            stream_reader()
                .poll_packet(parent.as_raw_fd(), Duration::ZERO)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn empty_socket_read_has_a_bounded_timeout() {
    let (parent, _peer) = UnixStream::pair().unwrap();
    let started = Instant::now();
    let error = stream_reader()
        .recv_packet(parent.as_raw_fd(), Duration::from_millis(20))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(started.elapsed() >= Duration::from_millis(20));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
fn interrupted_poll_and_backpressured_write_keep_absolute_deadlines() {
    // Signal disposition is process-wide: run the signal probe in a dedicated
    // process so no concurrently running test can receive its SIGUSR1 handler.
    let directory = tempfile::tempdir().unwrap();
    let proof = directory.path().join("interrupted-io-verified");
    // libtest names omit the binary/crate prefix from module_path!().
    let module = module_path!().split_once("::").unwrap().1;
    let probe = format!("{module}::interrupted_io_child_probe");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &probe, "--nocapture"])
        .env("SOLARIS_GUARDIAN_TEST_EINTR", "1")
        .env("SOLARIS_GUARDIAN_TEST_EINTR_PROOF", &proof)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            assert_eq!(std::fs::read(&proof).unwrap(), b"verified");
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("interrupted guardian I/O exceeded the subprocess watchdog");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn interrupted_io_child_probe() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    if std::env::var_os("SOLARIS_GUARDIAN_TEST_EINTR").is_none() {
        return;
    }
    static SIGNALS: AtomicUsize = AtomicUsize::new(0);
    extern "C" fn count_signal(_: libc::c_int) {
        SIGNALS.fetch_add(1, Ordering::Relaxed);
    }
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = count_signal as *const () as usize;
    action.sa_flags = 0; // Deliberately omit SA_RESTART so poll returns EINTR.
    assert_eq!(unsafe { libc::sigemptyset(&mut action.sa_mask) }, 0);
    assert_eq!(
        unsafe { libc::sigaction(libc::SIGUSR1, &action, std::ptr::null_mut()) },
        0
    );

    let thread = unsafe { libc::pthread_self() } as usize;
    let stop = Arc::new(AtomicBool::new(false));
    let sender_stop = Arc::clone(&stop);
    let signals = std::thread::spawn(move || {
        // A fixed upper bound also lets a broken timeout implementation finish
        // and fail the test instead of leaving the subprocess stuck forever.
        for _ in 0..200 {
            if sender_stop.load(Ordering::Relaxed) {
                break;
            }
            assert_eq!(
                unsafe { libc::pthread_kill(thread as libc::pthread_t, libc::SIGUSR1) },
                0
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    });
    let (parent, peer) = UnixStream::pair().unwrap();
    small_send_buffer(&peer);
    let started = Instant::now();
    let before_read = SIGNALS.load(Ordering::Relaxed);
    let read_result = stream_reader().recv_packet(parent.as_raw_fd(), Duration::from_millis(40));
    let after_read = SIGNALS.load(Ordering::Relaxed);
    let write_result = send_packet_until(
        peer.as_raw_fd(),
        &vec![0x5a; MAX_PACKET_BYTES],
        true,
        Instant::now() + Duration::from_millis(40),
    );
    let after_write = SIGNALS.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    signals.join().unwrap();
    assert_eq!(read_result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert_eq!(write_result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    assert!(after_read > before_read, "read probe must actually receive signals");
    assert!(after_write > after_read, "write probe must actually receive signals");
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "signals must not renew either operation's timeout"
    );
    let proof = std::env::var_os("SOLARIS_GUARDIAN_TEST_EINTR_PROOF").unwrap();
    std::fs::write(proof, b"verified").unwrap();
}
