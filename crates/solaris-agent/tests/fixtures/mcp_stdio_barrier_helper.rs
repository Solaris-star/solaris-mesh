use std::env;
use std::io::{self, BufRead, Read, Write};
use std::net::TcpStream;

fn main() {
    let address = env::var("SOLARIS_TEST_MCP_BARRIER_ADDR").expect("barrier address is required");
    let slot = env::var("SOLARIS_TEST_MCP_BARRIER_SLOT").expect("barrier slot is required");
    let slot = slot
        .as_bytes()
        .first()
        .copied()
        .expect("barrier slot must not be empty");
    let mut barrier = TcpStream::connect(address).expect("barrier connection failed");
    barrier.write_all(&[slot]).expect("barrier ready write failed");
    let mut release = [0_u8; 1];
    barrier.read_exact(&mut release).expect("barrier release read failed");

    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line.expect("MCP request read failed");
        if line.contains(r#""method":"initialize""#) {
            writeln!(
                stdout,
                r#"{{"jsonrpc":"2.0","id":1,"result":{{"protocolVersion":"2025-03-26","capabilities":{{}},"serverInfo":null}}}}"#
            )
            .expect("initialize response write failed");
            stdout.flush().expect("initialize response flush failed");
        } else if line.contains(r#""method":"tools/list""#) {
            writeln!(stdout, r#"{{"jsonrpc":"2.0","id":2,"result":{{"tools":[]}}}}"#)
                .expect("tools response write failed");
            stdout.flush().expect("tools response flush failed");
            return;
        }
    }
}
