//! Small deterministic process used by plugin process tests.
//!
//! Keeping this helper separate from the large Rust test executable lets the
//! production executable inspection limit remain meaningful during tests.

use std::env;
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

const PROVIDER: &str = "--solaris-plugin-provider-test-helper";
const WAIT: &str = "--solaris-plugin-descendant-wait-test-helper";
const OUTPUT: &str = "--solaris-plugin-descendant-output-test-helper";
const SLEEP: &str = "--solaris-plugin-descendant-sleep-test-helper";

fn main() {
    let args = env::args().collect::<Vec<_>>();
    let Some(mode) = args.get(1).map(String::as_str) else {
        std::process::exit(125);
    };
    if mode == SLEEP {
        thread::sleep(Duration::from_secs(30));
        return;
    }

    let mut input = Vec::new();
    if io::stdin().read_to_end(&mut input).is_err() || input.is_empty() {
        std::process::exit(125);
    }
    if mode == PROVIDER {
        let mut stdout = io::stdout().lock();
        if stdout.write_all(br#"{"ok":true}"#).is_err() || stdout.flush().is_err() {
            std::process::exit(125);
        }
        return;
    }
    if mode != WAIT && mode != OUTPUT {
        std::process::exit(125);
    }
    let Some(marker) = args.get(2) else {
        std::process::exit(125);
    };
    let executable = env::current_exe().unwrap_or_else(|_| std::process::exit(125));
    let child = Command::new(executable)
        .arg(SLEEP)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|_| std::process::exit(125));
    std::fs::write(marker, child.id().to_string()).unwrap_or_else(|_| std::process::exit(125));
    if mode == WAIT {
        let _ = child.wait_with_output();
        return;
    }
    let stream = args.get(3).map(String::as_str).unwrap_or("stdout");
    if stream == "stderr" {
        let mut output = io::stderr().lock();
        loop {
            if output.write_all(b"0123456789").is_err() {
                return;
            }
        }
    }
    let mut output = io::stdout().lock();
    loop {
        if output.write_all(b"0123456789").is_err() {
            return;
        }
    }
}
