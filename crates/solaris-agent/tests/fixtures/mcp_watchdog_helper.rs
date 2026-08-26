//! Deterministic helper for testing process-tree termination.

use std::env;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

const CONTROLLER: &str = "--solaris-mcp-watchdog-controller";
const DESCENDANT: &str = "--solaris-mcp-watchdog-descendant";

fn main() {
    let args = env::args().collect::<Vec<_>>();
    match args.get(1).map(String::as_str) {
        Some(DESCENDANT) => thread::sleep(Duration::from_secs(30)),
        Some(CONTROLLER) => {
            let Some(marker) = args.get(2) else {
                std::process::exit(125);
            };
            let executable = env::current_exe().unwrap_or_else(|_| std::process::exit(125));
            let descendant = Command::new(executable)
                .arg(DESCENDANT)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap_or_else(|_| std::process::exit(125));
            std::fs::write(marker, descendant.id().to_string()).unwrap_or_else(|_| std::process::exit(125));
            thread::sleep(Duration::from_secs(30));
        }
        _ => std::process::exit(125),
    }
}
