use sha2::{Digest, Sha256};

pub(crate) fn log_stop_hook_output(output: &str) {
    let mut hasher = Sha256::new();
    hasher.update(b"solaris.stop-hook-output/v1\0");
    hasher.update(output.as_bytes());
    tracing::info!(
        target: "solaris_agent",
        hook_output_status = "reported",
        hook_output_bytes = output.len(),
        hook_output_digest = %format!("sha256:{:x}", hasher.finalize()),
        "stop hook output"
    );
}
