use super::*;

#[test]
fn current_host_psec_preflight_is_typed_and_never_panics() {
    match preflight_runtime() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("PSEC runtime preflight: {error}");
            assert!(error.raw_os_error().is_some() || !error.to_string().is_empty());
        }
    }
}
