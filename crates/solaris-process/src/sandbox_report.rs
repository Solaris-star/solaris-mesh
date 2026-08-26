pub use solaris_types::sandbox::{SandboxBackend, SandboxEnforcement, SandboxReason, SandboxReport};

#[cfg(any(target_os = "linux", test))]
pub(crate) const fn linux_sandbox_capability_report(
    bwrap_available: bool,
    helper_available: bool,
    landlock_available: bool,
) -> SandboxReport {
    let (enforcement, backend, reason) = if !bwrap_available {
        (
            SandboxEnforcement::Unavailable,
            SandboxBackend::None,
            SandboxReason::BubblewrapUnavailable,
        )
    } else if !helper_available {
        (
            SandboxEnforcement::Unavailable,
            SandboxBackend::None,
            SandboxReason::HelperUnavailable,
        )
    } else if !landlock_available {
        (
            SandboxEnforcement::Unavailable,
            SandboxBackend::None,
            SandboxReason::LandlockUnavailable,
        )
    } else {
        (
            SandboxEnforcement::Full,
            SandboxBackend::LinuxBubblewrapLandlock,
            SandboxReason::Enforced,
        )
    };
    SandboxReport::new(enforcement, backend, reason)
}
