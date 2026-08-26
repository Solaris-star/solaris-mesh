use solaris_process::{SandboxEnforcement, platform_sandbox_report};

use crate::cli::SandboxAction;

pub(crate) fn run(action: SandboxAction) -> anyhow::Result<()> {
    match action {
        SandboxAction::VerifyPackage => verify_package(),
    }
}

fn verify_package() -> anyhow::Result<()> {
    let report = platform_sandbox_report();
    if report.enforcement() != SandboxEnforcement::Full {
        anyhow::bail!(
            "strict sandbox package verification failed: backend={:?}, enforcement={:?}, reason={:?}",
            report.backend(),
            report.enforcement(),
            report.reason()
        );
    }
    println!(
        "strict sandbox package verified: backend={:?}, enforcement={:?}",
        report.backend(),
        report.enforcement()
    );
    Ok(())
}
