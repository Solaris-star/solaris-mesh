//! Windows process security-environment (PSEC) request path.
//!
//! The ABI follows Microsoft's processmodel.dll security-environment contract.
//! Export/query presence is only prerequisite evidence: strict Auto additionally
//! requires a real create/close round-trip and the helper attaches a fresh
//! environment to the suspended target with
//! `PROC_THREAD_ATTRIBUTE_SECURITY_ENVIRONMENT`.

use crate::windows_psec_runtime::SecurityEnvironmentApi;

pub(super) fn preflight_runtime() -> std::io::Result<()> {
    let api = SecurityEnvironmentApi::load()?;
    let _support_flags = api.query_support()?;
    Ok(())
}

pub(super) fn prepare_proxy_policy(proxy_url: &str, package_family_name: &str) -> std::io::Result<Vec<u8>> {
    let specification = super::psec_codec::encode_proxy_policy(proxy_url, package_family_name)
        .map_err(|error| std::io::Error::other(format!("encode process security environment: {error}")))?;
    let api = SecurityEnvironmentApi::load()?;
    let _support_flags = api.query_support()?;
    let environment = api.create(&specification)?;
    drop(environment);
    Ok(specification)
}

#[cfg(test)]
#[path = "psec_test.rs"]
mod psec_test;
