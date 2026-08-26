use std::net::IpAddr;

use super::McpError;

pub(super) fn redirect_safe_client(url: &str) -> Result<reqwest::Client, McpError> {
    let parsed = reqwest::Url::parse(url).map_err(|_| McpError::Transport("Invalid MCP URL".to_owned()))?;
    let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
    if is_loopback_url(&parsed) {
        builder = builder.no_proxy();
    }
    builder
        .build()
        .map_err(|error| McpError::Transport(format!("Failed to build HTTP client: {error}")))
}

fn is_loopback_url(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let domain = host.trim_start_matches('[').trim_end_matches(']').trim_end_matches('.');
    domain.eq_ignore_ascii_case("localhost")
        || domain.to_ascii_lowercase().ends_with(".localhost")
        || domain.parse::<IpAddr>().is_ok_and(|address| address.is_loopback())
}

#[cfg(test)]
#[path = "client_test.rs"]
mod client_test;
