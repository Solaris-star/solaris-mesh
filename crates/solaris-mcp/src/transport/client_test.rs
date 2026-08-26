use super::is_loopback_url;

#[test]
fn loopback_urls_bypass_environment_proxies() {
    for value in [
        "http://127.0.0.1:1234/mcp",
        "http://[::1]:1234/mcp",
        "http://localhost:1234/mcp",
        "http://tools.localhost.:1234/mcp",
    ] {
        let url = reqwest::Url::parse(value).unwrap();
        assert!(is_loopback_url(&url), "{value}");
    }
}

#[test]
fn non_loopback_urls_keep_the_configured_proxy_behavior() {
    for value in ["https://mcp.example.test/rpc", "http://192.0.2.10/mcp"] {
        let url = reqwest::Url::parse(value).unwrap();
        assert!(!is_loopback_url(&url), "{value}");
    }
}
