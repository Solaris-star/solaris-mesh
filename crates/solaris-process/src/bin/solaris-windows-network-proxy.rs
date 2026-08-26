#[cfg(windows)]
#[allow(dead_code)]
#[path = "../network_proxy.rs"]
mod network_proxy;
#[cfg(all(windows, test))]
pub(crate) use network_proxy::NetworkProxyPolicy;
#[cfg(windows)]
mod windows_network_proxy;

#[cfg(windows)]
fn main() {
    if windows_network_proxy::run().is_err() {
        eprintln!("solaris-windows-network-proxy: startup failed");
        std::process::exit(125);
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("solaris-windows-network-proxy: Windows package target required");
    std::process::exit(125);
}
