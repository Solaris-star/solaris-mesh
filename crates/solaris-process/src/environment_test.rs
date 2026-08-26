#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::process::Stdio;

    #[cfg(unix)]
    use super::super::append_network_proxy_ca_environment;
    #[cfg(windows)]
    use super::super::safe_process_environment;
    use super::super::{filtered_environment, filtered_override_environment, is_network_proxy_environment_key};
    use crate::{inspect_executable, pin_executable};

    const CHILD_MODE: &str = "SOLARIS_PROCESS_SAFE_ENV_CHILD";
    const PRIVATE_TEMP_CHILD_MODE: &str = "SOLARIS_PROCESS_PRIVATE_TEMP_CHILD";
    #[cfg(windows)]
    const POLLUTED_TEMP_CHILD_MODE: &str = "SOLARIS_PROCESS_POLLUTED_TEMP_CHILD";
    #[cfg(windows)]
    const ABSOLUTE_TEMP_CHILD_MODE: &str = "SOLARIS_PROCESS_ABSOLUTE_TEMP_CHILD";
    const EXPLICIT_SAFE_KEY: &str = "SOLARIS_MAX_ACTIVE_AGENTS";
    const TEST_NAME: &str =
        "environment::environment_test::tests::safe_environment_launches_child_with_explicit_safe_variable";
    const COMMON_SECRET_KEYS: &[&str] = &[
        "API_KEY",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "NPM_TOKEN",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "PASSWORD",
        "TOKEN",
    ];

    #[test]
    fn safe_environment_omits_common_secret_and_arbitrary_variables() {
        let mut input = vec![("PATH", "safe-path"), ("CUSTOM_VALUE", "arbitrary")];
        input.extend(COMMON_SECRET_KEYS.iter().map(|key| (*key, "secret")));

        let environment = filtered_environment(input);

        assert_eq!(environment.get(OsStr::new("PATH")), Some(&"safe-path".into()));
        assert!(!environment.contains_key(OsStr::new("CUSTOM_VALUE")));
        for key in COMMON_SECRET_KEYS {
            assert!(
                !environment.contains_key(OsStr::new(key)),
                "unexpected secret key: {key}"
            );
        }
    }

    #[test]
    fn proxy_environment_keys_are_reserved_case_insensitively() {
        for key in [
            "HTTP_PROXY",
            "https_proxy",
            "All_Proxy",
            "NO_PROXY",
            "no_proxy",
            "SSL_CERT_FILE",
            "requests_ca_bundle",
            "Node_Extra_Ca_Certs",
        ] {
            assert!(is_network_proxy_environment_key(OsStr::new(key)), "{key}");
        }
        assert!(!is_network_proxy_environment_key(OsStr::new("PROXY_TOKEN")));
    }

    #[test]
    #[cfg(unix)]
    fn proxy_ca_environment_uses_only_the_supplied_sandbox_path() {
        let mut environment = Vec::new();
        let path = std::path::Path::new("/__solaris/state/network-proxy-ca.pem");

        append_network_proxy_ca_environment(&mut environment, path);

        assert!(!environment.is_empty());
        assert!(environment.iter().all(|(_, value)| value == path.as_os_str()));
        assert!(environment.iter().any(|(key, _)| key == "SSL_CERT_FILE"));
        assert!(environment.iter().any(|(key, _)| key == "NODE_EXTRA_CA_CERTS"));
    }

    #[test]
    fn resource_environment_rejects_arbitrary_and_non_numeric_values() {
        let environment = super::super::filter_resource_environment([
            ("MCP_API_KEY".to_owned(), "secret".to_owned()),
            ("SOLARIS_MAX_RUN_TOKENS".to_owned(), "32000".to_owned()),
            ("SOLARIS_MAX_RUN_COST".to_owned(), "secret-in-safe-key".to_owned()),
        ]);

        assert_eq!(
            environment,
            vec![("SOLARIS_MAX_RUN_TOKENS".to_owned(), "32000".to_owned())]
        );
    }

    #[test]
    fn explicit_private_temp_override_requires_a_concrete_absolute_path() {
        let private_temp = tempfile::tempdir().unwrap();
        let accepted =
            filtered_override_environment([(OsString::from("TMPDIR"), private_temp.path().as_os_str().to_owned())]);
        let rejected = filtered_override_environment([
            (OsString::from("TEMP"), OsString::from("relative-temp")),
            (OsString::from("TMP"), OsString::from(r"%SystemDrive%\private-temp")),
        ]);

        assert_eq!(
            accepted.get(OsStr::new("TMPDIR")),
            Some(&private_temp.path().as_os_str().to_owned())
        );
        assert!(!rejected.contains_key(OsStr::new("TEMP")));
        assert!(!rejected.contains_key(OsStr::new("TMP")));
    }

    #[cfg(windows)]
    #[test]
    fn windows_safe_environment_contains_concrete_system_and_temp_paths() {
        let environment = safe_process_environment();
        let system_drive = environment.get(OsStr::new("SYSTEMDRIVE")).unwrap();

        assert_eq!(system_drive.to_string_lossy().len(), 2);
        assert!(system_drive.to_string_lossy().ends_with(':'));
        for key in ["SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP"] {
            let value = environment.get(OsStr::new(key)).unwrap();
            assert!(std::path::Path::new(value).is_absolute(), "{key} must be absolute");
            assert!(!value.to_string_lossy().contains('%'), "{key} must be expanded");
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_safe_temp_is_really_writable_by_the_current_user() {
        let environment = safe_process_environment();
        let trusted_temp = environment.get(OsStr::new("TEMP")).unwrap();
        for key in ["TMP", "TMPDIR"] {
            assert_eq!(environment.get(OsStr::new(key)), Some(trusted_temp));
        }

        let mut probe = tempfile::Builder::new()
            .prefix("solaris-safe-temp-probe-")
            .tempfile_in(trusted_temp)
            .expect("safe process temp must allow a current-user file");
        std::io::Write::write_all(&mut probe, b"writable").unwrap();
        assert_eq!(probe.as_file().metadata().unwrap().len(), 8);
    }

    #[cfg(windows)]
    #[test]
    fn windows_safe_environment_uses_trusted_temp_fallback_when_parent_is_polluted() {
        if std::env::var_os(POLLUTED_TEMP_CHILD_MODE).is_some() {
            let environment = safe_process_environment();
            for key in ["TEMP", "TMP", "TMPDIR"] {
                let value = environment.get(OsStr::new(key)).unwrap();
                assert!(std::path::Path::new(value).is_absolute(), "{key} must be absolute");
                assert!(!value.to_string_lossy().contains('%'), "{key} must be expanded");
            }
            return;
        }

        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "environment::environment_test::tests::windows_safe_environment_uses_trusted_temp_fallback_when_parent_is_polluted",
                "--nocapture",
            ])
            .env(POLLUTED_TEMP_CHILD_MODE, "1")
            .env("TEMP", r"%SystemDrive%\polluted-temp")
            .env("TMP", r"%SystemDrive%\polluted-tmp")
            .env("TMPDIR", r"%SystemDrive%\polluted-tmpdir")
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "polluted child did not receive safe temp paths"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_safe_environment_ignores_absolute_parent_temp_paths() {
        if std::env::var_os(ABSOLUTE_TEMP_CHILD_MODE).is_some() {
            let polluted = std::env::var_os("TEMP").unwrap();
            let environment = safe_process_environment();
            for key in ["TEMP", "TMP", "TMPDIR"] {
                let value = environment.get(OsStr::new(key)).unwrap();
                assert_ne!(value, &polluted, "{key} inherited the parent temp path");
                assert!(std::path::Path::new(value).is_absolute(), "{key} must be absolute");
                assert!(!value.to_string_lossy().contains('%'), "{key} must be expanded");
            }
            return;
        }

        let polluted = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "environment::environment_test::tests::windows_safe_environment_ignores_absolute_parent_temp_paths",
                "--nocapture",
            ])
            .env(ABSOLUTE_TEMP_CHILD_MODE, "1")
            .env("TEMP", polluted.path())
            .env("TMP", polluted.path())
            .env("TMPDIR", polluted.path())
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "absolute-pollution child inherited its parent temp paths"
        );
    }

    #[tokio::test]
    async fn safe_environment_launches_child_with_explicit_safe_variable() {
        if std::env::var_os(CHILD_MODE).is_some() {
            assert_eq!(std::env::var(EXPLICIT_SAFE_KEY).as_deref(), Ok("4"));
            for key in COMMON_SECRET_KEYS {
                assert!(std::env::var_os(key).is_none(), "unexpected secret key: {key}");
            }
            return;
        }

        let executable = std::env::current_exe().unwrap();
        let identity = inspect_executable(&executable).unwrap();
        let mut command = pin_executable(&executable, &identity).unwrap().command().unwrap();
        command
            .args(["--exact", TEST_NAME, "--nocapture"])
            .reset_to_safe_environment_with_overrides([(EXPLICIT_SAFE_KEY, "4")])
            .env(CHILD_MODE, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let status = command.spawn().unwrap().wait().await.unwrap();

        assert!(status.success());
    }

    #[tokio::test]
    async fn explicit_private_temp_override_reaches_only_the_owned_child() {
        if std::env::var_os(PRIVATE_TEMP_CHILD_MODE).is_some() {
            let temp = std::env::var_os("TMPDIR").unwrap();
            assert!(std::path::Path::new(&temp).join("owned-temp-marker").is_file());
            return;
        }

        let private_temp = tempfile::tempdir().unwrap();
        std::fs::write(private_temp.path().join("owned-temp-marker"), b"owned").unwrap();
        let executable = std::env::current_exe().unwrap();
        let identity = inspect_executable(&executable).unwrap();
        let mut command = pin_executable(&executable, &identity).unwrap().command().unwrap();
        command
            .args([
                "--exact",
                "environment::environment_test::tests::explicit_private_temp_override_reaches_only_the_owned_child",
                "--nocapture",
            ])
            .reset_to_safe_environment_with_overrides([
                ("TEMP", private_temp.path()),
                ("TMP", private_temp.path()),
                ("TMPDIR", private_temp.path()),
            ])
            .env(PRIVATE_TEMP_CHILD_MODE, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let status = command.spawn().unwrap().wait().await.unwrap();

        assert!(status.success());
    }
}
