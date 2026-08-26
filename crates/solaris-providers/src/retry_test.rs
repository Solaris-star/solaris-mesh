use super::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::error::ProviderError;

    #[tokio::test]
    async fn test_retry_succeeds_first_try() {
        let result = with_retry(2, || async { Ok::<_, ProviderError>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        // Pause tokio time so sleep calls return immediately
        tokio::time::pause();

        let counter = Arc::new(AtomicU32::new(0));
        let result = with_retry(2, || {
            let counter = Arc::clone(&counter);
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(ProviderError::Connection("timeout".into()))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        tokio::time::pause();

        let result = with_retry(2, || async {
            Err::<(), _>(ProviderError::Connection("always fails".into()))
        })
        .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ProviderError::Connection(_)));
    }

    #[tokio::test]
    async fn test_retry_non_retryable_error_fails_immediately() {
        let counter = Arc::new(AtomicU32::new(0));
        let result = with_retry(2, || {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ProviderError::Api {
                    status: 401,
                    message: "unauthorized".into(),
                })
            }
        })
        .await;

        // Non-retryable errors should fail immediately without retrying
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_initial_connect_retry_succeeds_after_connection_failures() {
        tokio::time::pause();

        let counter = Arc::new(AtomicU32::new(0));
        let result = with_initial_connect_retry(|| {
            let counter = Arc::clone(&counter);
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(ProviderError::Connection("connection refused".into()))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 2);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_initial_connect_retry_does_not_retry_rate_limit() {
        let counter = Arc::new(AtomicU32::new(0));
        let result = with_initial_connect_retry(|| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ProviderError::RateLimited {
                    retry_after_ms: 5000,
                    body: None,
                })
            }
        })
        .await;

        assert!(matches!(
            result.unwrap_err(),
            ProviderError::RateLimited {
                retry_after_ms: 5000,
                body: None,
            }
        ));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_initial_http_retry_succeeds_after_server_errors() {
        tokio::time::pause();

        let counter = Arc::new(AtomicU32::new(0));
        let result = with_initial_http_retry(|| {
            let counter = Arc::clone(&counter);
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(ProviderError::Api {
                        status: 503,
                        message: "busy".into(),
                    })
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 2);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_initial_http_retry_exhausts_after_five_server_retries() {
        tokio::time::pause();

        let counter = Arc::new(AtomicU32::new(0));
        let result = with_initial_http_retry(|| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ProviderError::Api {
                    status: 503,
                    message: "still busy".into(),
                })
            }
        })
        .await;

        assert!(matches!(result.unwrap_err(), ProviderError::Api { status: 503, .. }));
        assert_eq!(counter.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn test_initial_http_retry_does_not_retry_other_4xx() {
        let counter = Arc::new(AtomicU32::new(0));
        let result = with_initial_http_retry(|| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ProviderError::Api {
                    status: 401,
                    message: "unauthorized".into(),
                })
            }
        })
        .await;

        assert!(matches!(result.unwrap_err(), ProviderError::Api { status: 401, .. }));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_initial_http_retry_succeeds_after_rate_limit() {
        tokio::time::pause();

        let counter = Arc::new(AtomicU32::new(0));
        let result = with_initial_http_retry(|| {
            let counter = Arc::clone(&counter);
            async move {
                let attempt = counter.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(ProviderError::RateLimited {
                        retry_after_ms: 5000,
                        body: None,
                    })
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 2);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_initial_connect_retry_retries_send_failure_before_response() {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("test listener should bind");
        let address = listener.local_addr().expect("test listener should have an address");
        let server = tokio::spawn(async move {
            for _ in 0..=MAX_INITIAL_CONNECT_RETRIES {
                let (stream, _) = listener.accept().await.expect("test server should accept");
                drop(stream);
            }
        });
        let counter = Arc::new(AtomicU32::new(0));
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("test client should build");
        let url = format!("http://{address}/chat/completions");

        let result = with_initial_connect_retry(|| {
            let client = client.clone();
            let counter = Arc::clone(&counter);
            let url = url.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                client
                    .post(url)
                    .body("request")
                    .send()
                    .await
                    .map_err(ProviderError::Http)
            }
        })
        .await;

        let attempts = counter.load(Ordering::SeqCst);
        server.abort();
        assert!(
            matches!(result, Err(ProviderError::Http(_))),
            "unexpected result: {result:?}"
        );
        assert_eq!(attempts, MAX_INITIAL_CONNECT_RETRIES + 1);
    }

    #[tokio::test]
    async fn test_initial_http_retry_exhausts_after_two_rate_limit_retries() {
        tokio::time::pause();

        let counter = Arc::new(AtomicU32::new(0));
        let result = with_initial_http_retry(|| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ProviderError::RateLimited {
                    retry_after_ms: 5000,
                    body: None,
                })
            }
        })
        .await;

        assert!(matches!(result.unwrap_err(), ProviderError::RateLimited { .. }));
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    // --- backoff_sleep tests ---

    #[tokio::test]
    async fn test_backoff_sleep_doubles_duration() {
        tokio::time::pause();

        let next = backoff_sleep(1, Duration::from_secs(1)).await;
        assert_eq!(next, Duration::from_secs(2));

        let next = backoff_sleep(2, Duration::from_secs(4)).await;
        assert_eq!(next, Duration::from_secs(8));
    }

    #[tokio::test]
    async fn test_backoff_sleep_caps_at_max() {
        tokio::time::pause();

        // 10s * 2 = 20s, but MAX_BACKOFF is 15s
        let next = backoff_sleep(1, Duration::from_secs(10)).await;
        assert_eq!(next, Duration::from_secs(15));

        // Already at max
        let next = backoff_sleep(2, Duration::from_secs(15)).await;
        assert_eq!(next, Duration::from_secs(15));
    }
}
