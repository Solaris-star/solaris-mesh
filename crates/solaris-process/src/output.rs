use std::io::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::task::JoinHandle;

pub(crate) async fn finish_stdin_writer(writer: Option<JoinHandle<Result<()>>>) -> Result<()> {
    if let Some(writer) = writer {
        writer
            .await
            .map_err(|error| Error::other(format!("process stdin writer failed: {error}")))??;
    }
    Ok(())
}

pub(crate) fn read_stream<R>(
    mut reader: R,
    output: Arc<Mutex<Vec<u8>>>,
    stream_limit: usize,
    total_limit: usize,
    total_output: Arc<AtomicUsize>,
    limit_tx: tokio::sync::mpsc::UnboundedSender<()>,
) -> JoinHandle<Result<()>>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buffer = [0_u8; 8192];
        let mut stream_bytes = 0usize;
        loop {
            let read = reader.read(&mut buffer).await?;
            if read == 0 {
                return Ok(());
            }

            let total_before = total_output.fetch_add(read, Ordering::AcqRel);
            let allowed = read
                .min(stream_limit.saturating_sub(stream_bytes))
                .min(total_limit.saturating_sub(total_before));
            if allowed > 0 {
                output
                    .lock()
                    .map_err(|_| Error::other("process output buffer lock was poisoned"))?
                    .extend_from_slice(&buffer[..allowed]);
                stream_bytes = stream_bytes.saturating_add(allowed);
            }
            if allowed < read {
                let _ = limit_tx.send(());
                return Ok(());
            }
        }
    })
}

pub(crate) async fn drain_reader(reader: Option<JoinHandle<Result<()>>>, drain: Duration) {
    let _reader_result = drain_reader_with_result(reader, drain).await;
}

pub(crate) async fn drain_reader_with_result(reader: Option<JoinHandle<Result<()>>>, drain: Duration) -> Result<()> {
    if let Some(mut reader) = reader {
        tokio::select! {
            _ = tokio::time::sleep(drain) => {
                reader.abort();
                let _abort_join_result = reader.await;
                Ok(())
            }
            result = &mut reader => {
                result.map_err(|error| Error::other(format!("process output reader failed: {error}")))?
            }
        }
    } else {
        Ok(())
    }
}

pub(crate) fn take_output(output: Arc<Mutex<Vec<u8>>>) -> Vec<u8> {
    output.lock().unwrap_or_else(|error| error.into_inner()).clone()
}
