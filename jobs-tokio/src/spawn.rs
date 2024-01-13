use std::future::Future;

use tokio::task::JoinHandle;

#[cfg(tokio_unstable)]
pub(crate) fn spawn<F>(name: &str, future: F) -> std::io::Result<JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::task::Builder::new().name(name).spawn(future)
}

#[cfg(not(tokio_unstable))]
pub(crate) fn spawn<F>(name: &str, future: F) -> std::io::Result<JoinHandle<F::Output>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let _ = name;
    Ok(tokio::task::spawn(future))
}
