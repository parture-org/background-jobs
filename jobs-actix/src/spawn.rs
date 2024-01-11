use std::future::Future;

use tokio::task::JoinHandle;

#[cfg(tokio_unstable)]
pub(crate) fn spawn<F>(name: &str, future: F) -> std::io::Result<JoinHandle<F::Output>>
where
    F: Future + 'static,
{
    tokio::task::Builder::new().name(name).spawn_local(future)
}

#[cfg(not(tokio_unstable))]
pub(crate) fn spawn<F>(name: &str, future: F) -> std::io::Result<JoinHandle<F::Output>>
where
    F: Future + 'static,
{
    Ok(tokio::task::spawn_local(future))
}
