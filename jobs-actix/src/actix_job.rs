use std::future::Future;

use background_jobs_core::{JoinError, UnsendSpawner};
use tokio::task::JoinHandle;

/// Provide a spawner for actix-based systems for Unsend Jobs
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActixSpawner;

#[doc(hidden)]
pub struct ActixHandle<T>(Option<JoinHandle<T>>);

impl UnsendSpawner for ActixSpawner {
    type Handle<T> = ActixHandle<T> where T: Send;

    fn spawn<Fut>(future: Fut) -> Self::Handle<Fut::Output>
    where
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        ActixHandle(crate::spawn::spawn("job-task", future).ok())
    }
}

impl<T> Unpin for ActixHandle<T> {}

impl<T> Future for ActixHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Some(mut handle) = self.0.as_mut() {
            let res = std::task::ready!(std::pin::Pin::new(&mut handle).poll(cx));
            std::task::Poll::Ready(res.map_err(|_| JoinError))
        } else {
            std::task::Poll::Ready(Err(JoinError))
        }
    }
}

impl<T> Drop for ActixHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}
