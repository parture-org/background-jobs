use std::future::Future;

use background_jobs_core::{JoinError, UnsendSpawner};

/// Provide a spawner for actix-based systems for Unsend Jobs
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ActixSpawner;

#[doc(hidden)]
pub struct ActixHandle<T>(actix_rt::task::JoinHandle<T>);

impl UnsendSpawner for ActixSpawner {
    type Handle<T> = ActixHandle<T> where T: Send;

    fn spawn<Fut>(future: Fut) -> Self::Handle<Fut::Output>
    where
        Fut: Future + 'static,
        Fut::Output: Send + 'static,
    {
        ActixHandle(actix_rt::spawn(future))
    }
}

impl<T> Unpin for ActixHandle<T> {}

impl<T> Future for ActixHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let res = std::task::ready!(std::pin::Pin::new(&mut self.0).poll(cx));

        std::task::Poll::Ready(res.map_err(|_| JoinError))
    }
}

impl<T> Drop for ActixHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
