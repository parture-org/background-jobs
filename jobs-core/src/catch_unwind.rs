use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

pub(crate) struct CatchUnwindFuture<F> {
    future: Mutex<F>,
}

pub(crate) fn catch_unwind<F>(future: F) -> CatchUnwindFuture<F>
where
    F: Future + Unpin,
{
    CatchUnwindFuture {
        future: Mutex::new(future),
    }
}

impl<F> Future for CatchUnwindFuture<F>
where
    F: Future + Unpin,
{
    type Output = std::thread::Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = &self.future;
        let waker = cx.waker().clone();
        let res = std::panic::catch_unwind(|| {
            let mut context = Context::from_waker(&waker);
            let mut guard = future.lock().unwrap();
            Pin::new(&mut *guard).poll(&mut context)
        });

        match res {
            Ok(poll) => poll.map(Ok),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
