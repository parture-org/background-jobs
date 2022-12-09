use std::{
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    task::{Context, Poll},
};

pub(crate) struct CatchUnwindFuture<F> {
    future: F,
}

pub(crate) fn catch_unwind<F>(future: F) -> CatchUnwindFuture<F>
where
    F: Future + Unpin,
{
    CatchUnwindFuture { future }
}

impl<F> Future for CatchUnwindFuture<F>
where
    F: Future + Unpin,
{
    type Output = std::thread::Result<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = &mut self.get_mut().future;
        let waker = cx.waker().clone();
        let res = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut context = Context::from_waker(&waker);
            Pin::new(future).poll(&mut context)
        }));

        match res {
            Ok(poll) => poll.map(Ok),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
