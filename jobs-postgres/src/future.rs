use std::{
    future::Future,
    time::{Duration, Instant},
};

pub(super) trait Timeout: Future {
    fn timeout(self, duration: Duration) -> tokio::time::Timeout<Self>
    where
        Self: Sized,
    {
        tokio::time::timeout(duration, self)
    }
}

pub(super) trait Metrics: Future {
    fn metrics(self, name: &'static str) -> MetricsFuture<Self>
    where
        Self: Sized,
    {
        MetricsFuture {
            future: self,
            metrics: MetricsGuard {
                name,
                start: Instant::now(),
                complete: false,
            },
        }
    }
}

impl<F> Metrics for F where F: Future {}
impl<F> Timeout for F where F: Future {}

pin_project_lite::pin_project! {
    pub(super) struct MetricsFuture<F> {
        #[pin]
        future: F,

        metrics: MetricsGuard,
    }
}

struct MetricsGuard {
    name: &'static str,
    start: Instant,
    complete: bool,
}

impl<F> Future for MetricsFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();

        let out = std::task::ready!(this.future.poll(cx));

        this.metrics.complete = true;

        std::task::Poll::Ready(out)
    }
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        metrics::histogram!(self.name, "complete" => self.complete.to_string())
            .record(self.start.elapsed().as_secs_f64());
    }
}
