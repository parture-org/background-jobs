use std::future::Future;

use anyhow::Error;
use background_jobs_core::{Backoff, JoinError, MaxRetries, UnsendJob, UnsendSpawner};
use serde::{de::DeserializeOwned, ser::Serialize};
use tracing::Span;

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

/// The UnsendJob trait defines parameters pertaining to an instance of a background job
///
/// This trait is used to implement generic Unsend Jobs in the background jobs library. It requires
/// that implementors specify a spawning mechanism that can turn an Unsend future into a Send
/// future
pub trait ActixJob: Serialize + DeserializeOwned + std::panic::UnwindSafe + 'static {
    /// The application state provided to this job at runtime.
    type State: Clone + 'static;

    /// The future returned by this job
    ///
    /// Importantly, this Future does not require Send
    type Future: Future<Output = Result<(), Error>>;

    /// The name of the job
    ///
    /// This name must be unique!!!
    const NAME: &'static str;

    /// The name of the default queue for this job
    ///
    /// This can be overridden on an individual-job level, but if a non-existant queue is supplied,
    /// the job will never be processed.
    const QUEUE: &'static str = "default";

    /// Define the default number of retries for this job
    ///
    /// Defaults to Count(5)
    /// Jobs can override
    const MAX_RETRIES: MaxRetries = MaxRetries::Count(5);

    /// Define the default backoff strategy for this job
    ///
    /// Defaults to Exponential(2)
    /// Jobs can override
    const BACKOFF: Backoff = Backoff::Exponential(2);

    /// Define the maximum number of milliseconds a job should be allowed to run before being
    /// considered dead.
    ///
    /// This is important for allowing the job server to reap processes that were started but never
    /// completed.
    ///
    /// Defaults to 15 seconds
    /// Jobs can override
    const TIMEOUT: i64 = 15_000;

    /// Users of this library must define what it means to run a job.
    ///
    /// This should contain all the logic needed to complete a job. If that means queuing more
    /// jobs, sending an email, shelling out (don't shell out), or doing otherwise lengthy
    /// processes, that logic should all be called from inside this method.
    ///
    /// The state passed into this job is initialized at the start of the application. The state
    /// argument could be useful for containing a hook into something like r2d2, or the address of
    /// an actor in an actix-based system.
    fn run(self, state: Self::State) -> Self::Future;

    /// Generate a Span that the job will be processed within
    fn span(&self) -> Option<Span> {
        None
    }

    /// If this job should not use it's default queue, this can be overridden in
    /// user-code.
    fn queue(&self) -> &str {
        Self::QUEUE
    }

    /// If this job should not use it's default maximum retry count, this can be
    /// overridden in user-code.
    fn max_retries(&self) -> MaxRetries {
        Self::MAX_RETRIES
    }

    /// If this job should not use it's default backoff strategy, this can be
    /// overridden in user-code.
    fn backoff_strategy(&self) -> Backoff {
        Self::BACKOFF
    }

    /// Define the maximum number of milliseconds this job should be allowed to run before being
    /// considered dead.
    ///
    /// This is important for allowing the job server to reap processes that were started but never
    /// completed.
    fn timeout(&self) -> i64 {
        Self::TIMEOUT
    }
}

/// Provide helper methods for queuing ActixJobs
pub trait ActixJobExt: ActixJob {
    /// Turn an ActixJob into a type that implements Job
    fn into_job(self) -> ActixJobWrapper<Self>
    where
        Self: Sized,
    {
        ActixJobWrapper(self)
    }
}

impl<T> ActixJobExt for T where T: ActixJob {}

impl<T> From<T> for ActixJobWrapper<T>
where
    T: ActixJob,
{
    fn from(value: T) -> Self {
        ActixJobWrapper(value)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
// A wrapper for ActixJob implementing UnsendJob with an ActixSpawner
pub struct ActixJobWrapper<T>(T);

impl<T> UnsendJob for ActixJobWrapper<T>
where
    T: ActixJob,
{
    type State = <T as ActixJob>::State;

    type Future = <T as ActixJob>::Future;

    type Spawner = ActixSpawner;

    const NAME: &'static str = <T as ActixJob>::NAME;
    const QUEUE: &'static str = <T as ActixJob>::QUEUE;
    const MAX_RETRIES: MaxRetries = <T as ActixJob>::MAX_RETRIES;
    const BACKOFF: Backoff = <T as ActixJob>::BACKOFF;
    const TIMEOUT: i64 = <T as ActixJob>::TIMEOUT;

    fn run(self, state: Self::State) -> Self::Future {
        <T as ActixJob>::run(self.0, state)
    }

    fn span(&self) -> Option<Span> {
        self.0.span()
    }

    fn queue(&self) -> &str {
        self.0.queue()
    }

    fn max_retries(&self) -> MaxRetries {
        self.0.max_retries()
    }

    fn backoff_strategy(&self) -> Backoff {
        self.0.backoff_strategy()
    }

    fn timeout(&self) -> i64 {
        self.0.timeout()
    }
}
