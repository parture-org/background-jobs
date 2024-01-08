use crate::{Backoff, Job, MaxRetries};
use anyhow::Error;
use serde::{de::DeserializeOwned, ser::Serialize};
use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::Span;
use tracing_futures::Instrument;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// The type produced when a task is dropped before completion as a result of being deliberately
/// canceled, or it panicking
pub struct JoinError;

impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Task has been canceled")
    }
}

impl std::error::Error for JoinError {}

/// The mechanism used to spawn Unsend futures, making them Send
pub trait UnsendSpawner {
    /// The Handle to the job, implements a Send future with the Job's output
    type Handle<T>: Future<Output = Result<T, JoinError>> + Send + Unpin
    where
        T: Send;

    /// Spawn the unsend future producing a Send handle
    fn spawn<Fut>(future: Fut) -> Self::Handle<Fut::Output>
    where
        Fut: Future + 'static,
        Fut::Output: Send + 'static;
}

/// The UnsendJob trait defines parameters pertaining to an instance of a background job
///
/// This trait is used to implement generic Unsend Jobs in the background jobs library. It requires
/// that implementors specify a spawning mechanism that can turn an Unsend future into a Send
/// future
pub trait UnsendJob: Serialize + DeserializeOwned + 'static {
    /// The application state provided to this job at runtime.
    type State: Clone + 'static;

    /// The future returned by this job
    ///
    /// Importantly, this Future does not require Send
    type Future: Future<Output = Result<(), Error>>;

    /// The spawner type that will be used to spawn the unsend future
    type Spawner: UnsendSpawner;

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

#[doc(hidden)]
pub struct UnwrapFuture<F>(F);

impl<F, T, E> Future for UnwrapFuture<F>
where
    F: Future<Output = Result<T, E>> + Unpin,
    E: Debug,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|res| res.unwrap())
    }
}

impl<T> Job for T
where
    T: UnsendJob,
{
    type State = T::State;
    type Future = UnwrapFuture<<T::Spawner as UnsendSpawner>::Handle<Result<(), Error>>>;

    const NAME: &'static str = <Self as UnsendJob>::NAME;
    const QUEUE: &'static str = <Self as UnsendJob>::QUEUE;
    const MAX_RETRIES: MaxRetries = <Self as UnsendJob>::MAX_RETRIES;
    const BACKOFF: Backoff = <Self as UnsendJob>::BACKOFF;
    const TIMEOUT: i64 = <Self as UnsendJob>::TIMEOUT;

    fn run(self, state: Self::State) -> Self::Future {
        UnwrapFuture(T::Spawner::spawn(
            UnsendJob::run(self, state).instrument(Span::current()),
        ))
    }

    fn span(&self) -> Option<Span> {
        UnsendJob::span(self)
    }

    fn queue(&self) -> &str {
        UnsendJob::queue(self)
    }

    fn max_retries(&self) -> MaxRetries {
        UnsendJob::max_retries(self)
    }

    fn backoff_strategy(&self) -> Backoff {
        UnsendJob::backoff_strategy(self)
    }

    fn timeout(&self) -> i64 {
        UnsendJob::timeout(self)
    }
}
