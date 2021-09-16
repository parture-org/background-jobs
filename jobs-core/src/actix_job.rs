use crate::{Backoff, Job, MaxRetries};
use anyhow::Error;
use serde::{de::DeserializeOwned, ser::Serialize};
use std::{future::Future, pin::Pin};
use tokio::sync::oneshot;
use tracing::{error, Span};
use tracing_futures::Instrument;

/// The ActixJob trait defines parameters pertaining to an instance of background job
///
/// This trait is specific to Actix, and will automatically implement the Job trait with the
/// proper translation from ?Send futures to Send futures
pub trait ActixJob: Serialize + DeserializeOwned + 'static {
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

impl<T> Job for T
where
    T: ActixJob + std::panic::UnwindSafe,
{
    type State = T::State;
    type Future = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

    const NAME: &'static str = <Self as ActixJob>::NAME;
    const QUEUE: &'static str = <Self as ActixJob>::QUEUE;
    const MAX_RETRIES: MaxRetries = <Self as ActixJob>::MAX_RETRIES;
    const BACKOFF: Backoff = <Self as ActixJob>::BACKOFF;
    const TIMEOUT: i64 = <Self as ActixJob>::TIMEOUT;

    fn run(self, state: Self::State) -> Self::Future {
        let (tx, rx) = oneshot::channel();

        let span = Span::current();
        let handle = actix_rt::spawn(async move {
            let entered = span.enter();
            let fut = ActixJob::run(self, state);
            drop(entered);

            let result = fut.instrument(span.clone()).await;

            if tx.send(result).is_err() {
                let entered = span.enter();
                error!("Job dropped");
                drop(entered);
            }
        });

        Box::pin(async move {
            handle.await.unwrap();
            rx.await?
        })
    }

    fn queue(&self) -> &str {
        ActixJob::queue(self)
    }

    fn max_retries(&self) -> MaxRetries {
        ActixJob::max_retries(self)
    }

    fn backoff_strategy(&self) -> Backoff {
        ActixJob::backoff_strategy(self)
    }

    fn timeout(&self) -> i64 {
        ActixJob::timeout(self)
    }
}
