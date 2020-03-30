use crate::{Backoff, Job, MaxRetries, Processor};
use anyhow::Error;
use log::error;
use serde::{de::DeserializeOwned, ser::Serialize};
use std::{future::Future, pin::Pin};
use tokio::sync::oneshot;

/// The ActixJob trait defines parameters pertaining to an instance of background job
///
/// This trait is specific to Actix, and will automatically implement the Job trait with the
/// proper translation from ?Send futures to Send futures
pub trait ActixJob: Serialize + DeserializeOwned + 'static {
    /// The processor this job is associated with. The job's processor can be used to create a
    /// JobInfo from a job, which is used to serialize the job into a storage mechanism.
    type Processor: Processor<Job = Self>;

    /// The application state provided to this job at runtime.
    type State: Clone + 'static;

    /// The future returned by this job
    type Future: Future<Output = Result<(), Error>>;

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

    /// If this job should not use the default queue for its processor, this can be overridden in
    /// user-code.
    ///
    /// Jobs will only be processed by processors that are registered, and if a queue is supplied
    /// here that is not associated with a valid processor for this job, it will never be
    /// processed.
    fn queue(&self) -> Option<&str> {
        None
    }

    /// If this job should not use the default maximum retry count for its processor, this can be
    /// overridden in user-code.
    fn max_retries(&self) -> Option<MaxRetries> {
        None
    }

    /// If this job should not use the default backoff strategy for its processor, this can be
    /// overridden in user-code.
    fn backoff_strategy(&self) -> Option<Backoff> {
        None
    }

    /// Define the maximum number of milliseconds this job should be allowed to run before being
    /// considered dead.
    ///
    /// This is important for allowing the job server to reap processes that were started but never
    /// completed.
    fn timeout(&self) -> Option<i64> {
        None
    }
}

impl<T> Job for T
where
    T: ActixJob,
{
    type Processor = T::Processor;
    type State = T::State;
    type Future = Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

    fn run(self, state: Self::State) -> Self::Future {
        let (tx, rx) = oneshot::channel();

        actix::spawn(async move {
            if let Err(_) = tx.send(ActixJob::run(self, state).await) {
                error!("Job dropped");
            }
        });

        Box::pin(async move { rx.await? })
    }

    fn queue(&self) -> Option<&str> {
        ActixJob::queue(self)
    }

    fn max_retries(&self) -> Option<MaxRetries> {
        ActixJob::max_retries(self)
    }

    fn backoff_strategy(&self) -> Option<Backoff> {
        ActixJob::backoff_strategy(self)
    }

    fn timeout(&self) -> Option<i64> {
        ActixJob::timeout(self)
    }
}
