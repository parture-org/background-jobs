use crate::{Backoff, MaxRetries, Processor};
use anyhow::Error;
use serde::{de::DeserializeOwned, ser::Serialize};

/// The Job trait defines parameters pertaining to an instance of background job
#[async_trait::async_trait]
pub trait Job: Serialize + DeserializeOwned + 'static {
    /// The processor this job is associated with. The job's processor can be used to create a
    /// JobInfo from a job, which is used to serialize the job into a storage mechanism.
    type Processor: Processor<Job = Self>;

    /// The application state provided to this job at runtime.
    type State: Clone + 'static;

    /// Users of this library must define what it means to run a job.
    ///
    /// This should contain all the logic needed to complete a job. If that means queuing more
    /// jobs, sending an email, shelling out (don't shell out), or doing otherwise lengthy
    /// processes, that logic should all be called from inside this method.
    ///
    /// The state passed into this job is initialized at the start of the application. The state
    /// argument could be useful for containing a hook into something like r2d2, or the address of
    /// an actor in an actix-based system.
    async fn run(self, state: Self::State) -> Result<(), Error>;

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
