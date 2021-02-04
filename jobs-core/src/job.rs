use crate::{Backoff, JobError, MaxRetries, NewJobInfo};
use anyhow::Error;
use chrono::{offset::Utc, DateTime};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::Value;
use std::{future::Future, pin::Pin};

/// The Job trait defines parameters pertaining to an instance of background job
///
/// Jobs are defnitions of work to be executed.
///
/// The simplest implementation defines the job's State and Future types, NAME contant, and
/// run method.
///
/// ### Example
///
/// ```rust
/// use anyhow::Error;
/// use background_jobs_core::{Job, new_job};
/// use log::info;
/// use std::future::{ready, Ready};
///
/// #[derive(serde::Deserialize, serde::Serialize)]
/// struct MyJob {
///     count: i64,
/// }
///
/// impl Job for MyJob {
///     type State = ();
///     type Future = Ready<Result<(), Error>>;
///
///     const NAME: &'static str = "MyJob";
///
///     fn run(self, _: Self::State) -> Self::Future {
///         info!("Processing {}", self.count);
///
///         ready(Ok(()))
///     }
/// }
///
/// fn main() -> Result<(), Error> {
///     let job = new_job(MyJob { count: 1234 })?;
///
///     Ok(())
/// }
/// ```
pub trait Job: Serialize + DeserializeOwned + 'static {
    /// The application state provided to this job at runtime.
    type State: Clone + 'static;

    /// The future returned by this job
    type Future: Future<Output = Result<(), Error>> + Send;

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

/// A provided method to create a new JobInfo from provided arguments
pub fn new_job<J>(job: J) -> Result<NewJobInfo, Error>
where
    J: Job,
{
    let job = NewJobInfo::new(
        J::NAME.to_owned(),
        job.queue().to_owned(),
        job.max_retries(),
        job.backoff_strategy(),
        job.timeout(),
        serde_json::to_value(job).map_err(|_| ToJson)?,
    );

    Ok(job)
}

/// Create a NewJobInfo to schedule a job to be performed after a certain time
pub fn new_scheduled_job<J>(job: J, after: DateTime<Utc>) -> Result<NewJobInfo, Error>
where
    J: Job,
{
    let mut job = new_job(job)?;
    job.schedule(after);

    Ok(job)
}

/// A provided method to coerce arguments into the expected type and run the job
pub fn process<J>(
    args: Value,
    state: J::State,
) -> Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>>
where
    J: Job,
{
    let res = serde_json::from_value::<J>(args).map(move |job| job.run(state));

    Box::pin(async move {
        res?.await?;

        Ok(())
    })
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Failed to to turn job into value")]
pub struct ToJson;
