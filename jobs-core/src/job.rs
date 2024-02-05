use crate::{Backoff, BoxError, JobError, MaxRetries, NewJobInfo};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::Value;
use std::{future::Future, pin::Pin, time::SystemTime};
use tracing::{Instrument, Span};

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
/// use background_jobs_core::{Job, new_job, BoxError};
/// use tracing::info;
/// use std::future::{ready, Ready};
///
/// #[derive(serde::Deserialize, serde::Serialize)]
/// struct MyJob {
///     count: i64,
/// }
///
/// impl Job for MyJob {
///     type State = ();
///     type Error = BoxError;
///     type Future = Ready<Result<(), BoxError>>;
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
/// fn main() -> Result<(), BoxError> {
///     let job = new_job(MyJob { count: 1234 })?;
///
///     Ok(())
/// }
/// ```
pub trait Job: Serialize + DeserializeOwned + 'static {
    /// The application state provided to this job at runtime.
    type State: Clone + 'static;

    /// The error type this job returns
    type Error: Into<BoxError>;

    /// The future returned by this job
    type Future: Future<Output = Result<(), Self::Error>> + Send;

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

    /// Define how often a job should update its heartbeat timestamp
    ///
    /// This is important for allowing the job server to reap processes that were started but never
    /// completed.
    ///
    /// Defaults to 5 seconds
    /// Jobs can override
    const HEARTBEAT_INTERVAL: u64 = 5_000;

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

    /// Define how often a job should update its heartbeat timestamp
    ///
    /// This is important for allowing the job server to reap processes that were started but never
    /// completed.
    fn heartbeat_interval(&self) -> u64 {
        Self::HEARTBEAT_INTERVAL
    }
}

/// A provided method to create a new JobInfo from provided arguments
pub fn new_job<J>(job: J) -> Result<NewJobInfo, BoxError>
where
    J: Job,
{
    let job = NewJobInfo::new(
        J::NAME.to_owned(),
        job.queue().to_owned(),
        job.max_retries(),
        job.backoff_strategy(),
        job.heartbeat_interval(),
        serde_json::to_value(job).map_err(|_| ToJson)?,
    );

    Ok(job)
}

/// Create a NewJobInfo to schedule a job to be performed after a certain time
pub fn new_scheduled_job<J>(job: J, after: SystemTime) -> Result<NewJobInfo, BoxError>
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
    let res = serde_json::from_value::<J>(args).map(move |job| {
        if let Some(span) = job.span() {
            let fut = span.in_scope(move || job.run(state));

            (fut, Some(span))
        } else {
            (job.run(state), None)
        }
    });

    Box::pin(async move {
        let (fut, span) = res?;

        let res = if let Some(span) = span {
            fut.instrument(span).await
        } else {
            fut.await
        };

        res.map_err(Into::into).map_err(JobError::Processing)
    })
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Failed to to turn job into value")]
pub struct ToJson;
