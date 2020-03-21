use crate::{Backoff, Job, JobError, MaxRetries, NewJobInfo};
use anyhow::Error;
use chrono::{offset::Utc, DateTime};
use serde_json::Value;
use std::{future::Future, pin::Pin};

/// ## The Processor trait
///
/// Processors define the logic spawning jobs such as
///  - The job's name
///  - The job's default queue
///  - The job's default maximum number of retries
///  - The job's [backoff
///    strategy](https://docs.rs/background-jobs/0.4.0/background_jobs/enum.Backoff.html)
///
/// Processors also provide the default mechanism for running a job, and the only mechanism for
/// creating a
/// [JobInfo](https://docs.rs/background-jobs-core/0.4.0/background_jobs_core/struct.JobInfo.html),
/// which is the type required for queuing jobs to be executed.
///
/// ### Example
///
/// ```rust
/// use anyhow::Error;
/// use background_jobs_core::{Job, Processor};
/// use futures::future::{ok, Ready};
/// use log::info;
///
/// #[derive(serde::Deserialize, serde::Serialize)]
/// struct MyJob {
///     count: i32,
/// }
///
/// impl Job for MyJob {
///     type Processor = MyProcessor;
///     type State = ();
///     type Future = Ready<Result<(), Error>>;
///
///     fn run(self, _state: Self::State) -> Self::Future {
///         info!("Processing {}", self.count);
///
///         ok(())
///     }
/// }
///
/// #[derive(Clone)]
/// struct MyProcessor;
///
/// impl Processor for MyProcessor {
///     type Job = MyJob;
///
///     const NAME: &'static str = "IncrementProcessor";
///     const QUEUE: &'static str = "default";
/// }
///
/// fn main() -> Result<(), Error> {
///     let job = MyProcessor::new_job(MyJob { count: 1234 })?;
///
///     Ok(())
/// }
/// ```
pub trait Processor: Clone {
    /// The job this processor will process
    type Job: Job + 'static;

    /// The name of the processor
    ///
    /// This name must be unique!!! It is used to look up which processor should handle a job
    const NAME: &'static str;

    /// The name of the default queue for jobs created with this processor
    ///
    /// This can be overridden on an individual-job level, but if a non-existant queue is supplied,
    /// the job will never be processed.
    const QUEUE: &'static str;

    /// Define the default number of retries for a given processor
    ///
    /// Defaults to Count(5)
    /// Jobs can override
    const MAX_RETRIES: MaxRetries = MaxRetries::Count(5);

    /// Define the default backoff strategy for a given processor
    ///
    /// Defaults to Exponential(2)
    /// Jobs can override
    const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);

    /// Define the maximum number of milliseconds a job should be allowed to run before being
    /// considered dead.
    ///
    /// This is important for allowing the job server to reap processes that were started but never
    /// completed.
    ///
    /// Defaults to 15 seconds
    /// Jobs can override
    const TIMEOUT: i64 = 15_000;

    /// A provided method to create a new JobInfo from provided arguments
    ///
    /// This is required for spawning jobs, since it enforces the relationship between the job and
    /// the Processor that should handle it.
    fn new_job(job: Self::Job) -> Result<NewJobInfo, Error> {
        let queue = job.queue().unwrap_or(Self::QUEUE).to_owned();
        let max_retries = job.max_retries().unwrap_or(Self::MAX_RETRIES);
        let backoff_strategy = job.backoff_strategy().unwrap_or(Self::BACKOFF_STRATEGY);
        let timeout = job.timeout().unwrap_or(Self::TIMEOUT);

        let job = NewJobInfo::new(
            Self::NAME.to_owned(),
            queue,
            serde_json::to_value(job).map_err(|_| ToJson)?,
            max_retries,
            backoff_strategy,
            timeout,
        );

        Ok(job)
    }

    /// Create a JobInfo to schedule a job to be performed after a certain time
    fn new_scheduled_job(job: Self::Job, after: DateTime<Utc>) -> Result<NewJobInfo, Error> {
        let mut job = Self::new_job(job)?;
        job.schedule(after);

        Ok(job)
    }

    /// A provided method to coerce arguments into the expected type and run the job
    ///
    /// Advanced users may want to override this method in order to provide their own custom
    /// before/after logic for certain job processors
    ///
    /// The state passed into this method is initialized at the start of the application. The state
    /// argument could be useful for containing a hook into something like r2d2, or the address of
    /// an actor in an actix-based system.
    ///
    /// ```rust,ignore
    /// fn process(
    ///     &self,
    ///     args: Value,
    ///     state: S
    /// ) -> Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>> {
    ///     let res = serde_json::from_value::<Self::Job>(args);
    ///
    ///     Box::pin(async move {
    ///         let job = res.map_err(|_| JobError::Json)?;
    ///         // Perform some custom pre-job locic
    ///
    ///         job.run(state).await.map_err(JobError::Processing)?;
    ///
    ///         // Perform some custom post-job logic
    ///         Ok(())
    ///     })
    /// }
    /// ```
    ///
    /// Patterns like this could be useful if you want to use the same job type for multiple
    /// scenarios. Defining the `process` method for multiple `Processor`s with different
    /// before/after logic for the same [`Job`] supported.
    fn process(
        &self,
        args: Value,
        state: <Self::Job as Job>::State,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>> {
        // Call run on the job here because State isn't Send, but the future produced by job IS
        // Send
        let res = serde_json::from_value::<Self::Job>(args).map(move |job| job.run(state));

        Box::pin(async move {
            res?.await?;

            Ok(())
        })
    }
}

#[derive(Clone, Debug, thiserror::Error)]
#[error("Failed to to turn job into value")]
pub struct ToJson;
