/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2018 Riley Trautman
 *
 * Background Jobs is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Background Jobs is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Background Jobs.  If not, see <http://www.gnu.org/licenses/>.
 */

use failure::Error;
use futures::{
    future::{Either, IntoFuture},
    Future,
};
use serde_json::Value;

use crate::{Backoff, Job, JobError, JobInfo, MaxRetries};

/// ## The Processor trait
///
/// Processors define the logic spawning jobs such as
///  - The job's name
///  - The job's default queue
///  - The job's default maximum number of retries
///  - The job's [backoff
///    strategy](https://docs.rs/background-jobs/0.2.0/background_jobs/enum.Backoff.html)
///
/// Processors also provide the default mechanism for running a job, and the only mechanism for
/// creating a
/// [JobInfo](https://docs.rs/background-jobs-core/0.2.0/background_jobs_core/struct.JobInfo.html),
/// which is the type required for queuing jobs to be executed.
///
/// ### Example
///
/// ```rust
/// use background_jobs_core::{Backoff, Job, MaxRetries, Processor};
/// use failure::Error;
/// use futures::future::{Future, IntoFuture};
/// use log::info;
/// use serde_derive::{Deserialize, Serialize};
///
/// #[derive(Deserialize, Serialize)]
/// struct MyJob {
///     count: i32,
/// }
///
/// impl Job for MyJob {
///     fn run(self) -> Box<dyn Future<Item = (), Error = Error> + Send> {
///         info!("Processing {}", self.count);
///
///         Box::new(Ok(()).into_future())
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
///     const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);
///     const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);
/// }
///
/// fn main() -> Result<(), Error> {
///     let job = MyProcessor::new_job(MyJob { count: 1234 })?;
///
///     Ok(())
/// }
/// ```
pub trait Processor: Clone {
    type Job: Job;

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
    /// Jobs can override
    const MAX_RETRIES: MaxRetries;

    /// Define the default backoff strategy for a given processor
    ///
    /// Jobs can override
    const BACKOFF_STRATEGY: Backoff;

    /// A provided method to create a new JobInfo from provided arguments
    ///
    /// This is required for spawning jobs, since it enforces the relationship between the job and
    /// the Processor that should handle it.
    fn new_job(job: Self::Job) -> Result<JobInfo, Error> {
        let queue = job.queue().unwrap_or(Self::QUEUE).to_owned();
        let max_retries = job.max_retries().unwrap_or(Self::MAX_RETRIES);
        let backoff_strategy = job.backoff_strategy().unwrap_or(Self::BACKOFF_STRATEGY);

        let job = JobInfo::new(
            Self::NAME.to_owned(),
            queue,
            serde_json::to_value(job)?,
            max_retries,
            backoff_strategy,
        );

        Ok(job)
    }

    /// A provided method to coerce arguments into the expected type and run the job
    ///
    /// Advanced users may want to override this method in order to provide their own custom
    /// before/after logic for certain job processors
    ///
    /// ```rust,ignore
    /// fn process(&self, args: Value) -> Box<dyn Future<Item = (), Error = JobError> + Send> {
    ///     let res = serde_json::from_value::<Self::Job>(args);
    ///
    ///     let fut = match res {
    ///         Ok(job) => {
    ///             // Perform some custom pre-job logic
    ///             Either::A(job.run().map_err(JobError::Processing))
    ///         },
    ///         Err(_) => Either::B(Err(JobError::Json).into_future()),
    ///     };
    ///
    ///     Box::new(fut.and_then(|_| {
    ///         // Perform some custom post-job logic
    ///     }))
    /// }
    /// ```
    ///
    /// Patterns like this could be useful if you want to use the same job type for multiple
    /// scenarios. Defining the `process` method for multiple `Processor`s with different
    /// before/after logic for the same
    /// [`Job`](https://docs.rs/background-jobs/0.2.0/background_jobs/trait.Job.html) type is
    /// supported.
    fn process(&self, args: Value) -> Box<dyn Future<Item = (), Error = JobError> + Send> {
        let res = serde_json::from_value::<Self::Job>(args);

        let fut = match res {
            Ok(job) => Either::A(job.run().map_err(JobError::Processing)),
            Err(_) => Either::B(Err(JobError::Json).into_future()),
        };

        Box::new(fut)
    }
}
