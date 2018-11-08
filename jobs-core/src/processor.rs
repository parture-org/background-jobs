use failure::Error;
use futures::{
    future::{Either, IntoFuture},
    Future,
};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::Value;

use crate::{Backoff, JobError, JobInfo, MaxRetries};

/// The Processor trait
///
/// Processors define the logic for executing jobs
pub trait Processor: Clone {
    type Arguments: Serialize + DeserializeOwned;

    /// The name of the processor
    ///
    /// This name must be unique!!! It is used to look up which processor should handle a job
    fn name() -> &'static str;

    /// Define the default number of retries for a given processor
    ///
    /// Jobs can override
    fn max_retries() -> MaxRetries;

    /// Define the default backoff strategy for a given processor
    ///
    /// Jobs can override
    fn backoff_strategy() -> Backoff;

    /// Defines how jobs for this processor are processed
    ///
    /// Please do not perform blocking operations in the process method except if put behind
    /// tokio's `blocking` abstraction
    fn process(&self, args: Self::Arguments) -> Box<dyn Future<Item = (), Error = Error> + Send>;

    /// A provided method to create a new Job from provided arguments
    ///
    /// ### Example
    ///
    /// ```rust
    /// #[macro_use]
    /// extern crate log;
    ///
    /// use jobs::{Processor, MaxRetries};
    /// use failure::Error;
    /// use futures::future::{Future, IntoFuture};
    ///
    /// struct MyProcessor;
    ///
    /// impl Processor for MyProcessor {
    ///     type Arguments = i32;
    ///
    ///     fn name() -> &'static str {
    ///         "IncrementProcessor"
    ///     }
    ///
    ///     fn max_retries() -> MaxRetries {
    ///         MaxRetries::Count(1)
    ///     }
    ///
    ///     fn backoff_strategy() -> Backoff {
    ///         Backoff::Exponential(2)
    ///     }
    ///
    ///     fn process(
    ///         &self,
    ///         args: Self::Arguments,
    ///     ) -> Box<dyn Future<Item = (), Error = Error> + Send> {
    ///         info!("Processing {}", args);
    ///
    ///         Box::new(Ok(()).into_future())
    ///     }
    /// }
    ///
    /// fn main() -> Result<(), Error> {
    ///     let job = MyProcessor::new_job(1234, None)?;
    ///
    ///     Ok(())
    /// }
    /// ```
    fn new_job(
        args: Self::Arguments,
        max_retries: Option<MaxRetries>,
        backoff_strategy: Option<Backoff>,
    ) -> Result<JobInfo, Error> {
        let job = JobInfo::new(
            Self::name().to_owned(),
            serde_json::to_value(args)?,
            max_retries.unwrap_or(Self::max_retries()),
            backoff_strategy.unwrap_or(Self::backoff_strategy()),
        );

        Ok(job)
    }

    /// A provided method to coerce arguments into the expected type
    fn do_processing(&self, args: Value) -> Box<dyn Future<Item = (), Error = JobError> + Send> {
        let res = serde_json::from_value::<Self::Arguments>(args);

        let fut = match res {
            Ok(item) => Either::A(self.process(item).map_err(JobError::Processing)),
            Err(_) => Either::B(Err(JobError::Json).into_future()),
        };

        Box::new(fut)
    }
}
