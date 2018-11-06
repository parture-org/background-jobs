#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::collections::HashMap;

use failure::Error;
use futures::future::{Either, Future, IntoFuture};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::Value;

pub mod storage;

#[derive(Debug, Fail)]
pub enum JobError {
    #[fail(display = "Error performing job: {}", _0)]
    Processing(#[cause] Error),
    #[fail(display = "Could not make JSON value from arguments")]
    Json,
    #[fail(display = "No processor available for job")]
    MissingProcessor,
}

/// The Processor trait
///
/// Processors are
pub trait Processor {
    type Arguments: Serialize + DeserializeOwned;

    /// The name of the processor
    ///
    /// This name must be unique!!! It is used to look up which processor should handle a job
    fn name() -> &'static str;

    /// Define the default number of retries for a given processor
    ///
    /// Jobs can override
    fn max_retries() -> MaxRetries;

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
    fn new_job(args: Self::Arguments, max_retries: Option<MaxRetries>) -> Result<JobInfo, Error> {
        let job = JobInfo {
            id: None,
            processor: Self::name().to_owned(),
            status: JobStatus::Pending,
            args: serde_json::to_value(args)?,
            retry_count: max_retries.unwrap_or(Self::max_retries()),
            requeued_at: None,
        };

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

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum JobStatus {
    Pending,
    Active,
    Finished,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum Backoff {
    /// Seconds between execution
    Linear(usize),

    /// Base for seconds between execution
    Exponential(usize),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum MaxRetries {
    /// Keep retrying forever
    Infinite,

    /// Put a limit on the number of retries
    Count(usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShouldStop {
    LimitReached,
    Requeue,
}

impl ShouldStop {
    pub fn should_requeue(&self) -> bool {
        *self == ShouldStop::Requeue
    }
}

impl MaxRetries {
    fn decrement(&mut self) -> ShouldStop {
        match *self {
            MaxRetries::Infinite => ShouldStop::Requeue,
            MaxRetries::Count(ref mut count) => {
                *count = *count - 1;

                if *count == 0 {
                    ShouldStop::LimitReached
                } else {
                    ShouldStop::Requeue
                }
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobInfo {
    /// ID of the job, None means an ID has not been set
    id: Option<usize>,

    /// Name of the processor that should handle this job
    processor: String,

    /// Arguments for a given job
    args: Value,

    /// Status of the job
    status: JobStatus,

    /// Retries left for this job, None means no limit
    retry_count: MaxRetries,

    /// The time this job was re-queued
    requeued_at: Option<usize>,
}

impl JobInfo {
    pub fn decrement(&mut self) -> ShouldStop {
        self.retry_count.decrement()
    }

    pub fn id(&self) -> Option<usize> {
        self.id.clone()
    }

    pub fn set_id(&mut self, id: usize) {
        if self.id.is_none() {
            self.id = Some(id);
        }
    }
}

pub type ProcessFn =
    Box<dyn Fn(Value) -> Box<dyn Future<Item = (), Error = JobError> + Send> + Send>;

pub struct Processors {
    inner: HashMap<String, ProcessFn>,
}

impl Processors {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.inner.insert(
            P::name().to_owned(),
            Box::new(move |value| processor.do_processing(value)),
        );
    }

    pub fn process_job(&self, job: JobInfo) -> impl Future<Item = JobInfo, Error = ()> {
        let opt = self
            .inner
            .get(&job.processor)
            .map(|processor| process(processor, job.clone()));

        if let Some(fut) = opt {
            Either::A(fut)
        } else {
            error!("Processor {} not present", job.processor);
            Either::B(Ok(job).into_future())
        }
    }
}

impl Default for Processors {
    fn default() -> Self {
        Processors {
            inner: Default::default(),
        }
    }
}

fn process(process_fn: &ProcessFn, job: JobInfo) -> impl Future<Item = JobInfo, Error = ()> {
    let args = job.args.clone();

    let processor = job.processor.clone();

    process_fn(args).then(move |res| match res {
        Ok(_) => {
            info!("Job completed, {}", processor);
            Ok(job)
        }
        Err(e) => {
            error!("Job errored, {}, {}", processor, e);
            Ok(job)
        }
    })
}
