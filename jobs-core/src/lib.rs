#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use failure::Error;

mod job_info;
mod processor;
mod processors;
mod storage;

pub use crate::{
    job_info::JobInfo, processor::Processor, processors::Processors, storage::Storage,
};

#[derive(Debug, Fail)]
pub enum JobError {
    #[fail(display = "Error performing job: {}", _0)]
    Processing(#[cause] Error),
    #[fail(display = "Could not make JSON value from arguments")]
    Json,
    #[fail(display = "No processor available for job")]
    MissingProcessor,
}

/// Set the status of a job when storing it
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum JobStatus {
    /// Job should be queued
    Pending,

    /// Job is running
    Running,

    /// Job has failed
    Failed,

    /// Job has finished
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

impl MaxRetries {
    fn compare(&self, retry_count: u32) -> ShouldStop {
        match *self {
            MaxRetries::Infinite => ShouldStop::Requeue,
            MaxRetries::Count(ref count) => {
                if (retry_count as usize) <= *count {
                    ShouldStop::Requeue
                } else {
                    ShouldStop::LimitReached
                }
            }
        }
    }
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
