#![deny(missing_docs)]

//! # Background Jobs Core
//! _basic types and traits for implementing a background jobs processor_
//!
//! This crate shouldn't be depended on directly, except in the case of implementing a custom jobs
//! processor. For a default solution based on Actix and Sled, look at the `background-jobs` crate.

use failure::{Error, Fail};
use serde_derive::{Deserialize, Serialize};

mod job;
mod job_info;
mod processor;
mod processor_map;
mod stats;
mod storage;

pub use crate::{
    job::Job,
    job_info::{JobInfo, NewJobInfo, ReturnJobInfo},
    processor::Processor,
    processor_map::{CachedProcessorMap, ProcessorMap},
    stats::{JobStat, Stats},
    storage::{memory_storage, Storage},
};

#[derive(Debug, Fail)]
/// The error type returned by a `Processor`'s `process` method
pub enum JobError {
    /// Some error occurred while processing the job
    #[fail(display = "Error performing job: {}", _0)]
    Processing(#[cause] Error),

    /// Creating a `Job` type from the provided `serde_json::Value` failed
    #[fail(display = "Could not make JSON value from arguments")]
    Json,

    /// No processor was present to handle a given job
    #[fail(display = "No processor available for job")]
    MissingProcessor,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
/// Indicate the state of a job after an attempted run
pub enum JobResult {
    /// The job succeeded
    Success,

    /// The job failed
    Failure,

    /// There was no processor to run the job
    MissingProcessor,
}

impl JobResult {
    /// Indicate a successful job
    pub fn success() -> Self {
        JobResult::Success
    }

    /// Indicate a failed job
    pub fn failure() -> Self {
        JobResult::Failure
    }

    /// Indicate that the job's processor is not present
    pub fn missing_processor() -> Self {
        JobResult::MissingProcessor
    }

    /// Check if the job failed
    pub fn is_failure(&self) -> bool {
        *self == JobResult::Failure
    }

    /// Check if the job succeeded
    pub fn is_success(&self) -> bool {
        *self == JobResult::Success
    }

    /// Check if the job is missing it's processor
    pub fn is_missing_processor(&self) -> bool {
        *self == JobResult::MissingProcessor
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
/// Set the status of a job when storing it
pub enum JobStatus {
    /// Job should be queued
    Pending,

    /// Job is running
    Running,
}

impl JobStatus {
    /// The job should be queued
    pub fn pending() -> Self {
        JobStatus::Pending
    }

    /// The job is running
    pub fn running() -> Self {
        JobStatus::Running
    }

    /// Check if the job is ready to be queued
    pub fn is_pending(&self) -> bool {
        *self == JobStatus::Pending
    }

    /// Check if the job is running
    pub fn is_running(&self) -> bool {
        *self == JobStatus::Running
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
/// Different styles for retrying jobs
pub enum Backoff {
    /// Seconds between execution
    ///
    /// For example, `Backoff::Linear(5)` will retry a failed job 5 seconds after the previous
    /// attempt
    Linear(usize),

    /// Base for seconds between execution
    ///
    /// For example, `Backoff::Exponential(2)` will retry a failed job 2 seconds after the first
    /// failure, 4 seconds after the second failure, 8 seconds after the third failure, and 16
    /// seconds after the fourth failure
    Exponential(usize),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
/// How many times a job should be retried before giving up
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
/// A type that represents whether a job should be requeued
pub enum ShouldStop {
    /// The job has hit the maximum allowed number of retries, and should be failed permanently
    LimitReached,

    /// The job is allowed to be put back into the job queue
    Requeue,
}

impl ShouldStop {
    /// A boolean representation of this state
    pub fn should_requeue(&self) -> bool {
        *self == ShouldStop::Requeue
    }
}
