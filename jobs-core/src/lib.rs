#![deny(missing_docs)]

//! # Background Jobs Core
//! _basic types and traits for implementing a background jobs processor_
//!
//! This crate shouldn't be depended on directly, except in the case of implementing a custom jobs
//! processor. For a default solution based on Actix and Sled, look at the `background-jobs` crate.

mod box_error;
mod catch_unwind;
mod job;
mod job_info;
mod processor_map;
mod storage;
mod unsend_job;

pub use crate::{
    box_error::BoxError,
    job::{new_job, new_scheduled_job, process, Job},
    job_info::{JobInfo, NewJobInfo, ReturnJobInfo},
    processor_map::{CachedProcessorMap, ProcessorMap},
    storage::{memory_storage, Storage},
};

pub use unsend_job::{JoinError, UnsendJob, UnsendSpawner};

#[derive(Debug, thiserror::Error)]
/// The error type returned by the `process` method
pub enum JobError {
    /// Some error occurred while processing the job
    #[error("{0}")]
    Processing(#[from] BoxError),

    /// Creating a `Job` type from the provided `serde_json::Value` failed
    #[error("Could not make JSON value from arguments")]
    Json,

    /// This job type was not registered for this client
    #[error("This job type was not registered for the client")]
    Unregistered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
/// Indicate the state of a job after an attempted run
pub enum JobResult {
    /// The job succeeded
    Success,

    /// The job failed
    Failure,

    /// The worker had no concept of this job
    Unregistered,

    /// The worker requesting this job closed
    Unexecuted,
}

impl JobResult {
    /// Indicate a successful job
    pub const fn success() -> Self {
        JobResult::Success
    }

    /// Indicate a failed job
    pub const fn failure() -> Self {
        JobResult::Failure
    }

    /// Indicate that the job was not registered for this worker
    pub const fn unregistered() -> Self {
        JobResult::Unregistered
    }

    /// Check if the job failed
    pub const fn is_failure(self) -> bool {
        matches!(self, JobResult::Failure)
    }

    /// Check if the job succeeded
    pub const fn is_success(self) -> bool {
        matches!(self, JobResult::Success)
    }

    /// Check if the job is missing it's processor
    pub const fn is_unregistered(self) -> bool {
        matches!(self, JobResult::Unregistered)
    }

    /// Check if the job was returned without an execution attempt
    pub const fn is_unexecuted(self) -> bool {
        matches!(self, JobResult::Unexecuted)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
/// How many times a job should be retried before giving up
pub enum MaxRetries {
    /// Keep retrying forever
    Infinite,

    /// Put a limit on the number of retries
    Count(usize),
}

impl MaxRetries {
    fn compare(self, retry_count: u32) -> ShouldStop {
        match self {
            MaxRetries::Infinite => ShouldStop::Requeue,
            MaxRetries::Count(count) => {
                if (retry_count as usize) <= count {
                    ShouldStop::Requeue
                } else {
                    ShouldStop::LimitReached
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A type that represents whether a job should be requeued
pub enum ShouldStop {
    /// The job has hit the maximum allowed number of retries, and should be failed permanently
    LimitReached,

    /// The job is allowed to be put back into the job queue
    Requeue,
}

impl ShouldStop {
    /// A boolean representation of this state
    pub const fn should_requeue(&self) -> bool {
        matches!(self, ShouldStop::Requeue)
    }
}

impl From<serde_json::error::Error> for JobError {
    fn from(_: serde_json::error::Error) -> Self {
        JobError::Json
    }
}
