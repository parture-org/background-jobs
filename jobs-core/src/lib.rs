/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2019 Riley Trautman
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
    processor_map::ProcessorMap,
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
pub enum JobResult {
    Success,
    Failure,
    MissingProcessor,
}

impl JobResult {
    pub fn success() -> Self {
        JobResult::Success
    }

    pub fn failure() -> Self {
        JobResult::Failure
    }

    pub fn missing_processor() -> Self {
        JobResult::MissingProcessor
    }

    pub fn is_failure(&self) -> bool {
        *self == JobResult::Failure
    }

    pub fn is_success(&self) -> bool {
        *self == JobResult::Success
    }

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
    pub fn pending() -> Self {
        JobStatus::Pending
    }

    pub fn running() -> Self {
        JobStatus::Running
    }

    pub fn is_pending(&self) -> bool {
        *self == JobStatus::Pending
    }

    pub fn is_running(&self) -> bool {
        *self == JobStatus::Running
    }
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
