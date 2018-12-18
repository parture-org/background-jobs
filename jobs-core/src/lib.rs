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

use failure::{Error, Fail};
use serde_derive::{Deserialize, Serialize};

mod job;
mod job_info;
mod processor;
mod processor_map;
mod storage;

pub use crate::{
    job::Job,
    job_info::{JobInfo, NewJobInfo},
    processor::Processor,
    processor_map::ProcessorMap,
    storage::{JobStat, Stat, Stats, Storage},
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
/// Set the status of a job when storing it
pub enum JobStatus {
    /// Job should be queued
    Pending,

    /// Job has been dequeued, but is not yet running
    Staged,

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
