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

use chrono::{offset::Utc, DateTime, Duration as OldDuration};
use log::trace;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Backoff, JobResult, JobStatus, MaxRetries, ShouldStop};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReturnJobInfo {
    pub(crate) id: u64,
    pub(crate) result: JobResult,
}

impl ReturnJobInfo {
    pub(crate) fn fail(id: u64) -> Self {
        ReturnJobInfo {
            id,
            result: JobResult::Failure,
        }
    }

    pub(crate) fn pass(id: u64) -> Self {
        ReturnJobInfo {
            id,
            result: JobResult::Success,
        }
    }

    pub(crate) fn missing_processor(id: u64) -> Self {
        ReturnJobInfo {
            id,
            result: JobResult::MissingProcessor,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct NewJobInfo {
    /// Name of the processor that should handle this job
    processor: String,

    /// Name of the queue that this job is a part of
    queue: String,

    /// Arguments for a given job
    args: Value,

    /// the initial MaxRetries value, for comparing to the current retry count
    max_retries: MaxRetries,

    /// How often retries should be scheduled
    backoff_strategy: Backoff,

    /// The time this job should be dequeued
    next_queue: Option<DateTime<Utc>>,
}

impl NewJobInfo {
    pub(crate) fn schedule(&mut self, time: DateTime<Utc>) {
        self.next_queue = Some(time);
    }

    pub(crate) fn new(
        processor: String,
        queue: String,
        args: Value,
        max_retries: MaxRetries,
        backoff_strategy: Backoff,
    ) -> Self {
        NewJobInfo {
            processor,
            queue,
            args,
            max_retries,
            next_queue: None,
            backoff_strategy,
        }
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn is_ready(&self) -> bool {
        self.next_queue.is_none()
    }

    pub(crate) fn with_id(self, id: u64) -> JobInfo {
        JobInfo {
            id,
            processor: self.processor,
            queue: self.queue,
            status: JobStatus::Pending,
            args: self.args,
            retry_count: 0,
            max_retries: self.max_retries,
            next_queue: self.next_queue,
            backoff_strategy: self.backoff_strategy,
            updated_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
/// Metadata pertaining to a job that exists within the background_jobs system
///
/// Although exposed publically, this type should only really be handled by the library itself, and
/// is impossible to create outside of a
/// [Processor](https://docs.rs/background-jobs/0.4.0/background_jobs/trait.Processor.html)'s
/// new_job method.
pub struct JobInfo {
    /// ID of the job
    id: u64,

    /// Name of the processor that should handle this job
    processor: String,

    /// Name of the queue that this job is a part of
    queue: String,

    /// Arguments for a given job
    args: Value,

    /// Status of the job
    status: JobStatus,

    /// Retries left for this job, None means no limit
    retry_count: u32,

    /// the initial MaxRetries value, for comparing to the current retry count
    max_retries: MaxRetries,

    /// How often retries should be scheduled
    backoff_strategy: Backoff,

    /// The time this job should be dequeued
    next_queue: Option<DateTime<Utc>>,

    /// The time this job was last updated
    updated_at: DateTime<Utc>,
}

impl JobInfo {
    pub fn queue(&self) -> &str {
        &self.queue
    }

    fn updated(&mut self) {
        self.updated_at = Utc::now();
    }

    pub(crate) fn processor(&self) -> &str {
        &self.processor
    }

    pub(crate) fn args(&self) -> Value {
        self.args.clone()
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn increment(&mut self) -> ShouldStop {
        self.updated();
        self.retry_count += 1;
        self.max_retries.compare(self.retry_count)
    }

    fn next_queue(&mut self) {
        let now = Utc::now();

        let next_queue = match self.backoff_strategy {
            Backoff::Linear(secs) => now + OldDuration::seconds(secs as i64),
            Backoff::Exponential(base) => {
                let secs = base.pow(self.retry_count);
                now + OldDuration::seconds(secs as i64)
            }
        };

        self.next_queue = Some(next_queue);

        trace!(
            "Now {}, Next queue {}, ready {}",
            now,
            next_queue,
            self.is_ready(now),
        );
    }

    pub fn is_ready(&self, now: DateTime<Utc>) -> bool {
        match self.next_queue {
            Some(ref time) => now > *time,
            None => true,
        }
    }

    pub(crate) fn needs_retry(&mut self) -> bool {
        let should_retry = self.increment().should_requeue();

        if should_retry {
            self.pending();
            self.next_queue();
        }

        should_retry
    }

    pub fn is_pending(&self) -> bool {
        self.status == JobStatus::Pending
    }

    pub(crate) fn is_in_queue(&self, queue: &str) -> bool {
        self.queue == queue
    }

    pub(crate) fn run(&mut self) {
        self.updated();
        self.status = JobStatus::Running;
    }

    pub(crate) fn pending(&mut self) {
        self.updated();
        self.status = JobStatus::Pending;
    }
}
