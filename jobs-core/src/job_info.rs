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

use chrono::{offset::Utc, DateTime, Duration as OldDuration};
use log::trace;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Backoff, JobStatus, MaxRetries, ShouldStop};

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

    pub(crate) fn with_id(self, id: usize) -> JobInfo {
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
/// [Processor](https://docs.rs/background-jobs/0.3.0/background_jobs/trait.Processor.html)'s
/// new_job method.
pub struct JobInfo {
    /// ID of the job
    id: usize,

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

    pub(crate) fn updated(&mut self) {
        self.updated_at = Utc::now();
    }

    pub(crate) fn processor(&self) -> &str {
        &self.processor
    }

    pub(crate) fn args(&self) -> Value {
        self.args.clone()
    }

    pub(crate) fn status(&self) -> JobStatus {
        self.status.clone()
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn increment(&mut self) -> ShouldStop {
        self.retry_count += 1;
        self.max_retries.compare(self.retry_count)
    }

    pub(crate) fn next_queue(&mut self) {
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

    pub(crate) fn is_stale(&self) -> bool {
        self.updated_at < Utc::now() - OldDuration::days(1)
    }

    pub(crate) fn is_ready(&self, now: DateTime<Utc>) -> bool {
        match self.next_queue {
            Some(ref time) => now > *time,
            None => true,
        }
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.status == JobStatus::Failed
    }

    pub fn needs_retry(&mut self) -> bool {
        let should_retry = self.is_failed() && self.increment().should_requeue();

        if should_retry {
            self.pending();
            self.next_queue();
        }

        should_retry
    }

    pub fn retry_ready(&self) -> bool {
        self.is_ready(Utc::now())
    }

    pub fn is_pending(&self) -> bool {
        self.status == JobStatus::Pending
    }

    pub(crate) fn is_in_queue(&self, queue: &str) -> bool {
        self.queue == queue
    }

    pub(crate) fn stage(&mut self) {
        self.status = JobStatus::Staged;
    }

    /// This method sets the Job's status to running
    ///
    /// Touching this outside of the background_jobs crates is dangerous, since these libraries
    /// rely on the state of the job being correct.
    pub fn set_running(&mut self) {
        self.status = JobStatus::Running;
    }

    pub(crate) fn pending(&mut self) {
        self.status = JobStatus::Pending;
    }

    pub(crate) fn fail(&mut self) {
        self.status = JobStatus::Failed;
    }

    pub(crate) fn pass(&mut self) {
        self.status = JobStatus::Finished;
    }
}
