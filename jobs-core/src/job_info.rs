use crate::{Backoff, JobResult, MaxRetries, ShouldStop};
use serde_json::Value;
use std::time::SystemTime;
use time::{Duration, OffsetDateTime};
use uuid::{NoContext, Timestamp, Uuid};

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
/// Information about the sate of an attempted job
pub struct ReturnJobInfo {
    /// The ID of the job being returned
    pub id: Uuid,
    /// The result status of the job
    pub result: JobResult,
}

impl ReturnJobInfo {
    pub(crate) fn fail(id: Uuid) -> Self {
        ReturnJobInfo {
            id,
            result: JobResult::Failure,
        }
    }

    pub(crate) fn pass(id: Uuid) -> Self {
        ReturnJobInfo {
            id,
            result: JobResult::Success,
        }
    }

    pub(crate) fn unregistered(id: Uuid) -> Self {
        ReturnJobInfo {
            id,
            result: JobResult::Unregistered,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
/// Information about a newly created job
pub struct NewJobInfo {
    /// Name of the job
    name: String,

    /// Name of the queue that this job is a part of
    queue: String,

    /// Arguments for a given job
    args: Value,

    /// the initial MaxRetries value, for comparing to the current retry count
    max_retries: MaxRetries,

    /// How often retries should be scheduled
    backoff_strategy: Backoff,

    /// The time this job should be dequeued
    next_queue: Option<OffsetDateTime>,

    /// Milliseconds from execution until the job is considered dead
    ///
    /// This is important for storage implementations to reap unfinished jobs
    heartbeat_interval: u64,
}

impl NewJobInfo {
    pub(crate) fn schedule(&mut self, time: SystemTime) {
        self.next_queue = Some(time.into());
    }

    pub(crate) fn new(
        name: String,
        queue: String,
        max_retries: MaxRetries,
        backoff_strategy: Backoff,
        heartbeat_interval: u64,
        args: Value,
    ) -> Self {
        NewJobInfo {
            name,
            queue,
            args,
            max_retries,
            next_queue: None,
            backoff_strategy,
            heartbeat_interval,
        }
    }

    /// The name of the queue this job will run in
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The name of this job
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether this job is ready to be run immediately
    pub fn is_ready(&self) -> bool {
        self.next_queue.is_none()
    }

    /// Construct a JobInfo from a NewJobInfo
    pub fn build(self) -> JobInfo {
        JobInfo {
            id: Uuid::now_v7(),
            name: self.name,
            queue: self.queue,
            args: self.args,
            retry_count: 0,
            max_retries: self.max_retries,
            next_queue: self.next_queue.unwrap_or(OffsetDateTime::now_utc()),
            backoff_strategy: self.backoff_strategy,
            heartbeat_interval: self.heartbeat_interval,
        }
    }
}

fn uuid_from_timestamp(timestamp: OffsetDateTime) -> Uuid {
    let unix_seconds = timestamp.unix_timestamp().abs_diff(0);
    let unix_nanos = (timestamp.unix_timestamp_nanos() % i128::from(timestamp.unix_timestamp()))
        .abs_diff(0) as u32;

    Uuid::new_v7(Timestamp::from_unix(NoContext, unix_seconds, unix_nanos))
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
/// Metadata pertaining to a job that exists within the background_jobs system
///
/// Although exposed publically, this type should only really be handled by the library itself, and
/// is impossible to create outside of the new_job method.
pub struct JobInfo {
    /// ID of the job
    pub id: Uuid,

    /// Name of the job
    pub name: String,

    /// Name of the queue that this job is a part of
    pub queue: String,

    /// Arguments for a given job
    pub args: Value,

    /// Retries left for this job, None means no limit
    pub retry_count: u32,

    /// the initial MaxRetries value, for comparing to the current retry count
    pub max_retries: MaxRetries,

    /// How often retries should be scheduled
    pub backoff_strategy: Backoff,

    /// The time this job should be dequeued
    pub next_queue: OffsetDateTime,

    /// Milliseconds from execution until the job is considered dead
    ///
    /// This is important for storage implementations to reap unfinished jobs
    pub heartbeat_interval: u64,
}

impl JobInfo {
    /// Convert a JobInfo into a ReturnJobInfo without executing it
    pub fn unexecuted(self) -> ReturnJobInfo {
        ReturnJobInfo {
            id: self.id,
            result: JobResult::Unexecuted,
        }
    }

    /// Produce a UUID from the next_queue timestamp
    pub fn next_queue_id(&self) -> Uuid {
        uuid_from_timestamp(self.next_queue)
    }

    // Increment the retry-count and determine if the job should be requeued
    fn increment(&mut self) -> ShouldStop {
        self.retry_count += 1;
        self.max_retries.compare(self.retry_count)
    }

    /// Update the timestamp on the JobInfo to reflect the next queue time
    fn set_next_queue(&mut self) {
        let now = OffsetDateTime::now_utc();

        self.next_queue = match self.backoff_strategy {
            Backoff::Linear(secs) => now + Duration::seconds(secs as i64),
            Backoff::Exponential(base) => {
                let secs = base.pow(self.retry_count);
                now + Duration::seconds(secs as i64)
            }
        };

        tracing::trace!("Now {}, Next queue {}", now, self.next_queue);
    }

    /// Increment the retry-count and set next_queue based on the job's configuration
    ///
    /// returns `true` if the job should be retried
    pub fn prepare_retry(&mut self) -> bool {
        let should_retry = self.increment().should_requeue();

        if should_retry {
            self.set_next_queue();
        }

        should_retry
    }
}
