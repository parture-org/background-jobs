use crate::{Backoff, JobResult, JobStatus, MaxRetries, ShouldStop};
use serde_json::Value;
use std::time::SystemTime;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
/// Information about the sate of an attempted job
pub struct ReturnJobInfo {
    pub(crate) id: Uuid,
    pub(crate) result: JobResult,
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
    timeout: i64,
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
        timeout: i64,
        args: Value,
    ) -> Self {
        NewJobInfo {
            name,
            queue,
            args,
            max_retries,
            next_queue: None,
            backoff_strategy,
            timeout,
        }
    }

    /// The name of the queue this job will run in
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// Whether this job is ready to be run immediately
    pub fn is_ready(&self) -> bool {
        self.next_queue.is_none()
    }

    pub(crate) fn with_id(self, id: Uuid) -> JobInfo {
        JobInfo {
            id,
            name: self.name,
            queue: self.queue,
            status: JobStatus::Pending,
            args: self.args,
            retry_count: 0,
            max_retries: self.max_retries,
            next_queue: self.next_queue,
            backoff_strategy: self.backoff_strategy,
            updated_at: OffsetDateTime::now_utc(),
            timeout: self.timeout,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
/// Metadata pertaining to a job that exists within the background_jobs system
///
/// Although exposed publically, this type should only really be handled by the library itself, and
/// is impossible to create outside of the new_job method.
pub struct JobInfo {
    /// ID of the job
    id: Uuid,

    /// Name of the job
    name: String,

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
    next_queue: Option<OffsetDateTime>,

    /// The time this job was last updated
    updated_at: OffsetDateTime,

    /// Milliseconds from execution until the job is considered dead
    ///
    /// This is important for storage implementations to reap unfinished jobs
    timeout: i64,
}

impl JobInfo {
    /// The name of the queue this job will run in
    pub fn queue(&self) -> &str {
        &self.queue
    }

    fn updated(&mut self) {
        self.updated_at = OffsetDateTime::now_utc();
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn args(&self) -> Value {
        self.args.clone()
    }

    /// The ID of this job
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// How long (in milliseconds) before this job is considered failed and can be requeued
    pub fn timeout(&self) -> i64 {
        self.timeout
    }

    /// Convert a JobInfo into a ReturnJobInfo without executing it
    pub fn unexecuted(self) -> ReturnJobInfo {
        ReturnJobInfo {
            id: self.id,
            result: JobResult::Unexecuted,
        }
    }

    /// If the job is queued to run in the future, when is that
    pub fn next_queue(&self) -> Option<SystemTime> {
        self.next_queue.map(|time| time.into())
    }

    pub(crate) fn increment(&mut self) -> ShouldStop {
        self.updated();
        self.retry_count += 1;
        self.max_retries.compare(self.retry_count)
    }

    fn set_next_queue(&mut self) {
        let now = OffsetDateTime::now_utc();

        let next_queue = match self.backoff_strategy {
            Backoff::Linear(secs) => now + Duration::seconds(secs as i64),
            Backoff::Exponential(base) => {
                let secs = base.pow(self.retry_count);
                now + Duration::seconds(secs as i64)
            }
        };

        self.next_queue = Some(next_queue);

        tracing::trace!(
            "Now {}, Next queue {}, ready {}",
            now,
            next_queue,
            self.is_ready(now.into()),
        );
    }

    /// Whether this job is ready to be run
    pub fn is_ready(&self, now: SystemTime) -> bool {
        match self.next_queue {
            Some(ref time) => now > *time,
            None => true,
        }
    }

    pub(crate) fn needs_retry(&mut self) -> bool {
        let should_retry = self.increment().should_requeue();

        if should_retry {
            self.pending();
            self.set_next_queue();
        }

        should_retry
    }

    /// Whether this job is pending execution
    pub fn is_pending(&self, now: SystemTime) -> bool {
        self.status == JobStatus::Pending
            || (self.status == JobStatus::Running
                && (self.updated_at + Duration::milliseconds(self.timeout)) < now)
    }

    /// Get the status of the job
    pub fn status(&self) -> JobStatus {
        self.status.clone()
    }

    /// The the date of the most recent update
    pub fn updated_at(&self) -> SystemTime {
        self.updated_at.into()
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
