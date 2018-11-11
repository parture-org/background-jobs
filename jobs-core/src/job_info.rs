use chrono::{offset::Utc, DateTime, Duration as OldDuration};
use serde_json::Value;

use crate::{Backoff, JobStatus, MaxRetries, ShouldStop};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobInfo {
    /// ID of the job, None means an ID has not been set
    id: Option<usize>,

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
    pub(crate) fn new(
        processor: String,
        queue: String,
        args: Value,
        max_retries: MaxRetries,
        backoff_strategy: Backoff,
    ) -> Self {
        JobInfo {
            id: None,
            processor,
            queue,
            status: JobStatus::Pending,
            args,
            retry_count: 0,
            max_retries,
            next_queue: None,
            backoff_strategy,
            updated_at: Utc::now(),
        }
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

    pub(crate) fn id(&self) -> Option<usize> {
        self.id.clone()
    }

    pub(crate) fn set_id(&mut self, id: usize) {
        if self.id.is_none() {
            self.id = Some(id);
        }
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

    pub(crate) fn is_in_queue(&self, queue: &str) -> bool {
        self.queue == queue
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
