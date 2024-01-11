use crate::{JobInfo, NewJobInfo, ReturnJobInfo};
use std::error::Error;
use uuid::Uuid;

/// Define a storage backend for jobs
///
/// This crate provides a default implementation in the `memory_storage` module, which is backed by
/// HashMaps and uses counting to assign IDs. If jobs must be persistent across application
/// restarts, look into the [`sled-backed`](https://github.com/spacejam/sled) implementation from
/// the `background-jobs-sled-storage` crate.
#[async_trait::async_trait]
pub trait Storage: Clone + Send {
    /// The error type used by the storage mechansim.
    type Error: Error + Send + Sync;

    /// Get the JobInfo for a given job ID
    async fn info(&self, job_id: Uuid) -> Result<Option<JobInfo>, Self::Error>;

    /// push a job into the queue
    async fn push(&self, job: NewJobInfo) -> Result<Uuid, Self::Error>;

    /// pop a job from the provided queue
    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, Self::Error>;

    /// mark a job as being actively worked on
    async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<(), Self::Error>;

    /// "Return" a job to the database, marking it for retry if needed
    ///
    /// returns `true` if the job has not been requeued
    async fn complete(&self, return_job_info: ReturnJobInfo) -> Result<bool, Self::Error>;
}

/// A default, in-memory implementation of a storage mechanism
pub mod memory_storage {
    use crate::{JobInfo, JobResult, NewJobInfo, ReturnJobInfo};

    use event_listener::{Event, EventListener};
    use std::{
        collections::{BTreeMap, HashMap},
        convert::Infallible,
        future::Future,
        ops::Bound,
        pin::Pin,
        sync::Arc,
        sync::Mutex,
        time::Duration,
    };
    use time::OffsetDateTime;
    use uuid::{NoContext, Timestamp, Uuid};

    /// Allows memory storage to set timeouts for when to retry checking a queue for a job
    #[async_trait::async_trait]
    pub trait Timer {
        /// Race a future against the clock, returning an empty tuple if the clock wins
        async fn timeout<F>(&self, duration: Duration, future: F) -> Result<F::Output, ()>
        where
            F: Future + Send + Sync;
    }

    #[derive(Clone)]
    /// An In-Memory store for jobs
    pub struct Storage<T> {
        timer: T,
        inner: Arc<Mutex<Inner>>,
    }

    type OrderedKey = (String, Uuid);
    type JobState = Option<(Uuid, OffsetDateTime)>;
    type JobMeta = (Uuid, time::Duration, JobState);

    struct Inner {
        queues: HashMap<String, Event>,
        jobs: HashMap<Uuid, JobInfo>,
        queue_jobs: BTreeMap<OrderedKey, JobMeta>,
    }

    impl<T: Timer> Storage<T> {
        /// Create a new, empty job store
        pub fn new(timer: T) -> Self {
            Storage {
                inner: Arc::new(Mutex::new(Inner {
                    queues: HashMap::new(),
                    jobs: HashMap::new(),
                    queue_jobs: BTreeMap::new(),
                })),
                timer,
            }
        }

        fn get(&self, job_id: Uuid) -> Option<JobInfo> {
            self.inner.lock().unwrap().jobs.get(&job_id).cloned()
        }

        fn listener(&self, pop_queue: String) -> (Pin<Box<EventListener>>, Duration) {
            let lower_bound = Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0));
            let now = OffsetDateTime::now_utc();

            let mut inner = self.inner.lock().unwrap();

            let listener = inner.queues.entry(pop_queue.clone()).or_default().listen();

            let duration = inner
                .queue_jobs
                .range((
                    Bound::Excluded((pop_queue.clone(), lower_bound)),
                    Bound::Unbounded,
                ))
                .filter(|(_, (_, _, meta))| meta.is_none())
                .filter_map(|(_, (id, _, _))| inner.jobs.get(id))
                .take_while(|JobInfo { queue, .. }| queue.as_str() == pop_queue.as_str())
                .map(|JobInfo { next_queue, .. }| {
                    if *next_queue > now {
                        *next_queue - now
                    } else {
                        time::Duration::seconds(0)
                    }
                })
                .find_map(|duration| duration.try_into().ok());

            (listener, duration.unwrap_or(Duration::from_secs(10)))
        }

        fn try_pop(&self, queue: &str, runner_id: Uuid) -> Option<JobInfo> {
            let lower_bound = Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0));
            let upper_bound = Uuid::now_v7();
            let now = time::OffsetDateTime::now_utc();

            let mut inner = self.inner.lock().unwrap();

            let mut pop_job = None;

            for (_, (job_id, heartbeat_interval, job_meta)) in inner.queue_jobs.range_mut((
                Bound::Excluded((queue.to_string(), lower_bound)),
                Bound::Included((queue.to_string(), upper_bound)),
            )) {
                if job_meta.is_none()
                    || job_meta.is_some_and(|(_, h)| h + (5 * *heartbeat_interval) < now)
                {
                    *job_meta = Some((runner_id, now));
                    pop_job = Some(*job_id);
                    break;
                }
            }

            if let Some(job_id) = pop_job {
                return inner.jobs.get(&job_id).cloned();
            }

            None
        }

        fn set_heartbeat(&self, job_id: Uuid, runner_id: Uuid) {
            let lower_bound = Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0));
            let upper_bound = Uuid::now_v7();

            let mut inner = self.inner.lock().unwrap();

            let queue = if let Some(job) = inner.jobs.get(&job_id) {
                job.queue.clone()
            } else {
                return;
            };

            for (_, (found_job_id, _, found_job_meta)) in inner.queue_jobs.range_mut((
                Bound::Excluded((queue.clone(), lower_bound)),
                Bound::Included((queue, upper_bound)),
            )) {
                if *found_job_id == job_id {
                    *found_job_meta = Some((runner_id, OffsetDateTime::now_utc()));
                    return;
                }
            }
        }

        fn remove_job(&self, job_id: Uuid) -> Option<JobInfo> {
            let lower_bound = Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0));
            let upper_bound = Uuid::now_v7();

            let mut inner = self.inner.lock().unwrap();

            let job = inner.jobs.remove(&job_id)?;

            let mut key = None;

            for (found_key, (found_job_id, _, _)) in inner.queue_jobs.range_mut((
                Bound::Excluded((job.queue.clone(), lower_bound)),
                Bound::Included((job.queue.clone(), upper_bound)),
            )) {
                if *found_job_id == job_id {
                    key = Some(found_key.clone());
                    break;
                }
            }

            if let Some(key) = key {
                if inner.queue_jobs.remove(&key).is_none() {
                    tracing::warn!("failed to remove {key:?}");
                }
            }

            Some(job)
        }

        fn insert(&self, job: JobInfo) -> Uuid {
            let id = job.id;
            let queue = job.queue.clone();
            let queue_time_id = job.next_queue_id();
            let heartbeat_interval = job.heartbeat_interval;

            let mut inner = self.inner.lock().unwrap();

            inner.jobs.insert(id, job);

            inner.queue_jobs.insert(
                (queue.clone(), queue_time_id),
                (
                    id,
                    time::Duration::milliseconds(heartbeat_interval as _),
                    None,
                ),
            );

            inner.queues.entry(queue).or_default().notify(1);

            id
        }
    }

    #[async_trait::async_trait]
    impl<T: Timer + Send + Sync + Clone> super::Storage for Storage<T> {
        type Error = Infallible;

        #[tracing::instrument(skip(self))]
        async fn info(&self, job_id: Uuid) -> Result<Option<JobInfo>, Self::Error> {
            Ok(self.get(job_id))
        }

        /// push a job into the queue
        #[tracing::instrument(skip_all)]
        async fn push(&self, job: NewJobInfo) -> Result<Uuid, Self::Error> {
            Ok(self.insert(job.build()))
        }

        /// pop a job from the provided queue
        #[tracing::instrument(skip(self))]
        async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, Self::Error> {
            loop {
                let (listener, duration) = self.listener(queue.to_string());

                if let Some(job) = self.try_pop(queue, runner_id) {
                    return Ok(job);
                }

                match self.timer.timeout(duration, listener).await {
                    Ok(()) => {
                        // listener wakeup
                    }
                    Err(()) => {
                        // timeout
                    }
                }
            }
        }

        /// mark a job as being actively worked on
        #[tracing::instrument(skip(self))]
        async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<(), Self::Error> {
            self.set_heartbeat(job_id, runner_id);
            Ok(())
        }

        /// "Return" a job to the database, marking it for retry if needed
        #[tracing::instrument(skip(self))]
        async fn complete(
            &self,
            ReturnJobInfo { id, result }: ReturnJobInfo,
        ) -> Result<bool, Self::Error> {
            let mut job = if let Some(job) = self.remove_job(id) {
                job
            } else {
                return Ok(true);
            };

            match result {
                // successful jobs are removed
                JobResult::Success => Ok(true),
                // Unregistered or Unexecuted jobs are restored as-is
                JobResult::Unregistered | JobResult::Unexecuted => {
                    self.insert(job);
                    Ok(false)
                }
                // retryable failed jobs are restored
                JobResult::Failure if job.prepare_retry() => {
                    self.insert(job);
                    Ok(false)
                }
                // dead jobs are removed
                JobResult::Failure => Ok(true),
            }
        }
    }
}
