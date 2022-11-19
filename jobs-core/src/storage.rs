use crate::{JobInfo, NewJobInfo, ReturnJobInfo};
use std::{error::Error, time::SystemTime};
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

    /// This method generates unique IDs for jobs
    async fn generate_id(&self) -> Result<Uuid, Self::Error>;

    /// This method should store the supplied job
    ///
    /// The supplied job _may already be present_. The implementation should overwrite the stored
    /// job with the new job so that future calls to `fetch_job` return the new one.
    async fn save_job(&self, job: JobInfo) -> Result<(), Self::Error>;

    /// This method should return the job with the given ID regardless of what state the job is in.
    async fn fetch_job(&self, id: Uuid) -> Result<Option<JobInfo>, Self::Error>;

    /// This should fetch a job ready to be processed from the queue
    ///
    /// If a job is not ready, is currently running, or is not in the requested queue, this method
    /// should not return it. If no jobs meet these criteria, this method wait until a job becomes available
    async fn fetch_job_from_queue(&self, queue: &str) -> Result<JobInfo, Self::Error>;

    /// This method tells the storage mechanism to mark the given job as being in the provided
    /// queue
    async fn queue_job(&self, queue: &str, id: Uuid) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to mark a given job as running
    async fn run_job(&self, id: Uuid, runner_id: Uuid) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to remove the job
    ///
    /// This happens when a job has been completed or has failed too many times
    async fn delete_job(&self, id: Uuid) -> Result<(), Self::Error>;

    /// Generate a new job based on the provided NewJobInfo
    async fn new_job(&self, job: NewJobInfo) -> Result<Uuid, Self::Error> {
        let id = self.generate_id().await?;

        let job = job.with_id(id);
        metrics::counter!("background-jobs.job.created", 1, "queue" => job.queue().to_string(), "name" => job.name().to_string());

        let queue = job.queue().to_owned();
        self.save_job(job).await?;
        self.queue_job(&queue, id).await?;

        Ok(id)
    }

    /// Fetch a job that is ready to be executed, marking it as running
    async fn request_job(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, Self::Error> {
        loop {
            let mut job = self.fetch_job_from_queue(queue).await?;

            let now = SystemTime::now();
            if job.is_pending(now) && job.is_ready(now) && job.is_in_queue(queue) {
                job.run();
                self.run_job(job.id(), runner_id).await?;
                self.save_job(job.clone()).await?;

                metrics::gauge!("background-jobs.job.running", 1.0, "queue" => job.queue().to_string(), "name" => job.name().to_string());

                return Ok(job);
            } else {
                tracing::warn!(
                    "Not fetching job {}, it is not ready for processing",
                    job.id()
                );
                self.queue_job(job.queue(), job.id()).await?;
            }
        }
    }

    /// "Return" a job to the database, marking it for retry if needed
    async fn return_job(
        &self,
        ReturnJobInfo { id, result }: ReturnJobInfo,
    ) -> Result<(), Self::Error> {
        if result.is_failure() {
            if let Some(mut job) = self.fetch_job(id).await? {
                if job.needs_retry() {
                    metrics::counter!("background-jobs.job.failed", 1, "queue" => job.queue().to_string(), "name" => job.name().to_string());
                    metrics::gauge!("background-jobs.job.running", -1.0, "queue" => job.queue().to_string(), "name" => job.name().to_string());

                    self.queue_job(job.queue(), id).await?;
                    self.save_job(job).await
                } else {
                    metrics::counter!("background-jobs.job.dead", 1, "queue" => job.queue().to_string(), "name" => job.name().to_string());
                    metrics::gauge!("background-jobs.job.running", -1.0, "queue" => job.queue().to_string(), "name" => job.name().to_string());

                    #[cfg(feature = "error-logging")]
                    tracing::warn!("Job {} failed permanently", id);

                    self.delete_job(id).await
                }
            } else {
                tracing::warn!("Returned non-existant job");
                metrics::counter!("background-jobs.job.missing", 1);
                Ok(())
            }
        } else if result.is_unregistered() || result.is_unexecuted() {
            if let Some(mut job) = self.fetch_job(id).await? {
                metrics::counter!("background-jobs.job.returned", 1, "queue" => job.queue().to_string(), "name" => job.name().to_string());
                metrics::gauge!("background-jobs.job.running", -1.0, "queue" => job.queue().to_string(), "name" => job.name().to_string());

                job.pending();
                self.queue_job(job.queue(), id).await?;
                self.save_job(job).await
            } else {
                tracing::warn!("Returned non-existant job");
                metrics::counter!("background-jobs.job.missing", 1);
                Ok(())
            }
        } else {
            if let Some(job) = self.fetch_job(id).await? {
                metrics::counter!("background-jobs.job.completed", 1, "queue" => job.queue().to_string(), "name" => job.name().to_string());
                metrics::gauge!("background-jobs.job.running", -1.0, "queue" => job.queue().to_string(), "name" => job.name().to_string());
            } else {
                tracing::warn!("Returned non-existant job");
                metrics::counter!("background-jobs.job.missing", 1);
            }

            self.delete_job(id).await
        }
    }
}

/// A default, in-memory implementation of a storage mechanism
pub mod memory_storage {
    use super::JobInfo;
    use event_listener::Event;
    use std::{
        collections::HashMap,
        convert::Infallible,
        sync::Arc,
        sync::Mutex,
        time::{Duration, SystemTime},
    };
    use uuid::Uuid;

    /// Allows memory storage to set timeouts for when to retry checking a queue for a job
    #[async_trait::async_trait]
    pub trait Timer {
        /// Race a future against the clock, returning an empty tuple if the clock wins
        async fn timeout<F>(&self, duration: Duration, future: F) -> Result<F::Output, ()>
        where
            F: std::future::Future + Send + Sync;
    }

    #[derive(Clone)]
    /// An In-Memory store for jobs
    pub struct Storage<T> {
        timer: T,
        inner: Arc<Mutex<Inner>>,
    }

    struct Inner {
        queues: HashMap<String, Event>,
        jobs: HashMap<Uuid, JobInfo>,
        job_queues: HashMap<Uuid, String>,
        worker_ids: HashMap<Uuid, Uuid>,
        worker_ids_inverse: HashMap<Uuid, Uuid>,
    }

    impl<T: Timer> Storage<T> {
        /// Create a new, empty job store
        pub fn new(timer: T) -> Self {
            Storage {
                inner: Arc::new(Mutex::new(Inner {
                    queues: HashMap::new(),
                    jobs: HashMap::new(),
                    job_queues: HashMap::new(),
                    worker_ids: HashMap::new(),
                    worker_ids_inverse: HashMap::new(),
                })),
                timer,
            }
        }
    }

    #[async_trait::async_trait]
    impl<T: Timer + Send + Sync + Clone> super::Storage for Storage<T> {
        type Error = Infallible;

        async fn generate_id(&self) -> Result<Uuid, Self::Error> {
            let uuid = loop {
                let uuid = Uuid::new_v4();
                if !self.inner.lock().unwrap().jobs.contains_key(&uuid) {
                    break uuid;
                }
            };

            Ok(uuid)
        }

        async fn save_job(&self, job: JobInfo) -> Result<(), Self::Error> {
            self.inner.lock().unwrap().jobs.insert(job.id(), job);

            Ok(())
        }

        async fn fetch_job(&self, id: Uuid) -> Result<Option<JobInfo>, Self::Error> {
            let j = self.inner.lock().unwrap().jobs.get(&id).cloned();

            Ok(j)
        }

        async fn fetch_job_from_queue(&self, queue: &str) -> Result<JobInfo, Self::Error> {
            loop {
                let listener = {
                    let mut inner = self.inner.lock().unwrap();
                    let now = SystemTime::now();

                    let j = inner.job_queues.iter().find_map(|(k, v)| {
                        if v == queue {
                            let job = inner.jobs.get(k)?;

                            if job.is_pending(now) && job.is_ready(now) && job.is_in_queue(queue) {
                                return Some(job.clone());
                            }
                        }

                        None
                    });

                    let duration = if let Some(j) = j {
                        if inner.job_queues.remove(&j.id()).is_some() {
                            return Ok(j);
                        } else {
                            continue;
                        }
                    } else {
                        inner.job_queues.iter().fold(
                            Duration::from_secs(5),
                            |duration, (id, v_queue)| {
                                if v_queue == queue {
                                    if let Some(job) = inner.jobs.get(id) {
                                        if let Some(ready_at) = job.next_queue() {
                                            let job_eta = ready_at
                                                .duration_since(now)
                                                .unwrap_or(Duration::from_secs(0));

                                            if job_eta < duration {
                                                return job_eta;
                                            }
                                        }
                                    }
                                }

                                duration
                            },
                        )
                    };

                    self.timer.timeout(
                        duration,
                        inner.queues.entry(queue.to_string()).or_default().listen(),
                    )
                };

                let _ = listener.await;
            }
        }

        async fn queue_job(&self, queue: &str, id: Uuid) -> Result<(), Self::Error> {
            let mut inner = self.inner.lock().unwrap();

            inner.job_queues.insert(id, queue.to_owned());

            inner.queues.entry(queue.to_string()).or_default().notify(1);

            Ok(())
        }

        async fn run_job(&self, id: Uuid, worker_id: Uuid) -> Result<(), Self::Error> {
            let mut inner = self.inner.lock().unwrap();

            inner.worker_ids.insert(id, worker_id);
            inner.worker_ids_inverse.insert(worker_id, id);
            Ok(())
        }

        async fn delete_job(&self, id: Uuid) -> Result<(), Self::Error> {
            let mut inner = self.inner.lock().unwrap();
            inner.jobs.remove(&id);
            inner.job_queues.remove(&id);
            if let Some(worker_id) = inner.worker_ids.remove(&id) {
                inner.worker_ids_inverse.remove(&worker_id);
            }
            Ok(())
        }
    }
}
