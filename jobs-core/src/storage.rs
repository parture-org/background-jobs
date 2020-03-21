use chrono::offset::Utc;
use log::info;
use std::error::Error;

use crate::{JobInfo, NewJobInfo, ReturnJobInfo, Stats};

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
    async fn generate_id(&self) -> Result<u64, Self::Error>;

    /// This method should store the supplied job
    ///
    /// The supplied job _may already be present_. The implementation should overwrite the stored
    /// job with the new job so that future calls to `fetch_job` return the new one.
    async fn save_job(&self, job: JobInfo) -> Result<(), Self::Error>;

    /// This method should return the job with the given ID regardless of what state the job is in.
    async fn fetch_job(&self, id: u64) -> Result<Option<JobInfo>, Self::Error>;

    /// This should fetch a job ready to be processed from the queue
    ///
    /// If a job is not ready, is currently running, or is not in the requested queue, this method
    /// should not return it. If no jobs meet these criteria, this method should return Ok(None)
    async fn fetch_job_from_queue(&self, queue: &str) -> Result<Option<JobInfo>, Self::Error>;

    /// This method tells the storage mechanism to mark the given job as being in the provided
    /// queue
    async fn queue_job(&self, queue: &str, id: u64) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to mark a given job as running
    async fn run_job(&self, id: u64, runner_id: u64) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to remove the job
    ///
    /// This happens when a job has been completed or has failed too many times
    async fn delete_job(&self, id: u64) -> Result<(), Self::Error>;

    /// This method returns the current statistics, or Stats::default() if none exists.
    async fn get_stats(&self) -> Result<Stats, Self::Error>;

    /// This method fetches the existing statistics or Stats::default(), and stores the result of
    /// calling `update_stats` on it.
    async fn update_stats<F>(&self, f: F) -> Result<(), Self::Error>
    where
        F: Fn(Stats) -> Stats + Send + 'static;

    /// Generate a new job based on the provided NewJobInfo
    async fn new_job(&self, job: NewJobInfo) -> Result<u64, Self::Error> {
        let id = self.generate_id().await?;

        let job = job.with_id(id);

        let queue = job.queue().to_owned();
        self.save_job(job).await?;
        self.queue_job(&queue, id).await?;
        self.update_stats(Stats::new_job).await?;

        Ok(id)
    }

    /// Fetch a job that is ready to be executed, marking it as running
    async fn request_job(
        &self,
        queue: &str,
        runner_id: u64,
    ) -> Result<Option<JobInfo>, Self::Error> {
        match self.fetch_job_from_queue(queue).await? {
            Some(mut job) => {
                let now = Utc::now();
                if job.is_pending(now) && job.is_ready(now) && job.is_in_queue(queue) {
                    job.run();
                    self.run_job(job.id(), runner_id).await?;
                    self.save_job(job.clone()).await?;
                    self.update_stats(Stats::run_job).await?;

                    Ok(Some(job))
                } else {
                    info!(
                        "Not fetching job {}, it is not ready for processing",
                        job.id()
                    );
                    self.queue_job(job.queue(), job.id()).await?;
                    Ok(None)
                }
            }
            None => Ok(None),
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
                    self.queue_job(job.queue(), id).await?;
                    self.save_job(job).await?;
                    self.update_stats(Stats::retry_job).await
                } else {
                    info!("Job {} failed permanently", id);
                    self.delete_job(id).await?;
                    self.update_stats(Stats::fail_job).await
                }
            } else {
                Ok(())
            }
        } else if result.is_missing_processor() || result.is_unexecuted() {
            if let Some(mut job) = self.fetch_job(id).await? {
                job.pending();
                self.queue_job(job.queue(), id).await?;
                self.save_job(job).await?;
                self.update_stats(Stats::retry_job).await
            } else {
                Ok(())
            }
        } else {
            self.delete_job(id).await?;
            self.update_stats(Stats::complete_job).await
        }
    }
}

/// A default, in-memory implementation of a storage mechanism
pub mod memory_storage {
    use super::{JobInfo, Stats};
    use futures::lock::Mutex;
    use std::{collections::HashMap, convert::Infallible, sync::Arc};

    #[derive(Clone)]
    /// An In-Memory store for jobs
    pub struct Storage {
        inner: Arc<Mutex<Inner>>,
    }

    #[derive(Clone)]
    struct Inner {
        count: u64,
        jobs: HashMap<u64, JobInfo>,
        queues: HashMap<u64, String>,
        worker_ids: HashMap<u64, u64>,
        worker_ids_inverse: HashMap<u64, u64>,
        stats: Stats,
    }

    impl Storage {
        /// Create a new, empty job store
        pub fn new() -> Self {
            Storage {
                inner: Arc::new(Mutex::new(Inner {
                    count: 0,
                    jobs: HashMap::new(),
                    queues: HashMap::new(),
                    worker_ids: HashMap::new(),
                    worker_ids_inverse: HashMap::new(),
                    stats: Stats::default(),
                })),
            }
        }
    }

    #[async_trait::async_trait]
    impl super::Storage for Storage {
        type Error = Infallible;

        async fn generate_id(&self) -> Result<u64, Self::Error> {
            let mut inner = self.inner.lock().await;
            let id = inner.count;
            inner.count = inner.count.wrapping_add(1);
            Ok(id)
        }

        async fn save_job(&self, job: JobInfo) -> Result<(), Self::Error> {
            self.inner.lock().await.jobs.insert(job.id(), job);

            Ok(())
        }

        async fn fetch_job(&self, id: u64) -> Result<Option<JobInfo>, Self::Error> {
            let j = self.inner.lock().await.jobs.get(&id).map(|j| j.clone());

            Ok(j)
        }

        async fn fetch_job_from_queue(&self, queue: &str) -> Result<Option<JobInfo>, Self::Error> {
            let mut inner = self.inner.lock().await;

            let j = inner
                .queues
                .iter()
                .filter_map(|(k, v)| {
                    if v == queue {
                        inner.jobs.get(k).map(|j| j.clone())
                    } else {
                        None
                    }
                })
                .next();

            if let Some(ref j) = j {
                inner.queues.remove(&j.id());
            }

            Ok(j)
        }

        async fn queue_job(&self, queue: &str, id: u64) -> Result<(), Self::Error> {
            self.inner.lock().await.queues.insert(id, queue.to_owned());
            Ok(())
        }

        async fn run_job(&self, id: u64, worker_id: u64) -> Result<(), Self::Error> {
            let mut inner = self.inner.lock().await;

            inner.worker_ids.insert(id, worker_id);
            inner.worker_ids_inverse.insert(worker_id, id);
            Ok(())
        }

        async fn delete_job(&self, id: u64) -> Result<(), Self::Error> {
            let mut inner = self.inner.lock().await;
            inner.jobs.remove(&id);
            inner.queues.remove(&id);
            if let Some(worker_id) = inner.worker_ids.remove(&id) {
                inner.worker_ids_inverse.remove(&worker_id);
            }
            Ok(())
        }

        async fn get_stats(&self) -> Result<Stats, Self::Error> {
            Ok(self.inner.lock().await.stats.clone())
        }

        async fn update_stats<F>(&self, f: F) -> Result<(), Self::Error>
        where
            F: Fn(Stats) -> Stats + Send,
        {
            let mut inner = self.inner.lock().await;

            inner.stats = (f)(inner.stats.clone());
            Ok(())
        }
    }
}
