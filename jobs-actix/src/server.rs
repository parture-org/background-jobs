use crate::{
    storage::{ActixStorage, StorageWrapper},
    worker::Worker,
};
use actix::clock::{interval_at, Duration, Instant};
use anyhow::Error;
use background_jobs_core::{NewJobInfo, ReturnJobInfo, Stats, Storage};
use log::{error, trace};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct ServerCache {
    cache: Arc<Mutex<HashMap<String, VecDeque<Box<dyn Worker + Send>>>>>,
}

/// The server Actor
///
/// This server guards access to Thee storage, and keeps a list of workers that are waiting for
/// jobs to process
#[derive(Clone)]
pub(crate) struct Server {
    storage: Arc<dyn ActixStorage + Send + Sync>,
    cache: ServerCache,
}

impl Server {
    /// Create a new Server from a compatible storage implementation
    pub(crate) fn new<S>(storage: S) -> Self
    where
        S: Storage + Sync + 'static,
    {
        let server = Server {
            storage: Arc::new(StorageWrapper(storage)),
            cache: ServerCache::new(),
        };

        let server2 = server.clone();
        actix::spawn(async move {
            let mut interval = interval_at(Instant::now(), Duration::from_secs(1));

            loop {
                interval.tick().await;
                if let Err(e) = server.check_db().await {
                    error!("Error while checking database for new jobs, {}", e);
                }
            }
        });

        server2
    }

    async fn check_db(&self) -> Result<(), Error> {
        trace!("Checking db for ready jobs");
        for queue in self.cache.keys().await {
            'worker_loop: while let Some(worker) = self.cache.pop(queue.clone()).await {
                if !self.try_turning(queue.clone(), worker).await? {
                    break 'worker_loop;
                }
            }
            trace!("Finished job lookups for queue {}", queue);
        }

        Ok(())
    }

    pub(crate) async fn new_job(&self, job: NewJobInfo) -> Result<(), Error> {
        let queue = job.queue().to_owned();
        let ready = job.is_ready();
        self.storage.new_job(job).await?;

        if !ready {
            trace!("New job is not ready for processing yet, returning");
            return Ok(());
        }

        if let Some(worker) = self.cache.pop(queue.clone()).await {
            self.try_turning(queue, worker).await?;
        }

        Ok(())
    }

    pub(crate) async fn request_job(
        &self,
        worker: Box<dyn Worker + Send + 'static>,
    ) -> Result<(), Error> {
        trace!("Worker {} requested job", worker.id());

        self.try_turning(worker.queue().to_owned(), worker).await?;

        Ok(())
    }

    async fn try_turning(
        &self,
        queue: String,
        worker: Box<dyn Worker + Send + 'static>,
    ) -> Result<bool, Error> {
        trace!("Trying to find job for worker {}", worker.id());
        if let Ok(Some(job)) = self.storage.request_job(&queue, worker.id()).await {
            if let Err(job) = worker.process_job(job).await {
                error!("Worker has hung up");
                self.storage.return_job(job.unexecuted()).await?
            }
        } else {
            trace!("No job exists, returning worker {}", worker.id());
            self.cache.push(queue.clone(), worker).await;
            return Ok(false);
        }

        Ok(true)
    }

    pub(crate) async fn return_job(&self, job: ReturnJobInfo) -> Result<(), Error> {
        Ok(self.storage.return_job(job).await?)
    }

    pub(crate) async fn get_stats(&self) -> Result<Stats, Error> {
        Ok(self.storage.get_stats().await?)
    }
}

impl ServerCache {
    fn new() -> Self {
        ServerCache {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn keys(&self) -> Vec<String> {
        let cache = self.cache.lock().await;

        cache.keys().cloned().collect()
    }

    async fn push(&self, queue: String, worker: Box<dyn Worker + Send>) {
        let mut cache = self.cache.lock().await;

        let entry = cache.entry(queue).or_insert(VecDeque::new());
        entry.push_back(worker);
    }

    async fn pop(&self, queue: String) -> Option<Box<dyn Worker + Send>> {
        let mut cache = self.cache.lock().await;

        let mut vec_deque = cache.remove(&queue)?;
        let item = vec_deque.pop_front()?;

        if !vec_deque.is_empty() {
            cache.insert(queue, vec_deque);
        }

        Some(item)
    }
}
