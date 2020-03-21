use crate::{
    storage::{ActixStorage, StorageWrapper},
    worker::Worker,
};
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
        Server {
            storage: Arc::new(StorageWrapper(storage)),
            cache: ServerCache::new(),
        }
    }

    pub(crate) async fn new_job(&self, job: NewJobInfo) -> Result<(), Error> {
        let queue = job.queue().to_owned();
        let ready = job.is_ready();
        self.storage.new_job(job).await?;

        if !ready {
            return Ok(());
        }

        if let Some(worker) = self.cache.pop(queue.clone()).await {
            if let Ok(Some(job)) = self.storage.request_job(&queue, worker.id()).await {
                if let Err(job) = worker.process_job(job).await {
                    error!("Worker has hung up");
                    self.storage.return_job(job.unexecuted()).await?;
                }
            } else {
                self.cache.push(queue, worker).await;
            }
        }

        Ok(())
    }

    pub(crate) async fn request_job(
        &self,
        worker: Box<dyn Worker + Send + 'static>,
    ) -> Result<(), Error> {
        trace!("Worker {} requested job", worker.id());

        if let Ok(Some(job)) = self.storage.request_job(worker.queue(), worker.id()).await {
            if let Err(job) = worker.process_job(job).await {
                error!("Worker has hung up");
                self.storage.return_job(job.unexecuted()).await?;
            }
        } else {
            trace!(
                "storing worker {} for queue {}",
                worker.id(),
                worker.queue()
            );
            self.cache.push(worker.queue().to_owned(), worker).await;
        }

        Ok(())
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
