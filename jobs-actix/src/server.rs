use crate::{
    storage::{ActixStorage, StorageWrapper},
    worker::Worker,
};
use anyhow::Error;
use background_jobs_core::{NewJobInfo, ReturnJobInfo, Stats, Storage};
use std::sync::Arc;
use tracing::{error, trace};

/// The server Actor
///
/// This server guards access to Thee storage, and keeps a list of workers that are waiting for
/// jobs to process
#[derive(Clone)]
pub(crate) struct Server {
    storage: Arc<dyn ActixStorage + Send + Sync>,
}

impl Server {
    /// Create a new Server from a compatible storage implementation
    pub(crate) fn new<S>(storage: S) -> Self
    where
        S: Storage + Sync + 'static,
    {
        Server {
            storage: Arc::new(StorageWrapper(storage)),
        }
    }

    pub(crate) async fn new_job(&self, job: NewJobInfo) -> Result<(), Error> {
        let ready = job.is_ready();
        self.storage.new_job(job).await?;

        if !ready {
            trace!("New job is not ready for processing yet, returning");
            return Ok(());
        }

        Ok(())
    }

    pub(crate) async fn request_job(
        &self,
        worker: Box<dyn Worker + Send + Sync + 'static>,
    ) -> Result<(), Error> {
        trace!("Worker {} requested job", worker.id());
        let job = self
            .storage
            .request_job(worker.queue(), worker.id())
            .await?;

        if let Err(job) = worker.process(job).await {
            error!("Worker {} has hung up", worker.id());
            self.storage.return_job(job.unexecuted()).await?;
        }

        Ok(())
    }

    pub(crate) async fn return_job(&self, job: ReturnJobInfo) -> Result<(), Error> {
        self.storage.return_job(job).await
    }

    pub(crate) async fn get_stats(&self) -> Result<Stats, Error> {
        self.storage.get_stats().await
    }
}
