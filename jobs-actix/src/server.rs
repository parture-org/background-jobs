use crate::storage::{ActixStorage, StorageWrapper};
use anyhow::Error;
use background_jobs_core::{JobInfo, NewJobInfo, ReturnJobInfo, Storage};
use std::sync::Arc;
use tracing::trace;
use uuid::Uuid;

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
        self.storage.new_job(job).await.map(|_| ())
    }

    pub(crate) async fn request_job(
        &self,
        worker_id: Uuid,
        worker_queue: &str,
    ) -> Result<JobInfo, Error> {
        trace!("Worker {} requested job", worker_id);
        self.storage.request_job(worker_queue, worker_id).await
    }

    pub(crate) async fn return_job(&self, job: ReturnJobInfo) -> Result<(), Error> {
        self.storage.return_job(job).await
    }
}
