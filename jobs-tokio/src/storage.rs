use std::{ops::Deref, sync::Arc};

use background_jobs_core::{BoxError, JobInfo, NewJobInfo, ReturnJobInfo, Storage as StorageTrait};
use uuid::Uuid;

#[async_trait::async_trait]
pub trait TokioStorage: Send + Sync {
    async fn push(&self, job: NewJobInfo) -> Result<Uuid, BoxError>;

    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, BoxError>;

    async fn heartbeat(&self, job_id: Uuid, worker_id: Uuid) -> Result<(), BoxError>;

    async fn complete(&self, return_job_info: ReturnJobInfo) -> Result<(), BoxError>;
}

#[derive(Clone)]
pub(crate) struct Storage {
    inner: Arc<dyn TokioStorage>,
}

struct StorageWrapper<S>(S);

#[async_trait::async_trait]
impl<S> TokioStorage for StorageWrapper<S>
where
    S: StorageTrait + Send + Sync + 'static,
{
    async fn push(&self, job: NewJobInfo) -> Result<Uuid, BoxError> {
        self.0.push(job).await.map_err(From::from)
    }

    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, BoxError> {
        self.0.pop(queue, runner_id).await.map_err(From::from)
    }

    async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<(), BoxError> {
        self.0
            .heartbeat(job_id, runner_id)
            .await
            .map_err(From::from)
    }

    async fn complete(&self, return_job_info: ReturnJobInfo) -> Result<(), BoxError> {
        self.0
            .complete(return_job_info)
            .await
            .map(|_| ())
            .map_err(From::from)
    }
}

impl Storage {
    pub(crate) fn new<S>(storage: S) -> Self
    where
        S: StorageTrait + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(StorageWrapper(storage)),
        }
    }
}

impl Deref for Storage {
    type Target = dyn TokioStorage + 'static;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}
