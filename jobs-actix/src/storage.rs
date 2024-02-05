use background_jobs_core::{BoxError, JobInfo, NewJobInfo, ReturnJobInfo, Storage};
use uuid::Uuid;

#[async_trait::async_trait]
pub(crate) trait ActixStorage {
    async fn push(&self, job: NewJobInfo) -> Result<Uuid, BoxError>;

    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, BoxError>;

    async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<(), BoxError>;

    async fn complete(&self, ret: ReturnJobInfo) -> Result<(), BoxError>;
}

pub(crate) struct StorageWrapper<S>(pub(crate) S)
where
    S: Storage + Send + Sync,
    S::Error: Send + Sync + 'static;

#[async_trait::async_trait]
impl<S> ActixStorage for StorageWrapper<S>
where
    S: Storage + Send + Sync,
    S::Error: Send + Sync + 'static,
{
    async fn push(&self, job: NewJobInfo) -> Result<Uuid, BoxError> {
        Ok(self.0.push(job).await?)
    }

    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, BoxError> {
        Ok(self.0.pop(queue, runner_id).await?)
    }

    async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<(), BoxError> {
        Ok(self.0.heartbeat(job_id, runner_id).await?)
    }

    async fn complete(&self, ret: ReturnJobInfo) -> Result<(), BoxError> {
        self.0.complete(ret).await?;

        Ok(())
    }
}
