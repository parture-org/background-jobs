use anyhow::Error;
use background_jobs_core::{JobInfo, NewJobInfo, ReturnJobInfo, Storage};
use uuid::Uuid;

#[async_trait::async_trait]
pub(crate) trait ActixStorage {
    async fn push(&self, job: NewJobInfo) -> Result<Uuid, Error>;

    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, Error>;

    async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<(), Error>;

    async fn complete(&self, ret: ReturnJobInfo) -> Result<(), Error>;
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
    async fn push(&self, job: NewJobInfo) -> Result<Uuid, Error> {
        Ok(self.0.push(job).await?)
    }

    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo, Error> {
        Ok(self.0.pop(queue, runner_id).await?)
    }

    async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<(), Error> {
        Ok(self.0.heartbeat(job_id, runner_id).await?)
    }

    async fn complete(&self, ret: ReturnJobInfo) -> Result<(), Error> {
        self.0.complete(ret).await?;

        Ok(())
    }
}
