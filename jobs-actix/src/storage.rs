use anyhow::Error;
use background_jobs_core::{JobInfo, NewJobInfo, ReturnJobInfo, Stats, Storage};

#[async_trait::async_trait]
pub(crate) trait ActixStorage {
    async fn new_job(&self, job: NewJobInfo) -> Result<u64, Error>;

    async fn request_job(&self, queue: &str, runner_id: u64) -> Result<Option<JobInfo>, Error>;

    async fn return_job(&self, ret: ReturnJobInfo) -> Result<(), Error>;

    async fn get_stats(&self) -> Result<Stats, Error>;
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
    async fn new_job(&self, job: NewJobInfo) -> Result<u64, Error> {
        Ok(self.0.new_job(job).await?)
    }

    async fn request_job(&self, queue: &str, runner_id: u64) -> Result<Option<JobInfo>, Error> {
        Ok(self.0.request_job(queue, runner_id).await?)
    }

    async fn return_job(&self, ret: ReturnJobInfo) -> Result<(), Error> {
        Ok(self.0.return_job(ret).await?)
    }

    async fn get_stats(&self) -> Result<Stats, Error> {
        Ok(self.0.get_stats().await?)
    }
}
