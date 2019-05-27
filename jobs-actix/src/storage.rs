use background_jobs_core::{JobInfo, NewJobInfo, ReturnJobInfo, Stats, Storage};
use failure::{Error, Fail};

pub(crate) trait ActixStorage {
    fn new_job(&mut self, job: NewJobInfo) -> Result<u64, Error>;

    fn request_job(&mut self, queue: &str, runner_id: u64) -> Result<Option<JobInfo>, Error>;

    fn return_job(&mut self, ret: ReturnJobInfo) -> Result<(), Error>;

    fn get_stats(&self) -> Result<Stats, Error>;
}

pub(crate) struct StorageWrapper<S, E>(pub(crate) S)
where
    S: Storage<Error = E>,
    E: Fail;

impl<S, E> ActixStorage for StorageWrapper<S, E>
where
    S: Storage<Error = E>,
    E: Fail,
{
    fn new_job(&mut self, job: NewJobInfo) -> Result<u64, Error> {
        self.0.new_job(job).map_err(Error::from)
    }

    fn request_job(&mut self, queue: &str, runner_id: u64) -> Result<Option<JobInfo>, Error> {
        self.0.request_job(queue, runner_id).map_err(Error::from)
    }

    fn return_job(&mut self, ret: ReturnJobInfo) -> Result<(), Error> {
        self.0.return_job(ret).map_err(Error::from)
    }

    fn get_stats(&self) -> Result<Stats, Error> {
        self.0.get_stats().map_err(Error::from)
    }
}
