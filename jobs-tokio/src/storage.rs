use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use jobs_core::JobInfo;
use kv::{bincode::Bincode, Config, Error, Manager, Serde, Store, ValueBuf};
use lmdb::Error as LmdbError;

/// Set the status of a job when storing it
pub enum JobStatus {
    /// Job should be queued
    Pending,

    /// Job is running
    Running,

    /// Job has failed
    Failed,
}

pub struct Storage {
    runner_id: usize,
    store: Arc<RwLock<Store>>,
}

impl Storage {
    pub fn new(runner_id: usize, store: Arc<RwLock<Store>>) -> Self {
        Storage { runner_id, store }
    }

    pub fn init(runner_id: usize, path: PathBuf) -> Result<Self, Error> {
        let mut manager = Manager::new();
        let mut cfg = Config::default(path);

        // Create our buckets
        for bucket in Storage::buckets().iter() {
            cfg.bucket(bucket, None);
        }

        let handle = manager.open(cfg)?;

        Ok(Storage::new(runner_id, handle))
    }

    pub fn get_new_id(&self) -> Result<usize, Error> {
        let store = self.store.write()?;

        let bucket = store.bucket::<&str, ValueBuf<Bincode<usize>>>(Some(Storage::id_store()))?;

        let mut txn = store.write_txn()?;

        let mut other_runner_id = 0;

        loop {
            let lock_value = Bincode::to_value_buf(self.runner_id)?;
            match txn.set_no_overwrite(&bucket, "id-lock", lock_value) {
                Ok(_) => break,
                Err(e) => {
                    match txn.get(&bucket, "id-lock") {
                        Ok(other_id) => {
                            let other_id = other_id.inner()?.to_serde();

                            if other_runner_id != other_id {
                                other_runner_id = other_id;
                                info!("Id lock held by runner {}", other_id);
                            }
                        }
                        Err(e) => match e {
                            Error::NotFound => continue,
                            e => return Err(e),
                        },
                    }

                    match e {
                        Error::LMDB(lmdb) => match lmdb {
                            LmdbError::KeyExist => continue,
                            e => return Err(Error::LMDB(e)),
                        },
                        e => return Err(e),
                    }
                }
            }
        }

        let id = match txn.get(&bucket, "current-id") {
            Ok(id) => id.inner()?.to_serde(),
            Err(e) => match e {
                Error::NotFound => 1,
                _ => return Err(e),
            },
        };

        let new_id = id + 1;

        let new_id_value = Bincode::to_value_buf(new_id)?;
        txn.set(&bucket, "current-id", new_id_value)?;

        txn.del(&bucket, "id-lock")?;
        txn.commit()?;

        Ok(new_id)
    }

    pub fn store_job(&self, mut job: JobInfo, status: JobStatus) -> Result<(), Error> {
        let job_id = match job.id() {
            Some(id) => id,
            None => {
                let id = self.get_new_id()?;
                job.set_id(id);
                id
            }
        };

        let job_value = Bincode::to_value_buf(job)?;

        {
            let store = self.store.write()?;
            let bucket =
                store.bucket::<&str, ValueBuf<Bincode<JobInfo>>>(Some(Storage::job_store()))?;

            let mut txn = store.write_txn()?;
            txn.set(&bucket, &job_id.to_string(), job_value)?;
            txn.commit()?;
        }

        match status {
            JobStatus::Pending => self.queue_job(job_id)?,
            JobStatus::Running => self.run_job(job_id)?,
            JobStatus::Failed => self.fail_job(job_id)?,
        }

        Ok(())
    }

    fn queue_job(&self, id: usize) -> Result<(), Error> {
        self.add_job_to(id, Storage::job_queue())?;
        self.delete_job_from(id, Storage::job_failed())?;
        self.delete_job_from(id, Storage::job_running())?;

        Ok(())
    }

    fn fail_job(&self, id: usize) -> Result<(), Error> {
        self.add_job_to(id, Storage::job_failed())?;
        self.delete_job_from(id, Storage::job_queue())?;
        self.delete_job_from(id, Storage::job_running())?;

        Ok(())
    }

    fn run_job(&self, id: usize) -> Result<(), Error> {
        self.add_job_to(id, Storage::job_running())?;
        self.delete_job_from(id, Storage::job_queue())?;
        self.delete_job_from(id, Storage::job_failed())?;

        Ok(())
    }

    fn add_job_to(&self, id: usize, bucket_name: &str) -> Result<(), Error> {
        let store = self.store.write()?;
        let bucket = store.bucket::<&str, ValueBuf<Bincode<usize>>>(Some(bucket_name))?;

        let mut txn = store.write_txn()?;
        txn.set(
            &bucket,
            &id.to_string(),
            Bincode::to_value_buf(self.runner_id)?,
        );
        txn.commit()?;

        Ok(())
    }

    fn delete_job_from(&self, id: usize, bucket_name: &str) -> Result<(), Error> {
        let store = self.store.write()?;
        let bucket = store.bucket::<&str, ValueBuf<Bincode<usize>>>(Some(bucket_name))?;

        let mut txn = store.write_txn()?;

        match txn.del(&bucket, &id.to_string()) {
            Ok(_) => (),
            Err(e) => match e {
                Error::NotFound => (),
                e => return Err(e),
            },
        }

        txn.commit()?;

        Ok(())
    }

    fn buckets() -> [&'static str; 5] {
        [
            Storage::id_store(),
            Storage::job_store(),
            Storage::job_queue(),
            Storage::job_failed(),
            Storage::job_running(),
        ]
    }

    fn id_store() -> &'static str {
        "id-store"
    }

    fn job_store() -> &'static str {
        "job-store"
    }

    fn job_queue() -> &'static str {
        "job-queue"
    }

    fn job_failed() -> &'static str {
        "job-failed"
    }

    fn job_running() -> &'static str {
        "job-running"
    }
}
