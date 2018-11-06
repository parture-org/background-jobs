use std::{
    path::PathBuf,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

use crate::JobInfo;
use kv::{bincode::Bincode, Bucket, Config, CursorOp, Error, Manager, Serde, Store, Txn, ValueBuf};
use lmdb::Error as LmdbError;

/// Set the status of a job when storing it
pub enum JobStatus {
    /// Job should be queued
    Pending,

    /// Job is running
    Running,

    /// Job has failed
    Failed,

    /// Job has finished
    Finished,
}

#[derive(Clone)]
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

        let new_id = self.with_lock(&store, &bucket, "id-lock", |txn| {
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

            Ok(new_id)
        })?;

        Ok(new_id)
    }

    pub fn dequeue_job(&self) -> Result<Option<JobInfo>, Error> {
        let store = self.store.write()?;

        let job_bucket =
            store.bucket::<&[u8], ValueBuf<Bincode<JobInfo>>>(Some(Storage::job_queue()))?;

        let txn = store.read_txn()?;

        let maybe_job = {
            let cursor = txn.read_cursor(&job_bucket)?;
            match cursor.get(None, CursorOp::Current) {
                Ok((k, v)) => {
                    let v = v.inner()?.to_serde();
                    Some((k, v))
                }
                Err(e) => match e {
                    Error::NotFound => None,
                    e => return Err(e),
                },
            }
        };

        let lock_bucket =
            store.bucket::<&str, ValueBuf<Bincode<usize>>>(Some(Storage::job_lock()))?;

        let result = if let Some((maybe_key, job)) = maybe_job {
            if let Some(key) = maybe_key {
                let job_key = std::str::from_utf8(&key).unwrap();

                self.with_lock(&store, &lock_bucket, job_key, move |_| {
                    self.run_job(job_key.parse().unwrap())
                })
                .map(|_| Some(job))
            } else {
                warn!("Didn't get key from cursor");
                Ok(None)
            }
        } else {
            info!("No jobs queued");
            Ok(None)
        };

        txn.commit()?;

        result
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
            JobStatus::Finished => self.finish_job(job_id)?,
        }

        Ok(())
    }

    fn queue_job(&self, id: usize) -> Result<(), Error> {
        self.add_job_to(id, Storage::job_queue())?;
        self.delete_job_from(id, Storage::job_failed())?;
        self.delete_job_from(id, Storage::job_running())?;
        self.delete_job_from(id, Storage::job_finished())?;

        Ok(())
    }

    fn fail_job(&self, id: usize) -> Result<(), Error> {
        self.add_job_to(id, Storage::job_failed())?;
        self.delete_job_from(id, Storage::job_queue())?;
        self.delete_job_from(id, Storage::job_running())?;
        self.delete_job_from(id, Storage::job_finished())?;

        Ok(())
    }

    fn run_job(&self, id: usize) -> Result<(), Error> {
        self.add_job_to(id, Storage::job_running())?;
        self.delete_job_from(id, Storage::job_queue())?;
        self.delete_job_from(id, Storage::job_failed())?;
        self.delete_job_from(id, Storage::job_finished())?;

        Ok(())
    }

    fn finish_job(&self, id: usize) -> Result<(), Error> {
        self.add_job_to(id, Storage::job_finished())?;
        self.delete_job_from(id, Storage::job_running())?;
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
        )?;
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

    // In all likelihood, this function is not necessary
    //
    // But in the event of multiple processes running on the same machine, it is good to have some
    // way to make sure they don't step on eachother's toes
    fn with_lock<F, T>(
        &self,
        store: &RwLockWriteGuard<Store>,
        bucket: &Bucket<&str, ValueBuf<Bincode<usize>>>,
        lock_key: &str,
        callback: F,
    ) -> Result<T, Error>
    where
        F: Fn(&mut Txn) -> Result<T, Error>,
    {
        let mut txn = store.write_txn()?;
        let mut other_runner_id = 0;

        loop {
            let lock_value = Bincode::to_value_buf(self.runner_id)?;

            let mut inner_txn = txn.txn()?;
            let res = inner_txn.set_no_overwrite(bucket, lock_key, lock_value);
            inner_txn.commit()?;

            match res {
                Ok(_) => break,
                Err(e) => {
                    let inner_txn = txn.txn()?;
                    let res = inner_txn.get(bucket, lock_key);
                    inner_txn.commit()?;

                    match res {
                        Ok(other_id) => {
                            let other_id = other_id.inner()?.to_serde();

                            if other_runner_id != other_id {
                                other_runner_id = other_id;
                                info!("Lock held by runner {}", other_id);
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

        let item = callback(&mut txn)?;

        txn.del(bucket, lock_key)?;
        txn.commit()?;

        Ok(item)
    }

    fn buckets() -> [&'static str; 7] {
        [
            Storage::id_store(),
            Storage::job_store(),
            Storage::job_queue(),
            Storage::job_failed(),
            Storage::job_running(),
            Storage::job_lock(),
            Storage::job_finished(),
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

    fn job_finished() -> &'static str {
        "job-finished"
    }

    fn job_lock() -> &'static str {
        "job-lock"
    }
}
