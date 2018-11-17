use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    str::Utf8Error,
    sync::{Arc, RwLock, RwLockWriteGuard},
};

use chrono::offset::Utc;
use kv::{json::Json, Bucket, Config, CursorOp, Error, Manager, Serde, Store, Txn, ValueBuf};
use lmdb::Error as LmdbError;

use crate::{JobInfo, JobStatus};

struct Buckets<'a> {
    queued: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    running: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    staged: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    failed: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    finished: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
}

impl<'a> Buckets<'a> {
    fn new(store: &'a RwLockWriteGuard<Store>) -> Result<Self, Error> {
        let b = Buckets {
            queued: store.bucket(Some(Storage::job_queue()))?,
            running: store.bucket(Some(Storage::job_running()))?,
            staged: store.bucket(Some(Storage::job_staged()))?,
            failed: store.bucket(Some(Storage::job_failed()))?,
            finished: store.bucket(Some(Storage::job_finished()))?,
        };

        Ok(b)
    }
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

        cfg.set_max_readers(18);

        // Create our buckets
        for bucket in Storage::buckets().iter() {
            cfg.bucket(bucket, None);
        }

        let handle = manager.open(cfg)?;

        Ok(Storage::new(runner_id, handle))
    }

    pub fn get_new_id(&self) -> Result<usize, Error> {
        let store = self.store.write()?;

        let bucket = store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::id_store()))?;

        let mut txn = store.write_txn()?;

        let new_id = self.with_lock(&bucket, &mut txn, b"id-lock", |txn| {
            let id = match txn.get(&bucket, b"current-id") {
                Ok(id) => id.inner()?.to_serde(),
                Err(e) => match e {
                    Error::NotFound => 1,
                    _ => return Err(e),
                },
            };

            let new_id = id + 1;

            let new_id_value = Json::to_value_buf(new_id)?;
            txn.set(&bucket, b"current-id", new_id_value)?;

            Ok(new_id)
        })?;

        txn.commit()?;

        Ok(new_id)
    }

    pub fn requeue_staged_jobs(&self) -> Result<(), Error> {
        let store = self.store.write()?;
        let job_bucket =
            store.bucket::<&[u8], ValueBuf<Json<JobInfo>>>(Some(Storage::job_store()))?;

        let lock_bucket =
            store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::job_lock()))?;

        let buckets = Buckets::new(&store)?;

        let mut write_txn = store.write_txn()?;
        let read_txn = store.read_txn()?;

        self.with_lock::<_, (), _>(&lock_bucket, &mut write_txn, b"job-queue", |inner_txn| {
            let mut cursor = read_txn.read_cursor(&buckets.staged)?;
            match cursor.get(None, CursorOp::First) {
                Ok(_) => (),
                Err(e) => match e {
                    Error::NotFound => {
                        return Ok(());
                    }
                    e => {
                        return Err(e);
                    }
                },
            }

            let initial_value = Ok(inner_txn) as Result<&mut Txn, Error>;

            let _ = cursor.iter().fold(initial_value, |acc, (key, _)| {
                acc.and_then(|inner_txn| {
                    let job = inner_txn.get(&job_bucket, &key)?.inner()?.to_serde();

                    let job_value = Json::to_value_buf(job)?;
                    inner_txn.set(&job_bucket, key, job_value)?;
                    self.queue_job(&buckets, inner_txn, key)?;

                    Ok(inner_txn)
                })
            })?;

            Ok(())
        })?;

        read_txn.commit()?;
        write_txn.commit()?;

        Ok(())
    }

    pub fn check_stalled_jobs(&self) -> Result<(), Error> {
        let store = self.store.write()?;
        let job_bucket =
            store.bucket::<&[u8], ValueBuf<Json<JobInfo>>>(Some(Storage::job_store()))?;

        let lock_bucket =
            store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::job_lock()))?;

        let buckets = Buckets::new(&store)?;

        let mut write_txn = store.write_txn()?;
        let read_txn = store.read_txn()?;

        self.with_lock::<_, (), _>(&lock_bucket, &mut write_txn, b"job-queue", |inner_txn| {
            let mut cursor = read_txn.read_cursor(&buckets.running)?;
            match cursor.get(None, CursorOp::First) {
                Ok(_) => (),
                Err(e) => match e {
                    Error::NotFound => {
                        return Ok(());
                    }
                    e => {
                        return Err(e);
                    }
                },
            }

            let initial_value = Ok(inner_txn) as Result<&mut Txn, Error>;

            let _ = cursor.iter().fold(initial_value, |acc, (key, _)| {
                acc.and_then(|inner_txn| {
                    let mut job = inner_txn.get(&job_bucket, &key)?.inner()?.to_serde();

                    if job.is_stale() {
                        if job.increment().should_requeue() {
                            let job_value = Json::to_value_buf(job)?;
                            inner_txn.set(&job_bucket, key, job_value)?;
                            self.queue_job(&buckets, inner_txn, key)?;
                        } else {
                            job.fail();
                            let job_value = Json::to_value_buf(job)?;
                            inner_txn.set(&job_bucket, key, job_value)?;
                            self.fail_job(&buckets, inner_txn, key)?;
                        }
                    }

                    Ok(inner_txn)
                })
            })?;

            Ok(())
        })?;

        read_txn.commit()?;
        write_txn.commit()?;

        Ok(())
    }

    pub fn stage_jobs(&self, limit: usize, queue: &str) -> Result<Vec<JobInfo>, Error> {
        let store = self.store.write()?;

        trace!("Got store");

        let job_bucket =
            store.bucket::<&[u8], ValueBuf<Json<JobInfo>>>(Some(Storage::job_store()))?;

        let lock_bucket =
            store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::job_lock()))?;

        let buckets = Buckets::new(&store)?;

        trace!("got buckets");

        let mut txn = store.write_txn()?;
        let read_txn = store.read_txn()?;

        let result = self.with_lock::<_, Vec<JobInfo>, _>(
            &lock_bucket,
            &mut txn,
            b"job-queue",
            |inner_txn| {
                let mut cursor = read_txn.read_cursor(&buckets.queued)?;
                trace!("Got cursor");
                match cursor.get(None, CursorOp::First) {
                    Ok(_) => (),
                    Err(e) => match e {
                        Error::NotFound => {
                            trace!("No items in queue");
                            return Ok(vec![]);
                        }
                        e => {
                            return Err(e);
                        }
                    },
                }
                trace!("Set cursor to first");

                let initial_value =
                    Ok((inner_txn, Vec::new())) as Result<(&mut Txn, Vec<JobInfo>), Error>;

                let now = Utc::now();

                trace!("Got lock");
                let (_inner_txn, vec) = cursor.iter().fold(initial_value, |acc, (key, _)| {
                    acc.and_then(|(inner_txn, mut jobs)| {
                        if jobs.len() < limit {
                            let mut job = inner_txn.get(&job_bucket, &key)?.inner()?.to_serde();

                            job.stage();

                            if job.is_ready(now) && job.is_in_queue(queue) {
                                self.stage_job(&buckets, inner_txn, key)?;

                                jobs.push(job);
                            }
                        }

                        Ok((inner_txn, jobs))
                    })
                })?;

                Ok(vec)
            },
        )?;

        trace!("Committing");

        read_txn.commit()?;

        txn.commit()?;

        trace!("Committed");

        Ok(result)
    }

    pub fn store_job(&self, mut job: JobInfo) -> Result<(), Error> {
        let job_id = match job.id() {
            Some(id) => id.to_string(),
            None => {
                let id = self.get_new_id()?;
                job.set_id(id);
                id.to_string()
            }
        };

        job.updated();

        trace!("Generaged job id, {}", job_id);

        if job.is_failed() {
            if job.increment().should_requeue() {
                job.pending();
                job.next_queue();
            }
        }

        let status = job.status();
        let job_value = Json::to_value_buf(job)?;

        trace!("Storing job");

        let store = self.store.write()?;
        trace!("Got store");
        let bucket = store.bucket::<&[u8], ValueBuf<Json<JobInfo>>>(Some(Storage::job_store()))?;
        trace!("Got bucket");

        let buckets = Buckets::new(&store)?;

        let mut txn = store.write_txn()?;
        trace!("Opened write txn");
        txn.set(&bucket, job_id.to_string().as_ref(), job_value)?;
        trace!("Set value");

        match status {
            JobStatus::Pending => self.queue_job(&buckets, &mut txn, job_id.as_ref())?,
            JobStatus::Running => self.run_job(&buckets, &mut txn, job_id.as_ref())?,
            JobStatus::Staged => self.stage_job(&buckets, &mut txn, job_id.as_ref())?,
            JobStatus::Failed => self.fail_job(&buckets, &mut txn, job_id.as_ref())?,
            JobStatus::Finished => self.finish_job(&buckets, &mut txn, job_id.as_ref())?,
        }

        trace!("Committing");

        txn.commit()?;
        trace!("Committed");

        Ok(())
    }

    pub fn get_port_mapping(
        &self,
        base_port: usize,
        queues: BTreeSet<String>,
    ) -> Result<BTreeMap<String, usize>, PortMapError> {
        let store = self.store.write().map_err(|e| Error::from(e))?;

        let queue_port_bucket =
            store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::queue_port()))?;

        let read_txn = store.read_txn()?;
        let mut write_txn = store.write_txn()?;

        let lock_name = "lock-queue";

        let queue_map = self.with_lock::<_, _, PortMapError>(
            &queue_port_bucket,
            &mut write_txn,
            lock_name.as_ref(),
            |write_txn| {
                let mut cursor = read_txn.read_cursor(&queue_port_bucket)?;

                let (unused_queues, queue_map) = cursor.iter().fold(
                    Ok((queues.clone(), BTreeMap::new())),
                    |acc: Result<_, PortMapError>, (queue, port)| {
                        acc.and_then(move |(mut queues, mut map)| {
                            let port: usize = port.inner()?.to_serde();
                            let queue = std::str::from_utf8(queue)?.to_owned();

                            if queue != lock_name {
                                queues.remove(&queue);
                                map.insert(queue, port);
                            }

                            Ok((queues, map))
                        })
                    },
                )?;

                // The starting port for new queues should be one greater than the maximum port
                // number in the btree set, or 2 greater than the base port.
                //
                // This is because there need to be two admin ports before the queue ports begin.
                let start_port = queue_map
                    .iter()
                    .map(|(_, v)| *v)
                    .filter(|v| *v != 0)
                    .max()
                    .unwrap_or(base_port + 1)
                    + 1;

                let (_, queue_map, _) = unused_queues.into_iter().fold(
                    Ok((write_txn, queue_map, start_port)),
                    |acc: Result<_, PortMapError>, queue_name| {
                        acc.and_then(|(write_txn, mut queue_map, port_num)| {
                            let port = Json::to_value_buf(port_num)?;

                            write_txn.set(&queue_port_bucket, queue_name.as_ref(), port)?;
                            queue_map.insert(queue_name, port_num);

                            Ok((write_txn, queue_map, port_num + 1))
                        })
                    },
                )?;

                Ok(queue_map)
            },
        )?;

        read_txn.commit()?;
        write_txn.commit()?;

        Ok(queue_map)
    }

    fn stage_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        txn: &mut Txn<'env>,
        id: &[u8],
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.staged, txn, id)?;
        self.delete_job_from(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.queued, txn, id)?;

        Ok(())
    }

    fn queue_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        txn: &mut Txn<'env>,
        id: &[u8],
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.queued, txn, id)?;
        self.delete_job_from(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;

        Ok(())
    }

    fn fail_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        txn: &mut Txn<'env>,
        id: &[u8],
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;
        self.delete_job_from(&buckets.queued, txn, id)?;

        Ok(())
    }

    fn run_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        txn: &mut Txn<'env>,
        id: &[u8],
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;
        self.delete_job_from(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.queued, txn, id)?;

        Ok(())
    }

    fn finish_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        txn: &mut Txn<'env>,
        id: &[u8],
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;
        self.delete_job_from(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.queued, txn, id)?;

        Ok(())
    }

    fn add_job_to<'env>(
        &self,
        bucket: &'env Bucket<&[u8], ValueBuf<Json<usize>>>,
        txn: &mut Txn<'env>,
        id: &[u8],
    ) -> Result<(), Error> {
        txn.set(bucket, id, Json::to_value_buf(self.runner_id)?)?;
        trace!("Set value");

        Ok(())
    }

    fn delete_job_from<'env>(
        &self,
        bucket: &'env Bucket<&[u8], ValueBuf<Json<usize>>>,
        txn: &mut Txn<'env>,
        id: &[u8],
    ) -> Result<(), Error> {
        match txn.del(bucket, id) {
            Ok(_) => (),
            Err(e) => match e {
                Error::NotFound => (),
                e => return Err(e),
            },
        }
        trace!("Deleted value");

        Ok(())
    }

    // In all likelihood, this function is not necessary
    //
    // But in the event of multiple processes running on the same machine, it is good to have some
    // way to make sure they don't step on eachother's toes
    fn with_lock<'env, F, T, E>(
        &self,
        lock_bucket: &'env Bucket<&[u8], ValueBuf<Json<usize>>>,
        txn: &mut Txn<'env>,
        lock_key: &[u8],
        callback: F,
    ) -> Result<T, E>
    where
        F: Fn(&mut Txn<'env>) -> Result<T, E>,
        E: From<Error>,
    {
        let mut other_runner_id = 0;

        loop {
            let lock_value = Json::to_value_buf(self.runner_id)?;

            let mut inner_txn = txn.txn()?;
            let res = inner_txn.set_no_overwrite(lock_bucket, lock_key, lock_value);
            inner_txn.commit()?;

            match res {
                Ok(_) => break,
                Err(e) => {
                    let inner_txn = txn.txn()?;
                    let res = inner_txn.get(lock_bucket, lock_key);
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
                            e => return Err(e.into()),
                        },
                    }

                    match e {
                        Error::LMDB(lmdb) => match lmdb {
                            LmdbError::KeyExist => continue,
                            e => return Err(Error::LMDB(e).into()),
                        },
                        e => return Err(e.into()),
                    }
                }
            }
        }

        let item = callback(txn)?;

        txn.del(lock_bucket, lock_key)?;

        Ok(item)
    }

    fn buckets() -> [&'static str; 9] {
        [
            Storage::id_store(),
            Storage::job_store(),
            Storage::job_queue(),
            Storage::job_failed(),
            Storage::job_running(),
            Storage::job_staged(),
            Storage::job_lock(),
            Storage::job_finished(),
            Storage::queue_port(),
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

    fn job_staged() -> &'static str {
        "job-staged"
    }

    fn job_finished() -> &'static str {
        "job-finished"
    }

    fn job_lock() -> &'static str {
        "job-lock"
    }

    fn queue_port() -> &'static str {
        "queue-port"
    }
}

#[derive(Debug, Fail)]
pub enum PortMapError {
    #[fail(display = "Error in KV, {}", _0)]
    Kv(#[cause] Error),

    #[fail(display = "Error parsing to Utf8, {}", _0)]
    Utf8(#[cause] Utf8Error),
}

impl From<Error> for PortMapError {
    fn from(e: Error) -> Self {
        PortMapError::Kv(e)
    }
}

impl From<Utf8Error> for PortMapError {
    fn from(e: Utf8Error) -> Self {
        PortMapError::Utf8(e)
    }
}
