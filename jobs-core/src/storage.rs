/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2018 Riley Trautman
 *
 * Background Jobs is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Background Jobs is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Background Jobs.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    str::Utf8Error,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use chrono::{offset::Utc, DateTime, Datelike, Timelike};
use failure::Fail;
use kv::{json::Json, Bucket, Config, CursorOp, Error, Manager, Serde, Store, Txn, ValueBuf};
use lmdb::Error as LmdbError;
use log::{info, trace};
use serde_derive::{Deserialize, Serialize};

use crate::{JobInfo, JobStatus, NewJobInfo};

struct Buckets<'a> {
    queued: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    running: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    staged: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    failed: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    finished: Bucket<'a, &'a [u8], ValueBuf<Json<usize>>>,
    stats: Bucket<'a, &'a [u8], ValueBuf<Json<Stat>>>,
}

impl<'a> Buckets<'a> {
    fn new(store: &'a RwLockWriteGuard<Store>) -> Result<Self, Error> {
        let b = Buckets {
            queued: store.bucket(Some(Storage::job_queue()))?,
            running: store.bucket(Some(Storage::job_running()))?,
            staged: store.bucket(Some(Storage::job_staged()))?,
            failed: store.bucket(Some(Storage::job_failed()))?,
            finished: store.bucket(Some(Storage::job_finished()))?,
            stats: store.bucket(Some(Storage::stats_store()))?,
        };

        Ok(b)
    }

    fn new_readonly(store: &'a RwLockReadGuard<Store>) -> Result<Self, Error> {
        let b = Buckets {
            queued: store.bucket(Some(Storage::job_queue()))?,
            running: store.bucket(Some(Storage::job_running()))?,
            staged: store.bucket(Some(Storage::job_staged()))?,
            failed: store.bucket(Some(Storage::job_failed()))?,
            finished: store.bucket(Some(Storage::job_finished()))?,
            stats: store.bucket(Some(Storage::stats_store()))?,
        };

        Ok(b)
    }
}

#[derive(Clone)]
/// All the logic to interact with the persisted data is defined on this type.
///
/// Perhapse in the future this will be made generic, but for now it is hard-coded to use LMDB to
/// store job information.
///
/// None of the methods in this module are intended to be used outside of a background-jobs
/// runtime.
pub struct Storage {
    store: Arc<RwLock<Store>>,
}

impl Storage {
    pub fn new(store: Arc<RwLock<Store>>) -> Self {
        Storage { store }
    }

    pub fn init(path: PathBuf) -> Result<Self, Error> {
        let mut manager = Manager::new();
        let mut cfg = Config::default(path);

        cfg.set_max_readers(18);

        // Create our buckets
        for bucket in Storage::buckets().iter() {
            cfg.bucket(bucket, None);
        }

        let handle = manager.open(cfg)?;

        Ok(Storage::new(handle))
    }

    pub fn get_new_id(&self, runner_id: usize) -> Result<usize, Error> {
        let store = self.store.write()?;

        let bucket = store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::id_store()))?;

        let mut txn = store.write_txn()?;

        let new_id = self.with_lock(&bucket, &mut txn, b"id-lock", runner_id, |txn| {
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

    pub fn requeue_staged_jobs(&self, runner_id: usize) -> Result<(), Error> {
        let store = self.store.write()?;
        let job_bucket =
            store.bucket::<&[u8], ValueBuf<Json<JobInfo>>>(Some(Storage::job_store()))?;

        let lock_bucket =
            store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::job_lock()))?;

        let buckets = Buckets::new(&store)?;

        let mut write_txn = store.write_txn()?;
        let read_txn = store.read_txn()?;

        self.with_lock::<_, (), _>(
            &lock_bucket,
            &mut write_txn,
            b"job-queue",
            runner_id,
            |inner_txn| {
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
                        self.queue_job(&buckets, inner_txn, key, runner_id)?;

                        Ok(inner_txn)
                    })
                })?;

                Ok(())
            },
        )?;

        read_txn.commit()?;
        write_txn.commit()?;

        Ok(())
    }

    pub fn check_stalled_jobs(&self, runner_id: usize) -> Result<(), Error> {
        let store = self.store.write()?;
        let job_bucket =
            store.bucket::<&[u8], ValueBuf<Json<JobInfo>>>(Some(Storage::job_store()))?;

        let lock_bucket =
            store.bucket::<&[u8], ValueBuf<Json<usize>>>(Some(Storage::job_lock()))?;

        let buckets = Buckets::new(&store)?;

        let mut write_txn = store.write_txn()?;
        let read_txn = store.read_txn()?;

        self.with_lock::<_, (), _>(
            &lock_bucket,
            &mut write_txn,
            b"job-queue",
            runner_id,
            |inner_txn| {
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
                                self.queue_job(&buckets, inner_txn, key, runner_id)?;
                            } else {
                                self.fail_job(&buckets, &job_bucket, inner_txn, key, runner_id)?;
                            }
                        }

                        Ok(inner_txn)
                    })
                })?;

                Ok(())
            },
        )?;

        read_txn.commit()?;
        write_txn.commit()?;

        Ok(())
    }

    pub fn stage_jobs(
        &self,
        limit: usize,
        queue: &str,
        runner_id: usize,
    ) -> Result<Vec<JobInfo>, Error> {
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
            runner_id,
            |inner_txn| {
                trace!("Got lock");

                let mut jobs = HashMap::new();
                let mut cursor = read_txn.read_cursor(&buckets.queued)?;
                let now = Utc::now();

                trace!("Got cursor");
                match cursor.get(None, CursorOp::First) {
                    Ok((maybe_key, _)) => {
                        if let Some(key) = maybe_key {
                            match inner_txn.get(&job_bucket, &key) {
                                Ok(job) => {
                                    let mut job = job.inner()?.to_serde();
                                    if job.is_ready(now) && job.is_in_queue(queue) {
                                        job.stage();
                                        self.stage_job(&buckets, inner_txn, key, runner_id)?;
                                        jobs.insert(job.id(), job);
                                    }
                                }
                                Err(e) => match e {
                                    Error::NotFound => (),
                                    err => return Err(err),
                                },
                            }
                        }
                    }
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
                    Ok((inner_txn, jobs)) as Result<(&mut Txn, HashMap<usize, JobInfo>), Error>;

                let (_inner_txn, mut hm) = cursor.iter().fold(initial_value, |acc, (key, _)| {
                    acc.and_then(|(inner_txn, mut jobs)| {
                        if jobs.len() < limit {
                            let mut job = inner_txn.get(&job_bucket, &key)?.inner()?.to_serde();

                            if job.is_ready(now) && job.is_in_queue(queue) {
                                job.stage();
                                self.stage_job(&buckets, inner_txn, key, runner_id)?;

                                jobs.insert(job.id(), job);
                            }
                        }

                        Ok((inner_txn, jobs))
                    })
                })?;

                Ok(hm.drain().map(|(_, v)| v).collect())
            },
        )?;

        trace!("Committing");

        read_txn.commit()?;

        txn.commit()?;

        trace!("Committed");

        Ok(result)
    }

    pub fn assign_id(&self, job: NewJobInfo, runner_id: usize) -> Result<JobInfo, Error> {
        let id = self.get_new_id(runner_id)?;
        let job = job.with_id(id);
        trace!("Generaged job id, {}", job.id());
        Ok(job)
    }

    pub fn store_job(&self, mut job: JobInfo, runner_id: usize) -> Result<(), Error> {
        let job_id = job.id().to_string();
        job.updated();

        job.needs_retry();

        let status = job.status();
        let job_value = Json::to_value_buf(job)?;

        trace!("Storing job");

        let store = self.store.write()?;
        trace!("Got store");
        let job_bucket =
            store.bucket::<&[u8], ValueBuf<Json<JobInfo>>>(Some(Storage::job_store()))?;
        trace!("Got bucket");

        let buckets = Buckets::new(&store)?;

        let mut txn = store.write_txn()?;
        trace!("Opened write txn");
        txn.set(&job_bucket, job_id.to_string().as_ref(), job_value)?;
        trace!("Set value");

        match status {
            JobStatus::Pending => self.queue_job(&buckets, &mut txn, job_id.as_ref(), runner_id)?,
            JobStatus::Running => self.run_job(&buckets, &mut txn, job_id.as_ref(), runner_id)?,
            JobStatus::Staged => self.stage_job(&buckets, &mut txn, job_id.as_ref(), runner_id)?,
            JobStatus::Failed => {
                self.fail_job(&buckets, &job_bucket, &mut txn, job_id.as_ref(), runner_id)?
            }
            JobStatus::Finished => {
                self.finish_job(&buckets, &job_bucket, &mut txn, job_id.as_ref(), runner_id)?
            }
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
        runner_id: usize,
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
            runner_id,
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
        runner_id: usize,
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.staged, txn, id, runner_id)?;
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
        runner_id: usize,
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.queued, txn, id, runner_id)?;
        self.delete_job_from(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;

        Ok(())
    }

    fn run_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        txn: &mut Txn<'env>,
        id: &[u8],
        runner_id: usize,
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.running, txn, id, runner_id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;
        self.delete_job_from(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.queued, txn, id)?;

        Ok(())
    }

    fn fail_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        job_store: &'env Bucket<&[u8], ValueBuf<Json<JobInfo>>>,
        txn: &mut Txn<'env>,
        id: &[u8],
        runner_id: usize,
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.failed, txn, id, runner_id)?;
        self.delete_job_from(&buckets.finished, txn, id)?;
        self.delete_job_from(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;
        self.delete_job_from(&buckets.queued, txn, id)?;
        txn.del(job_store, id)?;

        Stat::get_dead(&buckets.stats, txn)?
            .fail_job()
            .save(&buckets.stats, txn)?;

        Ok(())
    }

    fn finish_job<'env>(
        &self,
        buckets: &'env Buckets<'env>,
        job_store: &'env Bucket<&[u8], ValueBuf<Json<JobInfo>>>,
        txn: &mut Txn<'env>,
        id: &[u8],
        runner_id: usize,
    ) -> Result<(), Error> {
        self.add_job_to(&buckets.finished, txn, id, runner_id)?;
        self.delete_job_from(&buckets.running, txn, id)?;
        self.delete_job_from(&buckets.staged, txn, id)?;
        self.delete_job_from(&buckets.failed, txn, id)?;
        self.delete_job_from(&buckets.queued, txn, id)?;
        txn.del(job_store, id)?;

        Stat::get_finished(&buckets.stats, txn)?
            .finish_job()
            .save(&buckets.stats, txn)?;

        Ok(())
    }

    fn add_job_to<'env>(
        &self,
        bucket: &'env Bucket<&[u8], ValueBuf<Json<usize>>>,
        txn: &mut Txn<'env>,
        id: &[u8],
        runner_id: usize,
    ) -> Result<(), Error> {
        txn.set(bucket, id, Json::to_value_buf(runner_id)?)?;
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

    pub fn get_stats(&self) -> Result<Stats, Error> {
        let store = self.store.read()?;
        let buckets = Buckets::new_readonly(&store)?;

        let mut txn = store.read_txn()?;

        let stats = {
            let dead = Stat::get_dead(&buckets.stats, &mut txn)?.inner_stat();
            let complete = Stat::get_finished(&buckets.stats, &mut txn)?.inner_stat();

            let mut queued_cursor = txn.read_cursor(&buckets.queued)?;
            let mut staged_cursor = txn.read_cursor(&buckets.staged)?;

            let pending = queued_cursor.iter().count() + staged_cursor.iter().count();

            let mut running_cursor = txn.read_cursor(&buckets.running)?;
            let running = running_cursor.iter().count();

            Stats {
                dead,
                complete,
                pending,
                running,
            }
        };

        txn.commit()?;
        Ok(stats)
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
        runner_id: usize,
        callback: F,
    ) -> Result<T, E>
    where
        F: Fn(&mut Txn<'env>) -> Result<T, E>,
        E: From<Error>,
    {
        let mut other_runner_id = 10;

        loop {
            let lock_value = Json::to_value_buf(runner_id)?;

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

    fn buckets() -> [&'static str; 10] {
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
            Storage::stats_store(),
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

    fn stats_store() -> &'static str {
        "stats-store"
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Stats {
    pub pending: usize,
    pub running: usize,
    pub dead: JobStat,
    pub complete: JobStat,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Stat {
    DeadJobs(JobStat),
    CompletedJobs(JobStat),
}

impl Stat {
    fn get_finished<'env>(
        bucket: &'env Bucket<&[u8], ValueBuf<Json<Self>>>,
        txn: &mut Txn<'env>,
    ) -> Result<Self, Error> {
        Self::get(bucket, txn, Self::completed_jobs()).map(|opt| match opt {
            Some(stat) => stat,
            None => Stat::CompletedJobs(JobStat::new()),
        })
    }

    fn get_dead<'env>(
        bucket: &'env Bucket<&[u8], ValueBuf<Json<Self>>>,
        txn: &mut Txn<'env>,
    ) -> Result<Self, Error> {
        Self::get(bucket, txn, Self::dead_jobs()).map(|opt| match opt {
            Some(stat) => stat,
            None => Stat::DeadJobs(JobStat::new()),
        })
    }

    fn get<'env>(
        bucket: &'env Bucket<&[u8], ValueBuf<Json<Self>>>,
        txn: &mut Txn<'env>,
        key: &str,
    ) -> Result<Option<Self>, Error> {
        match txn.get(bucket, key.as_ref()) {
            Ok(stat) => Ok(Some(stat.inner()?.to_serde())),
            Err(e) => match e {
                Error::NotFound => Ok(None),
                err => return Err(err),
            },
        }
    }

    fn name(&self) -> &str {
        match *self {
            Stat::DeadJobs(_) => Stat::dead_jobs(),
            Stat::CompletedJobs(_) => Stat::completed_jobs(),
        }
    }

    fn finish_job(self) -> Self {
        match self {
            Stat::CompletedJobs(mut job_stat) => {
                job_stat.increment();
                Stat::CompletedJobs(job_stat)
            }
            other => other,
        }
    }

    fn fail_job(self) -> Self {
        match self {
            Stat::DeadJobs(mut job_stat) => {
                job_stat.increment();
                Stat::DeadJobs(job_stat)
            }
            other => other,
        }
    }

    fn inner_stat(self) -> JobStat {
        match self {
            Stat::DeadJobs(job_stat) => job_stat,
            Stat::CompletedJobs(job_stat) => job_stat,
        }
    }

    fn dead_jobs() -> &'static str {
        "DeadJobs"
    }

    fn completed_jobs() -> &'static str {
        "CompletedJobs"
    }

    fn save<'env>(
        self,
        bucket: &'env Bucket<&[u8], ValueBuf<Json<Self>>>,
        txn: &mut Txn<'env>,
    ) -> Result<(), Error> {
        let name = self.name().to_owned();
        txn.set(bucket, name.as_ref(), Json::to_value_buf(self)?)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobStat {
    this_hour: usize,
    today: usize,
    this_month: usize,
    all_time: usize,
    updated_at: DateTime<Utc>,
}

impl JobStat {
    fn new() -> Self {
        JobStat {
            this_hour: 0,
            today: 0,
            this_month: 0,
            all_time: 0,
            updated_at: Utc::now(),
        }
    }

    fn increment(&mut self) {
        self.this_hour += 1;
        self.today += 1;
        self.this_month += 1;
        self.all_time += 1;

        self.tick();
    }

    fn tick(&mut self) {
        let now = Utc::now();

        if now.month() != self.updated_at.month() {
            self.next_month();
        } else if now.day() != self.updated_at.day() {
            self.next_day();
        } else if now.hour() != self.updated_at.hour() {
            self.next_hour();
        }

        self.updated_at = now;
    }

    fn next_hour(&mut self) {
        self.this_hour = 0;
    }

    fn next_day(&mut self) {
        self.next_hour();
        self.today = 0;
    }

    fn next_month(&mut self) {
        self.next_day();
        self.this_month = 0;
    }

    pub fn this_hour(&self) -> usize {
        self.this_hour
    }

    pub fn today(&self) -> usize {
        self.today
    }

    pub fn this_month(&self) -> usize {
        self.this_month
    }

    pub fn all_time(&self) -> usize {
        self.all_time
    }
}
