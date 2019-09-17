/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2019 Riley Trautman
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

use chrono::offset::Utc;
use failure::Fail;
use log::error;

use crate::{JobInfo, NewJobInfo, ReturnJobInfo, Stats};

/// Define a storage backend for jobs
///
/// This crate provides a default implementation in the `memory_storage` module, which is backed by
/// HashMaps and uses counting to assign IDs. If jobs must be persistent across application
/// restarts, look into the `[sled-backed](https://github.com/spacejam/sled)` implementation from
/// the `background-jobs-sled-storage` crate.
pub trait Storage: Clone + Send {
    /// The error type used by the storage mechansim.
    type Error: Fail;

    /// This method generates unique IDs for jobs
    fn generate_id(&mut self) -> Result<u64, Self::Error>;

    /// This method should store the supplied job
    ///
    /// The supplied job _may already be present_. The implementation should overwrite the stored
    /// job with the new job so that future calls to `fetch_job` return the new one.
    fn save_job(&mut self, job: JobInfo) -> Result<(), Self::Error>;

    /// This method should return the job with the given ID regardless of what state the job is in.
    fn fetch_job(&mut self, id: u64) -> Result<Option<JobInfo>, Self::Error>;

    /// This should fetch a job ready to be processed from the queue
    ///
    /// If a job is not ready, is currently running, or is not in the requested queue, this method
    /// should not return it. If no jobs meet these criteria, this method should return Ok(None)
    fn fetch_job_from_queue(&mut self, queue: &str) -> Result<Option<JobInfo>, Self::Error>;

    /// This method tells the storage mechanism to mark the given job as being in the provided
    /// queue
    fn queue_job(&mut self, queue: &str, id: u64) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to mark a given job as running
    fn run_job(&mut self, id: u64, runner_id: u64) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to remove the job
    ///
    /// This happens when a job has been completed or has failed too many times
    fn delete_job(&mut self, id: u64) -> Result<(), Self::Error>;

    /// This method returns the current statistics, or Stats::default() if none exists.
    fn get_stats(&self) -> Result<Stats, Self::Error>;

    /// This method fetches the existing statistics or Stats::default(), and stores the result of
    /// calling `update_stats` on it.
    fn update_stats<F>(&mut self, f: F) -> Result<(), Self::Error>
    where
        F: Fn(Stats) -> Stats;

    fn new_job(&mut self, job: NewJobInfo) -> Result<u64, Self::Error> {
        let id = self.generate_id()?;

        let job = job.with_id(id);

        let queue = job.queue().to_owned();
        self.save_job(job)?;
        self.queue_job(&queue, id)?;
        self.update_stats(Stats::new_job)?;

        Ok(id)
    }

    fn request_job(&mut self, queue: &str, runner_id: u64) -> Result<Option<JobInfo>, Self::Error> {
        match self.fetch_job_from_queue(queue)? {
            Some(mut job) => {
                if job.is_pending() && job.is_ready(Utc::now()) && job.is_in_queue(queue) {
                    job.run();
                    self.run_job(job.id(), runner_id)?;
                    self.save_job(job.clone())?;
                    self.update_stats(Stats::run_job)?;

                    Ok(Some(job))
                } else {
                    error!(
                        "Not fetching job {}, it is not ready for processing",
                        job.id()
                    );
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn return_job(
        &mut self,
        ReturnJobInfo { id, result }: ReturnJobInfo,
    ) -> Result<(), Self::Error> {
        if result.is_failure() {
            if let Some(mut job) = self.fetch_job(id)? {
                if job.needs_retry() {
                    self.queue_job(job.queue(), id)?;
                    self.save_job(job)?;
                    self.update_stats(Stats::retry_job)
                } else {
                    self.delete_job(id)?;
                    self.update_stats(Stats::fail_job)
                }
            } else {
                Ok(())
            }
        } else if result.is_missing_processor() {
            if let Some(mut job) = self.fetch_job(id)? {
                job.pending();
                self.queue_job(job.queue(), id)?;
                self.save_job(job)?;
                self.update_stats(Stats::retry_job)
            } else {
                Ok(())
            }
        } else {
            self.delete_job(id)?;
            self.update_stats(Stats::complete_job)
        }
    }
}

pub mod memory_storage {
    use super::{JobInfo, Stats};
    use failure::Fail;
    use std::{
        collections::HashMap,
        fmt,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    pub struct Storage {
        inner: Arc<Mutex<Inner>>,
    }

    #[derive(Clone)]
    struct Inner {
        count: u64,
        jobs: HashMap<u64, JobInfo>,
        queues: HashMap<u64, String>,
        worker_ids: HashMap<u64, u64>,
        worker_ids_inverse: HashMap<u64, u64>,
        stats: Stats,
    }

    impl Storage {
        pub fn new() -> Self {
            Storage {
                inner: Arc::new(Mutex::new(Inner {
                    count: 0,
                    jobs: HashMap::new(),
                    queues: HashMap::new(),
                    worker_ids: HashMap::new(),
                    worker_ids_inverse: HashMap::new(),
                    stats: Stats::default(),
                })),
            }
        }
    }

    impl super::Storage for Storage {
        type Error = Never;

        fn generate_id(&mut self) -> Result<u64, Self::Error> {
            let mut inner = self.inner.lock().unwrap();
            let id = inner.count;
            inner.count = inner.count.wrapping_add(1);
            Ok(id)
        }

        fn save_job(&mut self, job: JobInfo) -> Result<(), Self::Error> {
            self.inner.lock().unwrap().jobs.insert(job.id(), job);

            Ok(())
        }

        fn fetch_job(&mut self, id: u64) -> Result<Option<JobInfo>, Self::Error> {
            let j = self.inner.lock().unwrap().jobs.get(&id).map(|j| j.clone());

            Ok(j)
        }

        fn fetch_job_from_queue(&mut self, queue: &str) -> Result<Option<JobInfo>, Self::Error> {
            let mut inner = self.inner.lock().unwrap();

            let j = inner
                .queues
                .iter()
                .filter_map(|(k, v)| {
                    if v == queue {
                        inner.jobs.get(k).map(|j| j.clone())
                    } else {
                        None
                    }
                })
                .next();

            if let Some(ref j) = j {
                inner.queues.remove(&j.id());
            }

            Ok(j)
        }

        fn queue_job(&mut self, queue: &str, id: u64) -> Result<(), Self::Error> {
            self.inner
                .lock()
                .unwrap()
                .queues
                .insert(id, queue.to_owned());
            Ok(())
        }

        fn run_job(&mut self, id: u64, worker_id: u64) -> Result<(), Self::Error> {
            let mut inner = self.inner.lock().unwrap();

            inner.worker_ids.insert(id, worker_id);
            inner.worker_ids_inverse.insert(worker_id, id);
            Ok(())
        }

        fn delete_job(&mut self, id: u64) -> Result<(), Self::Error> {
            let mut inner = self.inner.lock().unwrap();
            inner.jobs.remove(&id);
            inner.queues.remove(&id);
            if let Some(worker_id) = inner.worker_ids.remove(&id) {
                inner.worker_ids_inverse.remove(&worker_id);
            }
            Ok(())
        }

        fn get_stats(&self) -> Result<Stats, Self::Error> {
            Ok(self.inner.lock().unwrap().stats.clone())
        }

        fn update_stats<F>(&mut self, f: F) -> Result<(), Self::Error>
        where
            F: Fn(Stats) -> Stats,
        {
            let mut inner = self.inner.lock().unwrap();

            inner.stats = (f)(inner.stats.clone());
            Ok(())
        }
    }

    #[derive(Clone, Debug, Fail)]
    pub enum Never {}

    impl fmt::Display for Never {
        fn fmt(&self, _: &mut fmt::Formatter) -> fmt::Result {
            match *self {}
        }
    }

    #[derive(Clone, Debug, Fail)]
    #[fail(display = "Created too many storages, can't generate any more IDs")]
    pub struct TooManyStoragesError;
}
