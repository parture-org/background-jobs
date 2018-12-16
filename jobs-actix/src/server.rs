use std::collections::{HashMap, VecDeque};

use actix::{Actor, Addr, Context, Handler, Message, SyncContext};
use background_jobs_core::{JobInfo, NewJobInfo, Storage};
use failure::Error;
use log::{debug, trace};
use serde_derive::Deserialize;

use crate::ProcessJob;

#[derive(Clone, Debug, Deserialize)]
pub enum EitherJob {
    New(NewJobInfo),
    Existing(JobInfo),
}

impl Message for EitherJob {
    type Result = Result<(), Error>;
}

pub struct RequestJob<W>
where
    W: Actor + Handler<ProcessJob>,
{
    worker_id: usize,
    queue: String,
    addr: Addr<W>,
}

impl<W> RequestJob<W>
where
    W: Actor + Handler<ProcessJob>,
{
    pub fn new(worker_id: usize, queue: &str, addr: Addr<W>) -> Self {
        RequestJob {
            worker_id,
            queue: queue.to_owned(),
            addr,
        }
    }
}

impl<W> Message for RequestJob<W>
where
    W: Actor + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;
}

pub struct CheckDb;

impl Message for CheckDb {
    type Result = Result<(), Error>;
}

struct Cache<W>
where
    W: Actor + Handler<ProcessJob>,
{
    workers: VecDeque<RequestJob<W>>,
    jobs: VecDeque<JobInfo>,
}

impl<W> Cache<W>
where
    W: Actor + Handler<ProcessJob>,
{
    fn new() -> Self {
        Cache {
            workers: VecDeque::new(),
            jobs: VecDeque::new(),
        }
    }
}

pub struct Server<W>
where
    W: Actor + Handler<ProcessJob>,
{
    server_id: usize,
    storage: Storage,
    cache: HashMap<String, Cache<W>>,
    cache_size: usize,
}

impl<W> Server<W>
where
    W: Actor + Handler<ProcessJob>,
{
    pub fn new(server_id: usize, storage: Storage) -> Self {
        Server {
            server_id,
            storage,
            cache: HashMap::new(),
            cache_size: 25,
        }
    }

    pub fn set_cache_size(&mut self, cache_size: usize) {
        self.cache_size = cache_size;
    }

    fn populate(&mut self, queue: &str) -> Result<bool, Error> {
        trace!("Populating queue {}", queue);
        let entry = self.cache.entry(queue.to_owned()).or_insert(Cache::new());

        if entry.jobs.is_empty() {
            let new_jobs = self
                .storage
                .stage_jobs(self.cache_size, queue, self.server_id)?;
            let empty = new_jobs.is_empty();

            debug!("Retrieved {} jobs from storage", new_jobs.len());
            trace!("{:?}", new_jobs.iter().map(|j| j.id()).collect::<Vec<_>>());

            new_jobs
                .into_iter()
                .for_each(|job| entry.jobs.push_back(job));
            Ok(!empty)
        } else {
            Ok(true)
        }
    }
}

impl<W> Actor for Server<W>
where
    W: Actor + Handler<ProcessJob>,
{
    type Context = SyncContext<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        self.storage.requeue_staged_jobs(self.server_id).unwrap();
        self.storage.check_stalled_jobs(self.server_id).unwrap();
    }
}

impl<W> Handler<EitherJob> for Server<W>
where
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: EitherJob, _: &mut Self::Context) -> Self::Result {
        let mut job = match msg {
            EitherJob::New(new_job) => {
                let job = self.storage.assign_id(new_job, self.server_id)?;
                debug!("Created job {}, {:?}", job.id(), job);
                job
            }
            EitherJob::Existing(job) => job,
        };

        let retry_now = job.is_pending() || (job.needs_retry() && job.retry_ready());

        if job.is_pending() && !retry_now {
            trace!("Storing job {} for later processing", job.id());
        }
        self.storage.store_job(job.clone(), self.server_id)?;

        if retry_now {
            let entry = self
                .cache
                .entry(job.queue().to_owned())
                .or_insert(Cache::new());

            if let Some(worker) = entry.workers.pop_front() {
                debug!("Retrying job {} on worker {}", job.id(), worker.worker_id);
                worker.addr.do_send(ProcessJob::new(job.clone()));
                job.set_running();
                self.storage.store_job(job, worker.worker_id)?;
            } else if entry.jobs.len() < self.cache_size {
                entry.jobs.push_back(job);
            }
        }

        Ok(())
    }
}

impl<W> Handler<RequestJob<W>> for Server<W>
where
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: RequestJob<W>, _: &mut Self::Context) -> Self::Result {
        trace!("Worker {} requested job", msg.worker_id);
        self.populate(&msg.queue)?;

        let job = self
            .cache
            .get_mut(&msg.queue)
            .and_then(|cache| cache.jobs.pop_front());

        if let Some(mut job) = job {
            msg.addr.do_send(ProcessJob::new(job.clone()));
            job.set_running();
            self.storage.store_job(job, msg.worker_id)?;
        } else {
            trace!("storing worker {} for queue {}", msg.worker_id, msg.queue);
            let entry = self.cache.entry(msg.queue.clone()).or_insert(Cache::new());
            entry.workers.push_back(msg);
        }

        Ok(())
    }
}

impl<W> Handler<CheckDb> for Server<W>
where
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, _: CheckDb, _: &mut Self::Context) -> Self::Result {
        trace!("Checkdb");
        let queues: Vec<String> = self.cache.keys().cloned().collect();

        let mut todo = Vec::new();

        for queue in queues {
            if self.populate(&queue)? {
                debug!("Cached jobs for {}", queue);
            }

            let entry = self.cache.entry(queue.to_owned()).or_insert(Cache::new());

            let min_len = entry.jobs.len().min(entry.workers.len());

            entry
                .jobs
                .drain(..min_len)
                .zip(entry.workers.drain(..min_len))
                .for_each(|pair| {
                    todo.push(pair);
                });
        }

        for (mut job, worker) in todo {
            debug!("Sending job {} to worker {}", job.id(), worker.worker_id);
            worker.addr.do_send(ProcessJob::new(job.clone()));
            job.set_running();
            self.storage.store_job(job, worker.worker_id)?;
        }

        Ok(())
    }
}
