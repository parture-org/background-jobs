use std::collections::{HashMap, VecDeque};

use actix::{Actor, Addr, Context, Handler, Message, SyncContext};
use background_jobs_core::{NewJobInfo, ReturnJobInfo, Stats, Storage};
use failure::Error;
use log::trace;
use serde_derive::Deserialize;

use crate::ProcessJob;

#[derive(Clone, Debug, Deserialize)]
pub struct NewJob(pub(crate) NewJobInfo);

#[derive(Clone, Debug, Deserialize)]
pub struct ReturningJob(pub(crate) ReturnJobInfo);

impl Message for NewJob {
    type Result = Result<(), Error>;
}

impl Message for ReturningJob {
    type Result = Result<(), Error>;
}

pub struct RequestJob<W>
where
    W: Actor + Handler<ProcessJob>,
{
    worker_id: u64,
    queue: String,
    addr: Addr<W>,
}

impl<W> RequestJob<W>
where
    W: Actor + Handler<ProcessJob>,
{
    pub fn new(worker_id: u64, queue: &str, addr: Addr<W>) -> Self {
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

pub struct GetStats;

impl Message for GetStats {
    type Result = Result<Stats, Error>;
}

pub struct Server<S, W>
where
    S: Storage + 'static,
    W: Actor + Handler<ProcessJob>,
{
    storage: S,
    cache: HashMap<String, VecDeque<RequestJob<W>>>,
}

impl<S, W> Server<S, W>
where
    S: Storage + 'static,
    W: Actor + Handler<ProcessJob>,
{
    pub fn new(storage: S) -> Self {
        Server {
            storage,
            cache: HashMap::new(),
        }
    }
}

impl<S, W> Actor for Server<S, W>
where
    S: Storage + 'static,
    W: Actor + Handler<ProcessJob>,
{
    type Context = SyncContext<Self>;
}

impl<S, W> Handler<NewJob> for Server<S, W>
where
    S: Storage + 'static,
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: NewJob, _: &mut Self::Context) -> Self::Result {
        let queue = msg.0.queue().to_owned();
        let ready = msg.0.is_ready();
        self.storage.new_job(msg.0)?;

        if ready {
            let entry = self
                .cache
                .entry(queue.clone())
                .or_insert(VecDeque::new());

            if let Some(request) = entry.pop_front() {
                if let Some(job) = self.storage.request_job(&queue, request.worker_id)? {
                    request.addr.do_send(ProcessJob::new(job));
                } else {
                    entry.push_back(request);
                }
            }
        }

        Ok(())
    }
}

impl<S, W> Handler<ReturningJob> for Server<S, W>
where
    S: Storage + 'static,
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: ReturningJob, _: &mut Self::Context) -> Self::Result {
        self.storage.return_job(msg.0).map_err(|e| e.into())
    }
}

impl<S, W> Handler<RequestJob<W>> for Server<S, W>
where
    S: Storage + 'static,
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: RequestJob<W>, _: &mut Self::Context) -> Self::Result {
        trace!("Worker {} requested job", msg.worker_id);
        let job = self.storage.request_job(&msg.queue, msg.worker_id)?;

        if let Some(job) = job {
            msg.addr.do_send(ProcessJob::new(job.clone()));
        } else {
            trace!("storing worker {} for queue {}", msg.worker_id, msg.queue);
            let entry = self
                .cache
                .entry(msg.queue.to_owned())
                .or_insert(VecDeque::new());
            entry.push_back(msg);
        }

        Ok(())
    }
}

impl<S, W> Handler<CheckDb> for Server<S, W>
where
    S: Storage + 'static,
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<(), Error>;

    fn handle(&mut self, _: CheckDb, _: &mut Self::Context) -> Self::Result {
        trace!("Checkdb");

        for (queue, workers) in self.cache.iter_mut() {
            if let Some(request) = workers.pop_front() {
                if let Some(job) = self.storage.request_job(queue, request.worker_id)? {
                    request.addr.do_send(ProcessJob::new(job));
                } else {
                    workers.push_back(request);
                }
            }
        }

        Ok(())
    }
}

impl<S, W> Handler<GetStats> for Server<S, W>
where
    S: Storage + 'static,
    W: Actor<Context = Context<W>> + Handler<ProcessJob>,
{
    type Result = Result<Stats, Error>;

    fn handle(&mut self, _: GetStats, _: &mut Self::Context) -> Self::Result {
        self.storage.get_stats().map_err(|e| e.into())
    }
}
