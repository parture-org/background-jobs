use std::collections::{HashMap, VecDeque};

use actix::{Actor, Handler, Message, SyncContext};
use background_jobs_core::{NewJobInfo, ReturnJobInfo, Stats};
use failure::Error;
use log::trace;
use serde_derive::Deserialize;

use crate::{ActixStorage, Worker};

pub struct Server {
    storage: Box<dyn ActixStorage + Send>,
    cache: HashMap<String, VecDeque<Box<dyn Worker + Send>>>,
}

impl Server {
    pub(crate) fn new(storage: impl ActixStorage + Send + 'static) -> Self {
        Server {
            storage: Box::new(storage),
            cache: HashMap::new(),
        }
    }
}

impl Actor for Server {
    type Context = SyncContext<Self>;
}

#[derive(Clone, Debug, Deserialize)]
pub struct NewJob(pub(crate) NewJobInfo);

#[derive(Clone, Debug, Deserialize)]
pub struct ReturningJob(pub(crate) ReturnJobInfo);

pub struct RequestJob(pub(crate) Box<dyn Worker + Send + 'static>);

pub struct CheckDb;

pub struct GetStats;

impl Message for NewJob {
    type Result = Result<(), Error>;
}

impl Message for ReturningJob {
    type Result = Result<(), Error>;
}

impl Message for RequestJob {
    type Result = Result<(), Error>;
}

impl Message for CheckDb {
    type Result = ();
}

impl Message for GetStats {
    type Result = Result<Stats, Error>;
}

impl Handler<NewJob> for Server {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: NewJob, _: &mut Self::Context) -> Self::Result {
        let queue = msg.0.queue().to_owned();
        let ready = msg.0.is_ready();
        self.storage.new_job(msg.0)?;

        if ready {
            let entry = self.cache.entry(queue.clone()).or_insert(VecDeque::new());

            if let Some(worker) = entry.pop_front() {
                if let Ok(Some(job)) = self.storage.request_job(&queue, worker.id()) {
                    worker.process_job(job);
                } else {
                    entry.push_back(worker);
                }
            }
        }

        Ok(())
    }
}

impl Handler<ReturningJob> for Server {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: ReturningJob, _: &mut Self::Context) -> Self::Result {
        self.storage.return_job(msg.0).map_err(|e| e.into())
    }
}

impl Handler<RequestJob> for Server {
    type Result = Result<(), Error>;

    fn handle(&mut self, RequestJob(worker): RequestJob, _: &mut Self::Context) -> Self::Result {
        trace!("Worker {} requested job", worker.id());
        let job = self.storage.request_job(worker.queue(), worker.id())?;

        if let Some(job) = job {
            worker.process_job(job.clone());
        } else {
            trace!(
                "storing worker {} for queue {}",
                worker.id(),
                worker.queue()
            );
            let entry = self
                .cache
                .entry(worker.queue().to_owned())
                .or_insert(VecDeque::new());
            entry.push_back(worker);
        }

        Ok(())
    }
}

impl Handler<CheckDb> for Server {
    type Result = ();

    fn handle(&mut self, _: CheckDb, _: &mut Self::Context) -> Self::Result {
        trace!("Checkdb");

        for (queue, workers) in self.cache.iter_mut() {
            while !workers.is_empty() {
                if let Some(worker) = workers.pop_front() {
                    if let Ok(Some(job)) = self.storage.request_job(queue, worker.id()) {
                        worker.process_job(job);
                    } else {
                        workers.push_back(worker);
                        break;
                    }
                }
            }
        }
    }
}

impl Handler<GetStats> for Server {
    type Result = Result<Stats, Error>;

    fn handle(&mut self, _: GetStats, _: &mut Self::Context) -> Self::Result {
        self.storage.get_stats().map_err(|e| e.into())
    }
}
