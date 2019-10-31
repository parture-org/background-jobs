use actix::{
    dev::ToEnvelope,
    fut::{wrap_future, ActorFuture},
    Actor, Addr, AsyncContext, Context, Handler, Message,
};
use background_jobs_core::{JobInfo, CachedProcessorMap};
use log::info;

use crate::{RequestJob, ReturningJob};

pub trait Worker {
    fn process_job(&self, job: JobInfo);

    fn id(&self) -> u64;

    fn queue(&self) -> &str;
}

pub struct LocalWorkerHandle<W>
where
    W: Actor + Handler<ProcessJob>,
    W::Context: ToEnvelope<W, ProcessJob>,
{
    addr: Addr<W>,
    id: u64,
    queue: String,
}

impl<W> Worker for LocalWorkerHandle<W>
where
    W: Actor + Handler<ProcessJob>,
    W::Context: ToEnvelope<W, ProcessJob>,
{
    fn process_job(&self, job: JobInfo) {
        self.addr.do_send(ProcessJob(job));
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn queue(&self) -> &str {
        &self.queue
    }
}

/// A worker that runs on the same system as the jobs server
pub struct LocalWorker<S, State>
where
    S: Actor + Handler<ReturningJob> + Handler<RequestJob>,
    S::Context: ToEnvelope<S, ReturningJob> + ToEnvelope<S, RequestJob>,
    State: Clone + 'static,
{
    id: u64,
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Addr<S>,
}

impl<S, State> LocalWorker<S, State>
where
    S: Actor + Handler<ReturningJob> + Handler<RequestJob>,
    S::Context: ToEnvelope<S, ReturningJob> + ToEnvelope<S, RequestJob>,
    State: Clone + 'static,
{
    /// Create a new local worker
    pub fn new(id: u64, queue: String, processors: CachedProcessorMap<State>, server: Addr<S>) -> Self {
        LocalWorker {
            id,
            queue,
            processors,
            server,
        }
    }
}

impl<S, State> Actor for LocalWorker<S, State>
where
    S: Actor + Handler<ReturningJob> + Handler<RequestJob>,
    S::Context: ToEnvelope<S, ReturningJob> + ToEnvelope<S, RequestJob>,
    State: Clone + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.server.do_send(RequestJob(Box::new(LocalWorkerHandle {
            id: self.id,
            queue: self.queue.clone(),
            addr: ctx.address(),
        })));
    }
}

pub struct ProcessJob(JobInfo);

impl Message for ProcessJob {
    type Result = ();
}

impl<S, State> Handler<ProcessJob> for LocalWorker<S, State>
where
    S: Actor + Handler<ReturningJob> + Handler<RequestJob>,
    S::Context: ToEnvelope<S, ReturningJob> + ToEnvelope<S, RequestJob>,
    State: Clone + 'static,
{
    type Result = ();

    fn handle(&mut self, ProcessJob(job): ProcessJob, ctx: &mut Self::Context) -> Self::Result {
        info!("Worker {} processing job {}", self.id, job.id());
        let fut =
            wrap_future::<_, Self>(self.processors.process_job(job)).map(|job, actor, ctx| {
                actor.server.do_send(ReturningJob(job));
                actor.server.do_send(RequestJob(Box::new(LocalWorkerHandle {
                    id: actor.id,
                    queue: actor.queue.clone(),
                    addr: ctx.address(),
                })));
            });

        ctx.spawn(fut);
    }
}
