use std::sync::Arc;

use actix::{
    fut::{wrap_future, ActorFuture},
    Actor, Addr, AsyncContext, Context, Handler, Message,
};
use background_jobs_core::{JobInfo, ProcessorMap, Storage};
use log::info;

use crate::{RequestJob, ReturningJob, Server};

pub struct ProcessJob {
    job: JobInfo,
}

impl ProcessJob {
    pub fn new(job: JobInfo) -> Self {
        ProcessJob { job }
    }
}

impl Message for ProcessJob {
    type Result = ();
}

pub struct LocalWorker<S, State>
where
    S: Storage + 'static,
    State: Clone + 'static,
{
    id: u64,
    queue: String,
    processors: Arc<ProcessorMap<State>>,
    server: Addr<Server<S, LocalWorker<S, State>>>,
}

impl<S, State> LocalWorker<S, State>
where
    S: Storage + 'static,
    State: Clone + 'static,
{
    pub fn new(
        id: u64,
        queue: String,
        processors: Arc<ProcessorMap<State>>,
        server: Addr<Server<S, Self>>,
    ) -> Self {
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
    S: Storage + 'static,
    State: Clone + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.server
            .do_send(RequestJob::new(self.id, &self.queue, ctx.address()));
    }
}

impl<S, State> Handler<ProcessJob> for LocalWorker<S, State>
where
    S: Storage + 'static,
    State: Clone + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: ProcessJob, ctx: &mut Self::Context) -> Self::Result {
        info!("Worker {} processing job {}", self.id, msg.job.id());
        let fut =
            wrap_future::<_, Self>(self.processors.process_job(msg.job)).map(|job, actor, ctx| {
                actor.server.do_send(ReturningJob(job));
                actor
                    .server
                    .do_send(RequestJob::new(actor.id, &actor.queue, ctx.address()));
            });

        ctx.spawn(fut);
    }
}
