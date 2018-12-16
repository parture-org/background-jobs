use std::sync::Arc;

use actix::{
    fut::{wrap_future, ActorFuture},
    Actor, Addr, AsyncContext, Context, Handler, Message,
};
use background_jobs_core::{JobInfo, ProcessorMap};
use log::info;

use crate::{EitherJob, RequestJob, Server};

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

pub struct LocalWorker<State>
where
    State: Clone + Send + Sync + 'static,
{
    id: usize,
    queue: String,
    processors: Arc<ProcessorMap<State>>,
    server: Addr<Server<LocalWorker<State>>>,
}

impl<State> LocalWorker<State>
where
    State: Clone + Send + Sync + 'static,
{
    pub fn new(
        id: usize,
        queue: String,
        processors: Arc<ProcessorMap<State>>,
        server: Addr<Server<Self>>,
    ) -> Self {
        LocalWorker {
            id,
            queue,
            processors,
            server,
        }
    }
}

impl<State> Actor for LocalWorker<State>
where
    State: Clone + Send + Sync + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.server
            .do_send(RequestJob::new(self.id, &self.queue, ctx.address()));
    }
}

impl<State> Handler<ProcessJob> for LocalWorker<State>
where
    State: Clone + Send + Sync + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: ProcessJob, ctx: &mut Self::Context) -> Self::Result {
        info!("Worker {} processing job {}", self.id, msg.job.id());
        let fut =
            wrap_future::<_, Self>(self.processors.process_job(msg.job)).map(|job, actor, ctx| {
                actor.server.do_send(EitherJob::Existing(job));
                actor
                    .server
                    .do_send(RequestJob::new(actor.id, &actor.queue, ctx.address()));
            });

        ctx.spawn(fut);
    }
}
