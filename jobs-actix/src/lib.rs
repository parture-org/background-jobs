#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;

use std::{
    fs::create_dir_all,
    path::{Path, PathBuf},
    time::Duration,
};

use actix::{
    fut::wrap_future, utils::IntervalFunc, Actor, ActorFuture, ActorStream, Addr, AsyncContext,
    Context, ContextFutureSpawner, Handler, Message, ResponseFuture, SyncArbiter, SyncContext,
};
use failure::Error;
use futures::Future;
use jobs_core::{JobInfo, Processor, Processors, Storage};

fn coerce<I, E, F>(res: Result<Result<I, E>, F>) -> Result<I, E>
where
    F: Into<E>,
{
    match res {
        Ok(inner_res) => inner_res,
        Err(f) => Err(f.into()),
    }
}

#[derive(Clone)]
pub struct KvActor {
    storage: Storage,
}

impl KvActor {
    pub fn init(runner_id: usize, db_path: PathBuf) -> Result<Self, Error> {
        create_dir_all(db_path.clone())?;

        let storage = Storage::init(runner_id, db_path)?;

        let actor = KvActor { storage };

        Ok(actor)
    }

    pub fn store_job(&self, job: JobInfo) -> Result<(), Error> {
        self.storage.store_job(job)?;

        Ok(())
    }

    pub fn dequeue_jobs(&self, limit: usize, queue: &str) -> Result<Vec<JobInfo>, Error> {
        let jobs = self.storage.dequeue_job(limit, queue)?;

        Ok(jobs)
    }
}

impl Actor for KvActor {
    type Context = SyncContext<Self>;
}

impl Handler<StoreJob> for KvActor {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: StoreJob, _: &mut Self::Context) -> Self::Result {
        self.store_job(msg.0)
    }
}

impl Handler<DequeueJobs> for KvActor {
    type Result = Result<Vec<JobInfo>, Error>;

    fn handle(&mut self, msg: DequeueJobs, _: &mut Self::Context) -> Self::Result {
        self.dequeue_jobs(msg.0, &msg.1)
    }
}

#[derive(Debug)]
pub struct StoreJob(JobInfo);

impl Message for StoreJob {
    type Result = Result<(), Error>;
}

#[derive(Debug)]
pub struct DequeueJobs(usize, String);

impl Message for DequeueJobs {
    type Result = Result<Vec<JobInfo>, Error>;
}

pub struct JobsActor {
    store: Addr<KvActor>,
}

impl JobsActor {
    fn new(store: Addr<KvActor>) -> Self {
        JobsActor { store }
    }

    fn store_job(&mut self, job: JobInfo) -> impl Future<Item = (), Error = Error> {
        self.store.send(StoreJob(job)).then(coerce)
    }
}

impl Actor for JobsActor {
    type Context = Context<Self>;
}

impl Handler<QueueJob> for JobsActor {
    type Result = ResponseFuture<(), Error>;

    fn handle(&mut self, msg: QueueJob, _: &mut Self::Context) -> Self::Result {
        Box::new(self.store_job(msg.0))
    }
}

#[derive(Debug)]
pub struct QueueJob(pub JobInfo);

impl Message for QueueJob {
    type Result = Result<(), Error>;
}

pub struct ProcessorActor {
    processors: Processors,
    store: Addr<KvActor>,
}

impl ProcessorActor {
    fn new(processors: Processors, store: Addr<KvActor>) -> Self {
        ProcessorActor { processors, store }
    }

    fn process_job(&mut self, ctx: &mut Context<Self>) {
        Self::fetch_job((), self, ctx)
            .and_then(Self::run_job)
            .and_then(Self::return_job)
            .map(Self::call_process_job)
            .spawn(ctx);
    }

    fn fetch_job(
        _: (),
        actor: &mut Self,
        _: &mut Context<Self>,
    ) -> impl ActorFuture<Item = JobInfo, Error = (), Actor = Self> {
        wrap_future(
            actor
                .store
                .send(DequeueJobs(1, "default".to_owned()))
                .then(coerce)
                .map_err(|e| error!("Error fetching jobs, {}", e))
                .and_then(|jobs| jobs.into_iter().next().ok_or(())),
        )
    }

    fn run_job(
        job: JobInfo,
        actor: &mut Self,
        _: &mut Context<Self>,
    ) -> impl ActorFuture<Item = JobInfo, Error = (), Actor = Self> {
        wrap_future(actor.processors.process_job(job))
    }

    fn return_job(
        job: JobInfo,
        actor: &mut Self,
        _: &mut Context<Self>,
    ) -> impl ActorFuture<Item = (), Error = (), Actor = Self> {
        wrap_future(
            actor
                .store
                .send(StoreJob(job))
                .then(coerce)
                .map_err(|e| error!("Error returning jobs, {}", e)),
        )
    }

    fn call_process_job(_: (), _: &mut Self, ctx: &mut Context<Self>) {
        ctx.address().do_send(ProcessJob);
    }
}

impl Actor for ProcessorActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        IntervalFunc::new(Duration::from_millis(500), Self::process_job)
            .finish()
            .spawn(ctx);
    }
}

impl Handler<ProcessJob> for ProcessorActor {
    type Result = ();

    fn handle(&mut self, _: ProcessJob, ctx: &mut Self::Context) -> Self::Result {
        self.process_job(ctx)
    }
}

struct ProcessJob;

impl Message for ProcessJob {
    type Result = ();
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "No jobs to process")]
pub struct NoJobs;

#[derive(Clone, Debug, Fail)]
#[fail(display = "Error processing jobs")]
pub struct ProcessError;

#[derive(Clone, Debug, Fail)]
#[fail(display = "Error in Interval")]
pub struct IntervalError;

pub struct JobsBuilder {
    num_processors: usize,
    runner_id: usize,
    db_path: PathBuf,
    processors: Vec<Processors>,
}

impl JobsBuilder {
    pub fn new<P: AsRef<Path>>(runner_id: usize, num_processors: usize, db_path: P) -> Self {
        JobsBuilder {
            num_processors,
            runner_id,
            db_path: db_path.as_ref().to_owned(),
            processors: (0..num_processors).map(|_| Processors::new()).collect(),
        }
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        for processors in self.processors.iter_mut() {
            processors.register_processor(processor.clone());
        }
    }

    pub fn build(self) -> Result<Addr<JobsActor>, Error> {
        let JobsBuilder {
            num_processors,
            runner_id,
            db_path,
            processors,
        } = self;

        let kv_actor = KvActor::init(runner_id, db_path)?;
        let store = SyncArbiter::start(num_processors + 1, move || kv_actor.clone());

        for processors in processors {
            ProcessorActor::new(processors, store.clone()).start();
        }

        let actor = JobsActor::new(store).start();

        Ok(actor)
    }
}
