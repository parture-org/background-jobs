use actix::System;
use background_jobs::{
    Backoff, Job, MaxRetries, Processor, ServerConfig, SledStorage, WorkerConfig, QueueHandle,
};
use failure::Error;
use futures::{future::ok, Future};
use serde_derive::{Deserialize, Serialize};
use sled::Db;

const DEFAULT_QUEUE: &'static str = "default";

#[derive(Clone, Debug)]
pub struct MyState {
    pub app_name: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MyJob {
    some_usize: usize,
    other_usize: usize,
}

#[derive(Clone)]
pub struct MyProcessor(pub QueueHandle);

fn main() -> Result<(), Error> {
    let sys = System::new("my-actix-system");

    let db = Db::start_default("my-sled-db")?;
    let storage = SledStorage::new(db)?;

    let queue_handle = ServerConfig::new(storage).thread_count(2).start();

    let processor = MyProcessor(queue_handle.clone());

    WorkerConfig::new(move || MyState::new("My App"))
        .register(processor.clone())
        .set_processor_count(DEFAULT_QUEUE, 16)
        .start(queue_handle.clone());

    processor.queue(MyJob::new(1, 2))?;
    processor.queue(MyJob::new(3, 4))?;
    processor.queue(MyJob::new(5, 6))?;

    sys.run()?;
    Ok(())
}

impl MyState {
    pub fn new(app_name: &str) -> Self {
        MyState {
            app_name: app_name.to_owned(),
        }
    }
}

impl MyJob {
    pub fn new(some_usize: usize, other_usize: usize) -> Self {
        MyJob {
            some_usize,
            other_usize,
        }
    }
}

impl Job<MyState> for MyJob {
    fn run(self, state: MyState) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        println!("{}: args, {:?}", state.app_name, self);

        Box::new(ok(()))
    }
}

impl MyProcessor {
    fn queue(&self, job: <Self as Processor<MyState>>::Job) -> Result<(), Error> {
        self.0.queue::<Self, _>(job)
    }
}

impl Processor<MyState> for MyProcessor {
    // The kind of job this processor should execute
    type Job = MyJob;

    // The name of the processor. It is super important that each processor has a unique name,
    // because otherwise one processor will overwrite another processor when they're being
    // registered.
    const NAME: &'static str = "MyProcessor";

    // The queue that this processor belongs to
    //
    // Workers have the option to subscribe to specific queues, so this is important to
    // determine which worker will call the processor
    //
    // Jobs can optionally override the queue they're spawned on
    const QUEUE: &'static str = DEFAULT_QUEUE;

    // The number of times background-jobs should try to retry a job before giving up
    //
    // Jobs can optionally override this value
    const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);

    // The logic to determine how often to retry this job if it fails
    //
    // Jobs can optionally override this value
    const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);
}
