use actix::System;
use background_jobs::{ServerConfig, SledStorage, WorkerConfig, Processor, Job, Backoff, MaxRetries};
use failure::Error;
use futures::{Future, future::ok};
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

#[derive(Clone, Debug)]
pub struct MyProcessor;

fn main() -> Result<(), Error> {
    let sys = System::new("my-actix-system");

    let db = Db::start_default("my-sled-db")?;
    let storage = SledStorage::new(db)?;

    let queue_handle = ServerConfig::new(storage).start();

    let mut worker_config = WorkerConfig::new(move || MyState::new("My App"));
    worker_config.register(MyProcessor);
    worker_config.set_processor_count(DEFAULT_QUEUE, 16);
    worker_config.start(queue_handle.clone());

    queue_handle.queue::<MyProcessor>(MyJob::new(1, 2))?;
    queue_handle.queue::<MyProcessor>(MyJob::new(3, 4))?;
    queue_handle.queue::<MyProcessor>(MyJob::new(5, 6))?;

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
