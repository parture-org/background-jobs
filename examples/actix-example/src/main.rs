use actix_rt::Arbiter;
use anyhow::Error;
use background_jobs::{create_server_in_arbiter, ActixJob, MaxRetries, WorkerConfig};
use background_jobs_sled_storage::Storage;
use chrono::{Duration, Utc};
use std::{
    future::{ready, Future, Ready},
    pin::Pin,
};

const DEFAULT_QUEUE: &str = "default";

#[derive(Clone, Debug)]
pub struct MyState {
    pub app_name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct MyJob {
    some_usize: usize,
    other_usize: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PanickingJob;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct LongJob;

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();
    // Set up our Storage
    let db = sled::Config::new().temporary(true).open()?;
    let storage = Storage::new(db)?;

    let arbiter = Arbiter::new();

    // Start the application server. This guards access to to the jobs store
    let queue_handle = create_server_in_arbiter(&arbiter, storage);

    // Configure and start our workers
    WorkerConfig::new(move || MyState::new("My App"))
        .register::<LongJob>()
        .register::<PanickingJob>()
        .register::<MyJob>()
        .set_worker_count(DEFAULT_QUEUE, 16)
        .start_in_arbiter(&arbiter, queue_handle.clone());

    // Queue some panicking job
    for _ in 0..32 {
        queue_handle.queue(PanickingJob)?;
    }

    // Queue our jobs
    queue_handle.queue(MyJob::new(1, 2))?;
    queue_handle.queue(MyJob::new(3, 4))?;
    queue_handle.queue(MyJob::new(5, 6))?;
    queue_handle.schedule(MyJob::new(7, 8), Utc::now() + Duration::seconds(2))?;
    queue_handle.queue(LongJob)?;

    // Block on Actix
    actix_rt::signal::ctrl_c().await?;
    arbiter.stop();
    let _ = arbiter.join();
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

#[async_trait::async_trait]
impl ActixJob for MyJob {
    type State = MyState;
    type Future = Ready<Result<(), Error>>;

    // The name of the job. It is super important that each job has a unique name,
    // because otherwise one job will overwrite another job when they're being
    // registered.
    const NAME: &'static str = "MyJob";

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

    fn run(self, state: MyState) -> Self::Future {
        println!("{}: args, {:?}", state.app_name, self);

        ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl ActixJob for LongJob {
    type State = MyState;
    type Future = Pin<Box<dyn Future<Output = Result<(), Error>>>>;

    const NAME: &'static str = "LongJob";

    const QUEUE: &'static str = DEFAULT_QUEUE;

    const MAX_RETRIES: MaxRetries = MaxRetries::Count(0);

    fn run(self, _: MyState) -> Self::Future {
        Box::pin(async move {
            actix_rt::time::sleep(std::time::Duration::from_secs(120)).await;
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl ActixJob for PanickingJob {
    type State = MyState;
    type Future = Ready<Result<(), Error>>;

    const NAME: &'static str = "PanickingJob";

    const QUEUE: &'static str = DEFAULT_QUEUE;

    const MAX_RETRIES: MaxRetries = MaxRetries::Count(0);

    fn run(self, _: MyState) -> Self::Future {
        panic!("A panicking job does not stop others from running")
    }
}
