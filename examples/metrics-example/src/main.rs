use actix_rt::Arbiter;
use anyhow::Error;
use background_jobs::{
    ActixSpawner,
    MaxRetries,
    // memory_storage::{ActixTimer, Storage},
    UnsendJob as Job,
    WorkerConfig,
};
use background_jobs_sled_storage::Storage;
use std::{future::Future, pin::Pin, time::Duration};
use tracing::info;
use tracing_subscriber::EnvFilter;

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

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));

    // Install the metrics subscriber
    let stats_handle = background_jobs::metrics::install()?;

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .init();

    // Set up our Storage
    let db = sled::Config::new().temporary(true).open()?;
    let storage = Storage::new(db)?;
    // let storage = Storage::new(ActixTimer);

    let arbiter = Arbiter::new();

    // Configure and start our workers
    let queue_handle =
        WorkerConfig::new_in_arbiter(arbiter.handle(), storage, |_| MyState::new("My App"))
            .register::<MyJob>()
            .set_worker_count(DEFAULT_QUEUE, 4)
            .start();

    // Queue our jobs
    for _ in 0..1000 {
        queue_handle.queue(MyJob::new(0, 0)).await?;
    }

    actix_rt::spawn(async move {
        loop {
            actix_rt::time::sleep(Duration::from_millis(500)).await;
            // Get the current stats for the background jobs processor
            println!("{:?}", stats_handle.get());
        }
    });

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

impl Job for MyJob {
    type State = MyState;
    type Future = Pin<Box<dyn Future<Output = Result<(), Error>> + 'static>>;
    type Spawner = ActixSpawner;

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
        info!("{}: args, {:?}", state.app_name, self);

        Box::pin(async move {
            actix_rt::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        })
    }
}
