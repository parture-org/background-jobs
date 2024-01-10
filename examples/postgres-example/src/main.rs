use actix_rt::Arbiter;
use background_jobs::{
    postgres::Storage, ActixSpawner, MaxRetries, UnsendJob as Job, WorkerConfig,
};
// use background_jobs_sled_storage::Storage;
use std::{
    future::{ready, Ready},
    time::{Duration, SystemTime},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_QUEUE: &str = "default";

#[derive(Clone, Debug)]
pub struct MyState {
    pub app_name: String,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct MyJob {
    some_u64: u64,
    other_u64: u64,
}

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .init();

    // Set up our Storage
    let storage =
        Storage::connect("postgres://postgres:postgres@localhost:5432/db".parse()?).await?;

    let arbiter = Arbiter::new();

    // Configure and start our workers
    let queue_handle =
        WorkerConfig::new_in_arbiter(arbiter.handle(), storage, |_| MyState::new("My App"))
            .register::<MyJob>()
            .set_worker_count(DEFAULT_QUEUE, 16)
            .start();

    // Queue our jobs
    queue_handle.queue(MyJob::new(1, 2)).await?;
    queue_handle.queue(MyJob::new(3, 4)).await?;
    queue_handle.queue(MyJob::new(5, 6)).await?;
    for i in 0..20 {
        queue_handle
            .schedule(
                MyJob::new(7 + i, 8 + i),
                SystemTime::now() + Duration::from_secs(i),
            )
            .await?;
    }

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
    pub fn new(some_u64: u64, other_u64: u64) -> Self {
        MyJob {
            some_u64,
            other_u64,
        }
    }
}

impl Job for MyJob {
    type State = MyState;
    type Future = Ready<anyhow::Result<()>>;
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

        ready(Ok(()))
    }
}
