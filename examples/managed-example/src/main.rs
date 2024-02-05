use actix_rt::Arbiter;
use anyhow::Error;
use background_jobs::{
    actix::{Spawner, WorkerConfig},
    sled::Storage,
    MaxRetries, UnsendJob as Job,
};
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
    some_usize: usize,
    other_usize: usize,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct StopJob;

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .init();

    // Set up our Storage
    let db = sled::Config::new().temporary(true).open()?;
    let storage = Storage::new(db)?;

    // Configure and start our workers
    let manager = WorkerConfig::new_managed(storage, move |_| MyState::new("My App"))
        .register::<StopJob>()
        .register::<MyJob>()
        .set_worker_count(DEFAULT_QUEUE, 16)
        .start();

    // Queue our jobs
    manager.queue(MyJob::new(1, 2)).await?;
    manager.queue(MyJob::new(3, 4)).await?;
    manager.queue(MyJob::new(5, 6)).await?;
    manager
        .schedule(MyJob::new(7, 8), SystemTime::now() + Duration::from_secs(2))
        .await?;

    // Block on Actix
    tracing::info!("Press CTRL^C to continue");
    actix_rt::signal::ctrl_c().await?;

    // kill the current arbiter
    manager.queue(StopJob).await?;

    // Block on Actix
    tracing::info!("Press CTRL^C to continue");
    actix_rt::signal::ctrl_c().await?;

    // See that the workers have respawned
    manager.queue(MyJob::new(1, 2)).await?;
    manager.queue(MyJob::new(3, 4)).await?;
    manager.queue(MyJob::new(5, 6)).await?;
    manager
        .schedule(MyJob::new(7, 8), SystemTime::now() + Duration::from_secs(2))
        .await?;

    // Block on Actix
    tracing::info!("Press CTRL^C to quit");
    actix_rt::signal::ctrl_c().await?;

    drop(manager);

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
    type Error = Error;
    type Future = Ready<Result<(), Error>>;
    type Spawner = Spawner;

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

impl Job for StopJob {
    type State = MyState;
    type Error = Error;
    type Future = Ready<Result<(), Error>>;
    type Spawner = Spawner;

    const NAME: &'static str = "StopJob";
    const QUEUE: &'static str = DEFAULT_QUEUE;
    const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);

    fn run(self, _: MyState) -> Self::Future {
        Arbiter::current().stop();

        ready(Ok(()))
    }
}
