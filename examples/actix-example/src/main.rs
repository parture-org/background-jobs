use anyhow::Error;
use background_jobs::{create_server, ActixJob as Job, MaxRetries, WorkerConfig};
use background_jobs_sled_storage::Storage;
use chrono::{Duration, Utc};
use std::future::{ready, Ready};
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
pub struct PanickingJob;

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .init();

    // Set up our Storage
    let db = sled::Config::new().temporary(true).open()?;
    let storage = Storage::new(db)?;

    // Start the application server. This guards access to to the jobs store
    let queue_handle = create_server(storage);

    // Configure and start our workers
    WorkerConfig::new(move || MyState::new("My App"))
        .register::<PanickingJob>()
        .register::<MyJob>()
        .set_worker_count(DEFAULT_QUEUE, 16)
        .start(queue_handle.clone());

    // Queue some panicking job
    for _ in 0..32 {
        queue_handle.queue(PanickingJob).await?;
    }

    // Queue our jobs
    queue_handle.queue(MyJob::new(1, 2)).await?;
    queue_handle.queue(MyJob::new(3, 4)).await?;
    queue_handle.queue(MyJob::new(5, 6)).await?;
    queue_handle
        .schedule(MyJob::new(7, 8), Utc::now() + Duration::seconds(2))
        .await?;

    // Block on Actix
    actix_rt::signal::ctrl_c().await?;
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
impl Job for MyJob {
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
        info!("{}: args, {:?}", state.app_name, self);

        ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl Job for PanickingJob {
    type State = MyState;
    type Future = Ready<Result<(), Error>>;

    const NAME: &'static str = "PanickingJob";

    const QUEUE: &'static str = DEFAULT_QUEUE;

    const MAX_RETRIES: MaxRetries = MaxRetries::Count(0);

    fn run(self, _: MyState) -> Self::Future {
        panic!("boom")
    }
}
