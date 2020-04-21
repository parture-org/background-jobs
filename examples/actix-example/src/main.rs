use anyhow::Error;
use background_jobs::{create_server, Job, MaxRetries, WorkerConfig};
use futures::future::{ok, Ready};

const DEFAULT_QUEUE: &'static str = "default";

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
    env_logger::init();
    // Set up our Storage
    // For this example, we use the default in-memory storage mechanism
    use background_jobs::memory_storage::Storage;
    let storage = Storage::new();

    /*
    // Optionally, a storage backend using the Sled database is provided
    use background_jobs::sled_storage::Storage;
    use sled_extensions::Db;
    let db = Db::open("my-sled-db")?;
    let storage = Storage::new(db)?;
    */

    // Start the application server. This guards access to to the jobs store
    let queue_handle = create_server(storage);

    // Configure and start our workers
    WorkerConfig::new(move || MyState::new("My App"))
        .register::<MyJob>()
        .set_worker_count(DEFAULT_QUEUE, 16)
        .start(queue_handle.clone());

    // Queue our jobs
    queue_handle.queue(MyJob::new(1, 2))?;
    queue_handle.queue(MyJob::new(3, 4))?;
    queue_handle.queue(MyJob::new(5, 6))?;

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
        println!("{}: args, {:?}", state.app_name, self);

        ok(())
    }
}
