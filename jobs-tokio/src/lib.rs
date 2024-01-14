#![deny(missing_docs)]

//! # A Tokio-based Jobs Processor
//!
//! This library will spin up as many actors as requested for each processor to process jobs
//! concurrently. Keep in mind that, by default, spawned actors run on the same Arbiter, so in
//! order to achieve parallel execution, multiple Arbiters must be in use.
//!
//! The thread count is used to spawn Synchronous Actors to handle the storage of job
//! information. For storage backends that cannot be parallelized, a thread-count of 1 should be
//! used. By default, the number of cores of the running system is used.
//!
//! ### Example
//! ```rust
//! use anyhow::Error;
//! use background_jobs_core::{Backoff, Job, MaxRetries};
//! use background_jobs_tokio::{TokioTimer, WorkerConfig};
//! use std::future::{ready, Ready};
//!
//! const DEFAULT_QUEUE: &'static str = "default";
//!
//! #[derive(Clone, Debug)]
//! pub struct MyState {
//!     pub app_name: String,
//! }
//!
//! #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
//! pub struct MyJob {
//!     some_usize: usize,
//!     other_usize: usize,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     // Set up our Storage
//!     // For this example, we use the default in-memory storage mechanism
//!     use background_jobs_core::memory_storage::Storage;
//!     let storage = Storage::new(TokioTimer);
//!
//!     // Configure and start our workers
//!     let queue_handle = WorkerConfig::new(storage, move |_| MyState::new("My App"))
//!         .register::<MyJob>()
//!         .set_worker_count(DEFAULT_QUEUE, 16)
//!         .start();
//!
//!     // Queue our jobs
//!     queue_handle.queue(MyJob::new(1, 2)).await?;
//!     queue_handle.queue(MyJob::new(3, 4)).await?;
//!     queue_handle.queue(MyJob::new(5, 6)).await?;
//!
//!     // tokio::signal::ctrl_c().await?;
//!
//!     drop(queue_handle);
//!
//!     Ok(())
//! }
//!
//! impl MyState {
//!     pub fn new(app_name: &str) -> Self {
//!         MyState {
//!             app_name: app_name.to_owned(),
//!         }
//!     }
//! }
//!
//! impl MyJob {
//!     pub fn new(some_usize: usize, other_usize: usize) -> Self {
//!         MyJob {
//!             some_usize,
//!             other_usize,
//!         }
//!     }
//! }
//!
//! impl Job for MyJob {
//!     type State = MyState;
//!     type Future = Ready<Result<(), Error>>;
//!
//!     // The name of the job. It is super important that each job has a unique name,
//!     // because otherwise one job will overwrite another job when they're being
//!     // registered.
//!     const NAME: &'static str = "MyJob";
//!
//!     // The queue that this processor belongs to
//!     //
//!     // Workers have the option to subscribe to specific queues, so this is important to
//!     // determine which worker will call the processor
//!     //
//!     // Jobs can optionally override the queue they're spawned on
//!     const QUEUE: &'static str = DEFAULT_QUEUE;
//!
//!     // The number of times background-jobs should try to retry a job before giving up
//!     //
//!     // This value defaults to MaxRetries::Count(5)
//!     // Jobs can optionally override this value
//!     const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);
//!
//!     // The logic to determine how often to retry this job if it fails
//!     //
//!     // This value defaults to Backoff::Exponential(2)
//!     // Jobs can optionally override this value
//!     const BACKOFF: Backoff = Backoff::Exponential(2);
//!
//!     // This is important for allowing the job server to reap processes that were started but never
//!     // completed.
//!     //
//!     // Defaults to 5 seconds
//!     const HEARTBEAT_INTERVAL: u64 = 5_000;
//!
//!     fn run(self, state: MyState) -> Self::Future {
//!         println!("{}: args, {:?}", state.app_name, self);
//!
//!         ready(Ok(()))
//!     }
//! }
//! ```

use anyhow::Error;
use background_jobs_core::{
    memory_storage::Timer, new_job, new_scheduled_job, Job, ProcessorMap, Storage as StorageTrait,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use tokio::task::{JoinHandle, JoinSet};

mod every;
mod spawn;
mod storage;
mod worker;

use self::{every::every, storage::Storage};

/// A timer implementation for the Memory Storage backend
#[derive(Debug, Clone)]
pub struct TokioTimer;

#[async_trait::async_trait]
impl Timer for TokioTimer {
    async fn timeout<F>(&self, duration: Duration, future: F) -> Result<F::Output, ()>
    where
        F: std::future::Future + Send + Sync,
    {
        tokio::time::timeout(duration, future).await.map_err(|_| ())
    }
}

/// Create a new Server
fn create_server<S>(storage: S) -> QueueHandle
where
    S: StorageTrait + Sync + 'static,
{
    QueueHandle {
        inner: Storage::new(storage),
        manager_handle: Some(Arc::new(Mutex::new(None))),
    }
}

/// Worker Configuration
///
/// This type is used for configuring and creating workers to process jobs. Before starting the
/// workers, register `Job` types with this struct. This worker registration allows for
/// different worker processes to handle different sets of workers.
#[derive(Clone)]
pub struct WorkerConfig<State>
where
    State: Clone + 'static,
{
    processors: ProcessorMap<State>,
    queues: BTreeMap<String, u64>,
    queue_handle: QueueHandle,
}

impl<State> WorkerConfig<State>
where
    State: Send + Clone + 'static,
{
    /// Create a new WorkerConfig
    ///
    /// The supplied function should return the State required by the jobs intended to be
    /// processed. The function must be sharable between threads, but the state itself does not
    /// have this requirement.
    pub fn new<S: StorageTrait + Send + Sync + 'static>(
        storage: S,
        state_fn: impl Fn(QueueHandle) -> State + Send + Sync + 'static,
    ) -> Self {
        let queue_handle = create_server(storage);
        let q2 = queue_handle.clone();

        WorkerConfig {
            processors: ProcessorMap::new(Arc::new(move || state_fn(q2.clone()))),
            queues: BTreeMap::new(),
            queue_handle,
        }
    }

    /// Register a `Job` with the worker
    ///
    /// This enables the worker to handle jobs associated with this processor. If a processor is
    /// not registered, none of it's jobs will be run, even if another processor handling the same
    /// job queue is registered.
    pub fn register<J>(mut self) -> Self
    where
        J: Job<State = State>,
    {
        self.queues.insert(J::QUEUE.to_owned(), 4);
        self.processors.register::<J>();
        self
    }

    /// Set the number of workers to run for a given queue
    ///
    /// This does not spin up any additional threads. The `Arbiter` the workers are spawned onto
    /// will handle processing all workers, regardless of how many are configured.
    ///
    /// By default, 4 workers are spawned
    pub fn set_worker_count(mut self, queue: &str, count: u64) -> Self {
        self.queues.insert(queue.to_owned(), count);
        self
    }

    /// Start the workers in the provided arbiter
    pub fn start(self) -> std::io::Result<QueueHandle> {
        let Self {
            processors,
            queues,
            queue_handle,
        } = self;

        let mut sets = HashMap::new();

        for (key, count) in queues.iter() {
            let mut set = JoinSet::new();

            for _ in 0..*count {
                let queue = key.clone();
                let processors = processors.clone();
                let server = queue_handle.inner.clone();

                spawn::spawn_in(
                    &mut set,
                    "local-worker",
                    worker::local_worker(queue, processors.clone(), server),
                )?;
            }

            sets.insert(key.clone(), set);
        }

        let server = queue_handle.inner.clone();

        let manager_task = crate::spawn::spawn("set-supervisor", async move {
            let mut superset = JoinSet::new();

            for (queue, mut set) in sets {
                let server = server.clone();
                let processors = processors.clone();

                if let Err(e) = spawn::spawn_in(&mut superset, "worker-supervisor", async move {
                    while let Some(_) = set.join_next().await {
                        metrics::counter!("background-jobs.tokio.worker.finished", "queue" => queue.clone())
                                .increment(1);

                        tracing::warn!("worker closed, spawning another");

                        if let Err(e) = spawn::spawn_in(
                            &mut set,
                            "local-worker",
                            worker::local_worker(queue.clone(), processors.clone(), server.clone()),
                        ) {
                            tracing::warn!("Failed to respawn worker: {e}");
                            break;
                        }
                        metrics::counter!("background-jobs.tokio.worker.restart").increment(1);
                    }
                }) {
                    tracing::warn!("Failed to spawn worker supervisor: {e}");
                    break;
                }
            }

            let mut count = 0;
            while superset.join_next().await.is_some() {
                count += 1;
                tracing::info!("Joined worker-supervisor {count}");
            }
        })?;

        *queue_handle
            .manager_handle
            .as_ref()
            .unwrap()
            .lock()
            .unwrap() = Some(manager_task);

        Ok(queue_handle)
    }
}

/// A handle to the job server, used for queuing new jobs
///
/// `QueueHandle` should be stored in your application's state in order to allow all parts of your
/// application to spawn jobs.
#[derive(Clone)]
pub struct QueueHandle {
    inner: Storage,
    manager_handle: Option<Arc<Mutex<Option<JoinHandle<()>>>>>,
}

impl QueueHandle {
    /// Queues a job for execution
    ///
    /// This job will be sent to the server for storage, and will execute whenever a worker for the
    /// job's queue is free to do so.
    pub async fn queue<J>(&self, job: J) -> Result<(), Error>
    where
        J: Job,
    {
        let job = new_job(job)?;
        self.inner.push(job).await?;
        Ok(())
    }

    /// Schedule a job for execution later
    ///
    /// This job will be sent to the server for storage, and will execute after the specified time
    /// and when a worker for the job's queue is free to do so.
    pub async fn schedule<J>(&self, job: J, after: SystemTime) -> Result<(), Error>
    where
        J: Job,
    {
        let job = new_scheduled_job(job, after)?;
        self.inner.push(job).await?;
        Ok(())
    }

    /// Queues a job for recurring execution
    ///
    /// This job will be added to it's queue on the server once every `Duration`. It will be
    /// processed whenever workers are free to do so.
    pub fn every<J>(&self, duration: Duration, job: J) -> std::io::Result<()>
    where
        J: Job + Clone + Send + 'static,
    {
        spawn::spawn("every", every(self.clone(), duration, job)).map(|_| ())
    }
}

impl Drop for QueueHandle {
    fn drop(&mut self) {
        if let Some(handle) = self
            .manager_handle
            .take()
            .and_then(Arc::into_inner)
            .and_then(|m| m.lock().unwrap().take())
        {
            tracing::debug!("Dropping last QueueHandle");
            handle.abort();
        }
    }
}
