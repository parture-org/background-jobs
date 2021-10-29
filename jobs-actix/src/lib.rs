#![deny(missing_docs)]

//! # An Actix-based Jobs Processor
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
//! ```rust,ignore
//! use anyhow::Error;
//! use background_jobs::{create_server, Backoff, Job, MaxRetries, WorkerConfig};
//! use futures::future::{ok, Ready};
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
//! #[actix_rt::main]
//! async fn main() -> Result<(), Error> {
//!     // Set up our Storage
//!     // For this example, we use the default in-memory storage mechanism
//!     use background_jobs::memory_storage::Storage;
//!     let storage = Storage::new();
//!
//!     // Configure and start our workers
//!     let queue_handle = WorkerConfig::new(move || MyState::new("My App"))
//!         .register::<MyJob>()
//!         .set_worker_count(DEFAULT_QUEUE, 16)
//!         .start(storage);
//!
//!     // Queue our jobs
//!     queue_handle.queue(MyJob::new(1, 2))?;
//!     queue_handle.queue(MyJob::new(3, 4))?;
//!     queue_handle.queue(MyJob::new(5, 6))?;
//!
//!     actix_rt::signal::ctrl_c().await?;
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
//! #[async_trait::async_trait]
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
//!     const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);
//!
//!     // When should the job be considered dead
//!     //
//!     // The timeout defines when a job is allowed to be considered dead, and so can be retried
//!     // by the job processor. The value is in milliseconds and defaults to 15,000
//!     const TIMEOUT: i64 = 15_000
//!
//!     async fn run(self, state: MyState) -> Self::Future {
//!         println!("{}: args, {:?}", state.app_name, self);
//!
//!         ok(())
//!     }
//! }
//! ```

use actix_rt::{Arbiter, ArbiterHandle};
use anyhow::Error;
use background_jobs_core::{new_job, new_scheduled_job, Job, ProcessorMap, Stats, Storage};
use chrono::{DateTime, Utc};
use std::{collections::BTreeMap, ops::Deref, sync::Arc, time::Duration};

mod every;
mod server;
mod storage;
mod worker;

use self::{every::every, server::Server, worker::LocalWorkerStarter};

pub use background_jobs_core::ActixJob;

/// Manager for worker threads
///
/// Manager attempts to restart workers as their arbiters die
pub struct Manager {
    // the manager arbiter
    _arbiter: ArbiterDropper,

    // handle for queueing
    queue_handle: QueueHandle,
}

struct ArbiterDropper {
    arbiter: Option<Arbiter>,
}

impl Manager {
    /// Create a new manager to keep jobs alive
    ///
    /// Manager works by startinng a new Arbiter to run jobs, and if that arbiter ever dies, it
    /// spins up another one and spawns the workers again
    fn new<S, State>(storage: S, worker_config: WorkerConfig<State>) -> Self
    where
        S: Storage + Sync + 'static,
        State: Clone,
    {
        let arbiter = Arbiter::new();
        let worker_arbiter = Arbiter::new();
        let notifier = Arc::new(tokio::sync::Notify::new());

        let queue_handle = create_server_managed(storage);

        let drop_notifier = DropNotifier::new(Arc::clone(&notifier));
        let queue_handle_2 = queue_handle.clone();
        arbiter.spawn(async move {
            let queue_handle = queue_handle_2;

            let mut drop_notifier = drop_notifier;
            let mut arbiter = ArbiterDropper {
                arbiter: Some(worker_arbiter),
            };

            loop {
                queue_handle
                    .inner
                    .ticker(arbiter.handle(), drop_notifier.clone());
                worker_config.start_managed(
                    &arbiter.handle(),
                    queue_handle.clone(),
                    &drop_notifier,
                );

                notifier.notified().await;
                // drop_notifier needs to live at least as long as notifier.notified().await
                // in order to ensure we get notified by ticker or a worker, and not ourselves
                drop(drop_notifier);

                // Assume arbiter is dead if we were notified
                let online = arbiter.spawn(async {});
                if online {
                    panic!("Arbiter should be dead by now");
                }

                arbiter = ArbiterDropper {
                    arbiter: Some(Arbiter::new()),
                };

                drop_notifier = DropNotifier::new(Arc::clone(&notifier));
            }
        });

        Manager {
            _arbiter: ArbiterDropper {
                arbiter: Some(arbiter),
            },
            queue_handle,
        }
    }

    /// Retrieve the QueueHandle for the managed workers
    pub fn queue_handle(&self) -> &QueueHandle {
        &self.queue_handle
    }
}

impl Deref for Manager {
    type Target = QueueHandle;

    fn deref(&self) -> &Self::Target {
        &self.queue_handle
    }
}

impl Deref for ArbiterDropper {
    type Target = Arbiter;

    fn deref(&self) -> &Self::Target {
        self.arbiter.as_ref().unwrap()
    }
}

impl Drop for ArbiterDropper {
    fn drop(&mut self) {
        self.stop();
        let _ = self.arbiter.take().unwrap().join();
    }
}

#[derive(Clone)]
struct DropNotifier {
    inner: Arc<std::sync::Mutex<Option<Arc<tokio::sync::Notify>>>>,
}

impl DropNotifier {
    fn new(notify: Arc<tokio::sync::Notify>) -> Self {
        DropNotifier {
            inner: Arc::new(std::sync::Mutex::new(Some(notify))),
        }
    }
}

impl Drop for DropNotifier {
    fn drop(&mut self) {
        if let Some(notifier) = self.inner.lock().unwrap().take() {
            notifier.notify_one();
        }
    }
}

/// Create a new Server
///
/// In previous versions of this library, the server itself was run on it's own dedicated threads
/// and guarded access to jobs via messages. Since we now have futures-aware synchronization
/// primitives, the Server has become an object that gets shared between client threads.
fn create_server_in_arbiter<S>(arbiter: ArbiterHandle, storage: S) -> QueueHandle
where
    S: Storage + Sync + 'static,
{
    let handle = create_server_managed(storage);
    handle.inner.ticker(arbiter, ());
    handle
}

/// Create a new managed Server
///
/// In previous versions of this library, the server itself was run on it's own dedicated threads
/// and guarded access to jobs via messages. Since we now have futures-aware synchronization
/// primitives, the Server has become an object that gets shared between client threads.
fn create_server_managed<S>(storage: S) -> QueueHandle
where
    S: Storage + Sync + 'static,
{
    QueueHandle {
        inner: Server::new(storage),
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
}

impl<State> WorkerConfig<State>
where
    State: Clone + 'static,
{
    /// Create a new WorkerConfig
    ///
    /// The supplied function should return the State required by the jobs intended to be
    /// processed. The function must be sharable between threads, but the state itself does not
    /// have this requirement.
    pub fn new(state_fn: impl Fn() -> State + Send + Sync + 'static) -> Self {
        WorkerConfig {
            processors: ProcessorMap::new(Arc::new(state_fn)),
            queues: BTreeMap::new(),
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

    /// Start the workers in the current arbiter
    ///
    /// This method will panic if not called from an actix runtime
    pub fn start<S: Storage + Send + Sync + 'static>(self, storage: S) -> QueueHandle {
        self.start_in_arbiter(&Arbiter::current(), storage)
    }

    /// Start the workers in the provided arbiter
    pub fn start_in_arbiter<S: Storage + Send + Sync + 'static>(
        self,
        arbiter: &ArbiterHandle,
        storage: S,
    ) -> QueueHandle {
        let queue_handle = create_server_in_arbiter(arbiter.clone(), storage);
        self.start_managed(arbiter, queue_handle.clone(), &());
        queue_handle
    }

    /// Start the workers on a managed arbiter, and return the manager struct
    pub fn managed<S: Storage + Send + Sync + 'static>(self, storage: S) -> Manager {
        Manager::new(storage, self)
    }

    /// Start a workers in a managed way
    fn start_managed<Extras: Clone + Send + 'static>(
        &self,
        arbiter: &ArbiterHandle,
        queue_handle: QueueHandle,
        extras: &Extras,
    ) {
        for (key, count) in self.queues.iter() {
            for _ in 0..*count {
                let queue = key.clone();
                let processors = self.processors.clone();
                let server = queue_handle.inner.clone();

                let extras_2 = extras.clone();

                arbiter.spawn_fn(move || {
                    drop(LocalWorkerStarter::new(
                        queue,
                        processors.cached(),
                        server,
                        extras_2,
                    ));
                });
            }
        }
    }
}

/// A handle to the job server, used for queuing new jobs
///
/// `QueueHandle` should be stored in your application's state in order to allow all parts of your
/// application to spawn jobs.
#[derive(Clone)]
pub struct QueueHandle {
    inner: Server,
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
        self.inner.new_job(job).await?;
        Ok(())
    }

    /// Schedule a job for execution later
    ///
    /// This job will be sent to the server for storage, and will execute after the specified time
    /// and when a worker for the job's queue is free to do so.
    pub async fn schedule<J>(&self, job: J, after: DateTime<Utc>) -> Result<(), Error>
    where
        J: Job,
    {
        let job = new_scheduled_job(job, after)?;
        let server = self.inner.clone();
        server.new_job(job).await?;
        Ok(())
    }

    /// Queues a job for recurring execution
    ///
    /// This job will be added to it's queue on the server once every `Duration`. It will be
    /// processed whenever workers are free to do so.
    pub fn every<J>(&self, duration: Duration, job: J)
    where
        J: Job + Clone + Send + 'static,
    {
        actix_rt::spawn(every(self.clone(), duration, job));
    }

    /// Return an overview of the processor's statistics
    pub async fn get_stats(&self) -> Result<Stats, Error> {
        self.inner.get_stats().await
    }
}
