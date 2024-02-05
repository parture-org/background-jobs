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
//! ```rust
//! use background_jobs_core::{Backoff, Job, MaxRetries, BoxError};
//! use background_jobs_actix::{ActixTimer, WorkerConfig};
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
//! #[actix_rt::main]
//! async fn main() -> Result<(), BoxError> {
//!     // Set up our Storage
//!     // For this example, we use the default in-memory storage mechanism
//!     use background_jobs_core::memory_storage::Storage;
//!     let storage = Storage::new(ActixTimer);
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
//!     // actix_rt::signal::ctrl_c().await?;
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
//!     type Future = Ready<Result<(), BoxError>>;
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

use actix_rt::{Arbiter, ArbiterHandle};
use background_jobs_core::{
    memory_storage::Timer, new_job, new_scheduled_job, BoxError, Job, ProcessorMap, Storage,
};
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    num::NonZeroUsize,
    ops::Deref,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::Notify;

mod actix_job;
mod every;
mod server;
mod spawn;
mod storage;
mod worker;

use self::{every::every, server::Server};

pub use actix_job::ActixSpawner;

/// A timer implementation for the Memory Storage backend
#[derive(Debug, Clone)]
pub struct ActixTimer;

#[async_trait::async_trait]
impl Timer for ActixTimer {
    async fn timeout<F>(&self, duration: Duration, future: F) -> Result<F::Output, ()>
    where
        F: std::future::Future + Send + Sync,
    {
        tokio::time::timeout(duration, future).await.map_err(|_| ())
    }
}

/// Manager for worker threads
///
/// Manager attempts to restart workers as their arbiters die. Dropping the manager kills the
/// workers
pub struct Manager {
    // the manager arbiter
    arbiter: Option<Arbiter>,

    // handle for queueing
    queue_handle: QueueHandle,
}

impl Manager {
    /// Create a new manager to keep jobs alive
    ///
    /// Manager works by startinng a new Arbiter to run jobs, and if that arbiter ever dies, it
    /// spins up another one and spawns the workers again
    fn new<State>(worker_config: WorkerConfig<State, Managed>, thread_count: NonZeroUsize) -> Self
    where
        State: Clone,
    {
        let manager_arbiter = Arbiter::new();
        let queue_handle = worker_config.queue_handle.clone();

        for i in 0..thread_count.into() {
            let worker_config = worker_config.clone();

            manager_arbiter.spawn(async move {
                let mut worker_arbiter = ArbiterDropper::new();

                loop {
                    let notifier = DropNotifier::default();
                    worker_config.start_managed(&worker_arbiter.handle(), &());

                    let notified = notifier.notify.notified();

                    let drop_notifier = notifier.clone();
                    worker_arbiter.spawn(async move {
                        std::future::pending::<()>().await;
                        drop(drop_notifier);
                    });

                    notified.await;

                    metrics::counter!("background-jobs.actix.worker-arbiter.restart", "number" => i.to_string()).increment(1);
                    tracing::warn!("Recovering from dead worker arbiter");

                    drop(worker_arbiter);

                    worker_arbiter = ArbiterDropper::new();
                }
            });
        }

        Manager {
            arbiter: Some(manager_arbiter),
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

impl Drop for Manager {
    fn drop(&mut self) {
        tracing::warn!("Dropping manager, tearing down workers");
        if let Some(arbiter) = self.arbiter.take() {
            arbiter.stop();
            let _ = arbiter.join();
        }
    }
}

#[derive(Clone, Default)]
struct DropNotifier {
    notify: Arc<Notify>,
}

impl Drop for DropNotifier {
    fn drop(&mut self) {
        tracing::warn!("DropNotifier dropped - Arbiter tearing down");
        self.notify.notify_waiters();
    }
}

struct ArbiterDropper {
    arbiter: Option<Arbiter>,
}

impl ArbiterDropper {
    fn new() -> Self {
        Self {
            arbiter: Some(Arbiter::new()),
        }
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
        tracing::warn!("Stopping and joining arbiter");
        let arbiter = self.arbiter.take().unwrap();
        arbiter.stop();
        let _ = arbiter.join();
        tracing::warn!("Joined");
    }
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

/// Marker type for Unmanaged workers
#[derive(Clone)]
pub struct Unmanaged;
/// Marker type for Managed workers
#[derive(Clone)]
pub struct Managed;

/// Worker Configuration
///
/// This type is used for configuring and creating workers to process jobs. Before starting the
/// workers, register `Job` types with this struct. This worker registration allows for
/// different worker processes to handle different sets of workers.
#[derive(Clone)]
pub struct WorkerConfig<State, M>
where
    State: Clone + 'static,
{
    processors: ProcessorMap<State>,
    queues: BTreeMap<String, u64>,
    arbiter: Option<ArbiterHandle>,
    queue_handle: QueueHandle,
    managed: PhantomData<M>,
}

impl<State> WorkerConfig<State, Managed>
where
    State: Clone + 'static,
{
    /// Create a new managed WorkerConfig
    ///
    /// The supplied function should return the State required by the jobs intended to be
    /// processed. The function must be sharable between threads, but the state itself does not
    /// have this requirement.
    pub fn new_managed<S: Storage + Send + Sync + 'static>(
        storage: S,
        state_fn: impl Fn(QueueHandle) -> State + Send + Sync + 'static,
    ) -> Self {
        let queue_handle = create_server_managed(storage);
        let q2 = queue_handle.clone();

        WorkerConfig {
            processors: ProcessorMap::new(Arc::new(move || state_fn(q2.clone()))),
            queues: BTreeMap::new(),
            arbiter: None,
            queue_handle,
            managed: PhantomData,
        }
    }

    /// Start the workers on a managed thread, returning the manager struct
    pub fn start(self) -> Manager {
        Self::start_with_threads(self, NonZeroUsize::try_from(1).expect("nonzero"))
    }

    /// Start the workers on the specified number of managed threads, returning the Manager struct
    pub fn start_with_threads(self, thread_count: NonZeroUsize) -> Manager {
        Manager::new(self, thread_count)
    }
}

impl<State> WorkerConfig<State, Unmanaged>
where
    State: Clone + 'static,
{
    /// Create a new WorkerConfig in the current arbiter
    ///
    /// The supplied function should return the State required by the jobs intended to be
    /// processed. The function must be sharable between threads, but the state itself does not
    /// have this requirement.
    pub fn new<S: Storage + Send + Sync + 'static>(
        storage: S,
        state_fn: impl Fn(QueueHandle) -> State + Send + Sync + 'static,
    ) -> Self {
        Self::new_in_arbiter(Arbiter::current(), storage, state_fn)
    }

    /// Create a new WorkerConfig in the provided arbiter
    ///
    /// The supplied function should return the State required by the jobs intended to be
    /// processed. The function must be sharable between threads, but the state itself does not
    /// have this requirement.
    pub fn new_in_arbiter<S: Storage + Send + Sync + 'static>(
        arbiter: ArbiterHandle,
        storage: S,
        state_fn: impl Fn(QueueHandle) -> State + Send + Sync + 'static,
    ) -> Self {
        let queue_handle = create_server_managed(storage);
        let q2 = queue_handle.clone();

        WorkerConfig {
            processors: ProcessorMap::new(Arc::new(move || state_fn(q2.clone()))),
            queues: BTreeMap::new(),
            arbiter: Some(arbiter),
            queue_handle,
            managed: PhantomData,
        }
    }

    /// Start the workers in the provided arbiter
    pub fn start(self) -> QueueHandle {
        self.start_managed(self.arbiter.as_ref().unwrap(), &());

        self.queue_handle
    }
}

impl<State, M> WorkerConfig<State, M>
where
    State: Clone + 'static,
{
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

    /// Start a workers in a managed way
    fn start_managed<Extras: Clone + Send + 'static>(
        &self,
        arbiter: &ArbiterHandle,
        extras: &Extras,
    ) {
        for (key, count) in self.queues.iter() {
            for _ in 0..*count {
                let queue = key.clone();
                let processors = self.processors.clone();
                let server = self.queue_handle.inner.clone();

                let extras_2 = extras.clone();

                arbiter.spawn_fn(move || {
                    if let Err(e) = spawn::spawn(
                        "local-worker",
                        worker::local_worker(queue, processors.cached(), server, extras_2),
                    ) {
                        tracing::error!("Failed to spawn worker {e}");
                    }
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
    pub async fn queue<J>(&self, job: J) -> Result<(), BoxError>
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
    pub async fn schedule<J>(&self, job: J, after: SystemTime) -> Result<(), BoxError>
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
