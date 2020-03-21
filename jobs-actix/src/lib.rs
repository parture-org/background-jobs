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
//! use actix::System;
//! use background_jobs::{create_server, Backoff, Job, MaxRetries, Processor, WorkerConfig};
//! use anyhow::Error;
//! use serde_derive::{Deserialize, Serialize};
//!
//! const DEFAULT_QUEUE: &'static str = "default";
//!
//! #[derive(Clone, Debug)]
//! pub struct MyState {
//!     pub app_name: String,
//! }
//!
//! #[derive(Clone, Debug, Deserialize, Serialize)]
//! pub struct MyJob {
//!     some_usize: usize,
//!     other_usize: usize,
//! }
//!
//! #[derive(Clone, Debug)]
//! pub struct MyProcessor;
//!
//! #[actix_rt::main]
//! async fn main() -> Result<(), Error> {
//!     // Set up our Storage
//!     // For this example, we use the default in-memory storage mechanism
//!     use background_jobs::memory_storage::Storage;
//!     let storage = Storage::new();
//!
//!     // Start the application server. This guards access to to the jobs store
//!     let queue_handle = create_server(storage);
//!
//!     // Configure and start our workers
//!     WorkerConfig::new(move || MyState::new("My App"))
//!         .register(MyProcessor)
//!         .set_processor_count(DEFAULT_QUEUE, 16)
//!         .start(queue_handle.clone());
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
//!     type Processor = MyProcessor;
//!     type State = MyState;
//!
//!     async fn run(self, state: MyState) -> Result<(), Error> {
//!         println!("{}: args, {:?}", state.app_name, self);
//!
//!         Ok(())
//!     }
//! }
//!
//! impl Processor for MyProcessor {
//!     // The kind of job this processor should execute
//!     type Job = MyJob;
//!
//!     // The name of the processor. It is super important that each processor has a unique name,
//!     // because otherwise one processor will overwrite another processor when they're being
//!     // registered.
//!     const NAME: &'static str = "MyProcessor";
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
//! }
//! ```

use actix::Arbiter;
use anyhow::Error;
use background_jobs_core::{Job, Processor, ProcessorMap, Stats, Storage};
use log::error;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

mod every;
mod server;
mod storage;
mod worker;

use self::{every::every, server::Server, worker::local_worker};

/// Create a new Server
///
/// In previous versions of this library, the server itself was run on it's own dedicated threads
/// and guarded access to jobs via messages. Since we now have futures-aware synchronization
/// primitives, the Server has become an object that gets shared between client threads.
///
/// This method should only be called once.
pub fn create_server<S>(storage: S) -> QueueHandle
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
/// workers, register `Processor` types with this struct. This worker registration allows for
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

    /// Register a `Processor` with the worker
    ///
    /// This enables the worker to handle jobs associated with this processor. If a processor is
    /// not registered, none of it's jobs will be run, even if another processor handling the same
    /// job queue is registered.
    pub fn register<P, J>(mut self, processor: P) -> Self
    where
        P: Processor<Job = J> + Send + Sync + 'static,
        J: Job<State = State>,
    {
        self.queues.insert(P::QUEUE.to_owned(), 4);
        self.processors.register_processor(processor);
        self
    }

    /// Set the number of workers to run for a given queue
    ///
    /// This does not spin up any additional threads. The `Arbiter` the workers are spawned onto
    /// will handle processing all workers, regardless of how many are configured.
    ///
    /// By default, 4 workers are spawned
    pub fn set_processor_count(mut self, queue: &str, count: u64) -> Self {
        self.queues.insert(queue.to_owned(), count);
        self
    }

    /// Start the workers in the current arbiter
    pub fn start(self, queue_handle: QueueHandle) {
        let processors = self.processors.clone();

        self.queues.into_iter().fold(0, |acc, (key, count)| {
            (0..count).for_each(|i| {
                local_worker(
                    acc + i + 1000,
                    key.clone(),
                    processors.cached(),
                    queue_handle.inner.clone(),
                );
            });

            acc + count
        });
    }

    /// Start the workers in the provided arbiter
    pub fn start_in_arbiter(self, arbiter: &Arbiter, queue_handle: QueueHandle) {
        let processors = self.processors.clone();
        self.queues.into_iter().fold(0, |acc, (key, count)| {
            (0..count).for_each(|i| {
                let processors = processors.clone();
                let queue_handle = queue_handle.clone();
                let key = key.clone();
                arbiter.exec_fn(move || {
                    local_worker(
                        acc + i + 1000,
                        key.clone(),
                        processors.cached(),
                        queue_handle.inner.clone(),
                    );
                });
            });

            acc + count
        });
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
    pub fn queue<J>(&self, job: J) -> Result<(), Error>
    where
        J: Job,
    {
        let job = J::Processor::new_job(job)?;
        let server = self.inner.clone();
        actix::spawn(async move {
            if let Err(e) = server.new_job(job).await {
                error!("Error creating job, {}", e);
            }
        });
        Ok(())
    }

    /// Queues a job for recurring execution
    ///
    /// This job will be added to it's queue on the server once every `Duration`. It will be
    /// processed whenever workers are free to do so.
    pub fn every<J>(&self, duration: Duration, job: J)
    where
        J: Job + Clone + 'static,
    {
        every(self.clone(), duration, job);
    }

    /// Return an overview of the processor's statistics
    pub async fn get_stats(&self) -> Result<Stats, Error> {
        self.inner.get_stats().await
    }
}
