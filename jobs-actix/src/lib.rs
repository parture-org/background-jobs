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
//! use background_jobs::{Backoff, Job, MaxRetries, Processor, ServerConfig, WorkerConfig};
//! use failure::Error;
//! use futures::{future::ok, Future};
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
//! fn main() -> Result<(), Error> {
//!     // First set up the Actix System to ensure we have a runtime to spawn jobs on.
//!     let sys = System::new("my-actix-system");
//!
//!     // Set up our Storage
//!     // For this example, we use the default in-memory storage mechanism
//!     use background_jobs::memory_storage::Storage;
//!     let storage = Storage::new();
//!
//!     // Start the application server. This guards access to to the jobs store
//!     let queue_handle = ServerConfig::new(storage).thread_count(8).start();
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
//!     // Block on Actix
//!     sys.run()?;
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
//!     type Processor = MyProcessor;
//!     type State = MyState;
//!     type Future = Result<(), Error>;
//!
//!     fn run(self, state: MyState) -> Self::Future {
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
//!     // Jobs can optionally override this value
//!     const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);
//!
//!     // The logic to determine how often to retry this job if it fails
//!     //
//!     // Jobs can optionally override this value
//!     const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);
//! }
//! ```

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use actix::{Actor, Addr, Arbiter, SyncArbiter};
use background_jobs_core::{Job, Processor, ProcessorMap, Stats, Storage};
use failure::{Error, Fail};
use futures::{future::IntoFuture, Future};

mod every;
mod pinger;
mod server;
mod storage;
mod worker;

pub use self::{every::Every, server::Server, worker::LocalWorker};

use self::{
    pinger::Pinger,
    server::{CheckDb, GetStats, NewJob, RequestJob, ReturningJob},
    storage::{ActixStorage, StorageWrapper},
    worker::Worker,
};

/// The configuration for a jobs server
///
/// The server guards access to the storage backend, and keeps job information properly
/// up-to-date when workers request jobs to process
pub struct ServerConfig<S> {
    storage: S,
    threads: usize,
}

impl<S> ServerConfig<S>
where
    S: Storage + Sync + 'static,
    S::Error: Fail,
{
    /// Create a new ServerConfig
    pub fn new(storage: S) -> Self {
        ServerConfig {
            storage,
            threads: num_cpus::get(),
        }
    }

    /// Set the number of threads to use for the server.
    ///
    /// This is not related to the number of workers or the number of worker threads. This is
    /// purely how many threads will be used to manage access to the job store.
    ///
    /// By default, this is the number of processor cores available to the application. On systems
    /// with logical cores (such as Intel hyperthreads), this will be the total number of logical
    /// cores.
    ///
    /// In certain cases, it may be beneficial to limit the server process count to 1.
    ///
    /// When using actix-web, any configuration performed inside `HttpServer::new` closure will
    /// happen on each thread started by the web server. In order to reduce the number of running
    /// threads, one job server can be started per web server thread.
    ///
    /// Another case to use a single server is if your job store has not locking guarantee, and you
    /// want to enforce that no job can be requested more than once. The default storage
    /// implementation does provide this guarantee, but other implementations may not.
    pub fn thread_count(mut self, threads: usize) -> Self {
        self.threads = threads;
        self
    }

    /// Spin up the server processes
    pub fn start(self) -> QueueHandle {
        let ServerConfig { storage, threads } = self;

        let server = SyncArbiter::start(threads, move || {
            Server::new(StorageWrapper(storage.clone()))
        });

        Pinger::new(server.clone(), threads).start();

        QueueHandle { inner: server }
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
        <J::Future as IntoFuture>::Future: Send,
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
                LocalWorker::new(
                    acc + i + 1000,
                    key.clone(),
                    processors.cached(),
                    queue_handle.inner.clone(),
                )
                .start();
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
                LocalWorker::start_in_arbiter(arbiter, move |_| {
                    LocalWorker::new(
                        acc + i + 1000,
                        key.clone(),
                        processors.cached(),
                        queue_handle.inner.clone(),
                    )
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
    inner: Addr<Server>,
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
        self.inner.do_send(NewJob(J::Processor::new_job(job)?));
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
        Every::new(self.clone(), duration, job).start();
    }

    /// Return an overview of the processor's statistics
    pub fn get_stats(&self) -> Box<dyn Future<Item = Stats, Error = Error> + Send> {
        Box::new(self.inner.send(GetStats).then(coerce))
    }
}

fn coerce<I, E, F>(res: Result<Result<I, E>, F>) -> Result<I, E>
where
    E: From<F>,
{
    match res {
        Ok(inner) => inner,
        Err(e) => Err(e.into()),
    }
}
