use std::{collections::BTreeMap, sync::Arc, time::Duration};

use actix::{Actor, Addr, Arbiter, SyncArbiter};
use background_jobs_core::{Job, Processor, ProcessorMap, Stats, Storage};
use failure::Error;
use futures::Future;

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

pub struct ServerConfig<S> {
    storage: S,
    threads: usize,
}

impl<S> ServerConfig<S>
where
    S: Storage + Sync + 'static,
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
                    processors.clone(),
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
                        processors.clone(),
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
