use std::{collections::BTreeMap, sync::Arc};

use actix::{Actor, Addr, SyncArbiter};
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
    pub fn new(state_fn: impl Fn() -> State + Send + Sync + 'static) -> Self {
        WorkerConfig {
            processors: ProcessorMap::new(Box::new(state_fn)),
            queues: BTreeMap::new(),
        }
    }

    pub fn register(
        mut self,
        processor: impl Processor<Job = impl Job<State = State> + Send + 'static> + Send + 'static,
    ) -> Self {
        self.queues.insert(processor.queue().to_owned(), 4);
        self.processors.register_processor(processor);
        self
    }

    pub fn set_processor_count(mut self, queue: &str, count: u64) -> Self {
        self.queues.insert(queue.to_owned(), count);
        self
    }

    pub fn start(self, queue_handle: QueueHandle) {
        let processors = Arc::new(self.processors);

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
}

#[derive(Clone)]
pub struct QueueHandle {
    inner: Addr<Server>,
}

impl QueueHandle {
    pub fn queue<J>(&self, job: J) -> Result<(), Error>
    where
        J: Job,
    {
        self.inner.do_send(NewJob(J::Processor::new_job(job)?));
        Ok(())
    }

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
