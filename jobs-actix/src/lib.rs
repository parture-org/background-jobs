use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use actix::{Actor, Addr, SyncArbiter};
use background_jobs_core::{Processor, ProcessorMap, Stats, Storage};
use failure::Error;
use futures::Future;

mod pinger;
mod server;
mod worker;
pub use self::{server::Server, worker::LocalWorker};

use self::{
    pinger::Pinger,
    server::{CheckDb, EitherJob, GetStats, RequestJob},
    worker::ProcessJob,
};

pub struct ServerConfig {
    server_id: usize,
    db_path: PathBuf,
}

impl ServerConfig {
    pub fn new(server_id: usize, db_path: PathBuf) -> Self {
        ServerConfig { server_id, db_path }
    }

    pub fn start<S>(self) -> QueueHandle<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        let ServerConfig { server_id, db_path } = self;

        let server = SyncArbiter::start(1, move || {
            Server::new(server_id, Storage::init(db_path.clone()).unwrap())
        });

        Pinger::new(server.clone()).start();

        QueueHandle { inner: server }
    }
}

pub struct WorkerConfig<S>
where
    S: Clone + Send + Sync + 'static,
{
    processors: ProcessorMap<S>,
    queues: BTreeMap<String, usize>,
}

impl<S> WorkerConfig<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new(state: S) -> Self {
        WorkerConfig {
            processors: ProcessorMap::new(state),
            queues: BTreeMap::new(),
        }
    }

    pub fn register<P>(&mut self, processor: P)
    where
        P: Processor<S> + Send + Sync + 'static,
    {
        self.queues.insert(P::QUEUE.to_owned(), 4);
        self.processors.register_processor(processor);
    }

    pub fn set_processor_count(&mut self, queue: &str, count: usize) {
        self.queues.insert(queue.to_owned(), count);
    }

    pub fn start(self, queue_handle: QueueHandle<S>) {
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
pub struct QueueHandle<S>
where
    S: Clone + Send + Sync + 'static,
{
    inner: Addr<Server<LocalWorker<S>>>,
}

impl<S> QueueHandle<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn queue<P>(&self, job: P::Job) -> Result<(), Error>
    where
        P: Processor<S>,
    {
        self.inner.do_send(EitherJob::New(P::new_job(job)?));
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
