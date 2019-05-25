use std::{collections::BTreeMap, sync::Arc};

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
    server::{CheckDb, GetStats, NewJob, RequestJob, ReturningJob},
    worker::ProcessJob,
};

pub struct ServerConfig<S> {
    storage: S,
}

impl<S> ServerConfig<S>
where
    S: Storage + Sync + 'static,
{
    pub fn new(storage: S) -> Self {
        ServerConfig { storage }
    }

    pub fn start<State>(self) -> QueueHandle<S, State>
    where
        State: Clone + 'static,
    {
        let ServerConfig { storage } = self;

        let server = SyncArbiter::start(4, move || Server::new(storage.clone()));

        Pinger::new(server.clone()).start();

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

    pub fn register<P>(&mut self, processor: P)
    where
        P: Processor<State> + Send + 'static,
    {
        self.queues.insert(P::QUEUE.to_owned(), 4);
        self.processors.register_processor(processor);
    }

    pub fn set_processor_count(&mut self, queue: &str, count: u64) {
        self.queues.insert(queue.to_owned(), count);
    }

    pub fn start<S>(self, queue_handle: QueueHandle<S, State>)
    where
        S: Storage + 'static,
    {
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
pub struct QueueHandle<S, State>
where
    S: Storage + 'static,
    State: Clone + 'static,
{
    inner: Addr<Server<S, LocalWorker<S, State>>>,
}

impl<S, State> QueueHandle<S, State>
where
    S: Storage + 'static,
    State: Clone + 'static,
{
    pub fn queue<P>(&self, job: P::Job) -> Result<(), Error>
    where
        P: Processor<State>,
    {
        self.inner.do_send(NewJob(P::new_job(job)?));
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
