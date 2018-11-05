#[macro_use]
extern crate log;

mod storage;

use std::path::{Path, PathBuf};

use futures::{
    future::poll_fn,
    sync::mpsc::{channel, Receiver, SendError, Sender},
    Future, Sink, Stream,
};
use jobs_core::{JobInfo, Processor, Processors};
use tokio_threadpool::blocking;

use crate::storage::Storage;

pub struct ProcessorHandle {
    spawner: Sender<JobInfo>,
}

impl ProcessorHandle {
    pub fn queue(
        &self,
        job: JobInfo,
    ) -> impl Future<Item = Sender<JobInfo>, Error = SendError<JobInfo>> {
        self.spawner.clone().send(job)
    }
}

fn setup_kv(db_path: PathBuf) -> impl Future<Item = Storage, Error = ()> {
    tokio::fs::create_dir_all(db_path.clone())
        .map_err(|e| error!("Failed to create db directory: {}", e))
        .and_then(move |_| {
            poll_fn(move || {
                let path = db_path.clone();

                blocking(move || {
                    Storage::init(0, path).map_err(|e| error!("Error initializing db, {}", e))
                })
                .map_err(|e| error!("Error in blocking, {}", e))
            })
        })
        .and_then(|res| res)
}

pub struct JobRunner {
    processors: Processors,
    receiver: Receiver<JobInfo>,
    sender: Sender<JobInfo>,
    db_path: PathBuf,
}

impl JobRunner {
    pub fn new<P: AsRef<Path>>(db_path: P) -> Self {
        let (tx, rx) = channel::<JobInfo>(100);

        JobRunner {
            processors: Default::default(),
            receiver: rx,
            sender: tx,
            db_path: db_path.as_ref().to_owned(),
        }
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.processors.register_processor(processor);
    }

    fn runner(self) -> impl Future<Item = (), Error = ()> {
        let JobRunner {
            processors,
            receiver,
            sender,
            db_path,
        } = self;

        let _ = sender;
        let _ = db_path;

        // tokio::spawn(setup_kv(db_path));

        receiver
            .fold(processors, |mut processors, job| {
                processors.queue(job);

                Box::new(processors.turn())
            })
            .map(|_| ())
    }

    pub fn spawn(self) -> ProcessorHandle {
        let spawner = self.sender.clone();

        tokio::spawn(self.runner());

        ProcessorHandle { spawner }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
