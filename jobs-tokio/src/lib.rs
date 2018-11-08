#[macro_use]
extern crate log;

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use futures::{
    future::{poll_fn, Either, IntoFuture},
    sync::mpsc::{channel, Receiver, SendError, Sender},
    Future, Sink, Stream,
};
use jobs_core::{JobInfo, Processor, Processors, Storage};
use tokio::timer::Interval;
use tokio_threadpool::blocking;

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

fn setup_kv(runner_id: usize, db_path: PathBuf) -> impl Future<Item = Storage, Error = ()> {
    tokio::fs::create_dir_all(db_path.clone())
        .map_err(|e| error!("Failed to create db directory: {}", e))
        .and_then(move |_| {
            poll_fn(move || {
                let path = db_path.clone();

                blocking(move || {
                    Storage::init(runner_id, path)
                        .map_err(|e| error!("Error initializing db, {}", e))
                })
                .map_err(|e| error!("Error in blocking, {}", e))
            })
        })
        .and_then(|res| res)
}

enum ProcessorMessage {
    Job(JobInfo),
    Time(Instant),
    Stop,
}

fn return_job(
    storage: Storage,
    processor_count: usize,
    processors: Processors,
    job: JobInfo,
) -> impl Future<Item = (Processors, usize), Error = ()> {
    poll_fn(move || {
        let storage = storage.clone();
        let job = job.clone();

        blocking(move || {
            storage
                .store_job(job)
                .map_err(|e| error!("Error finishing job, {}", e))
        })
        .map_err(|e| error!("Error blocking, {}", e))
    })
    .and_then(|res| res)
    .map(move |_| (processors, processor_count + 1))
}

fn try_process_job(
    storage: Storage,
    processor_count: usize,
    processors: Processors,
    tx: Sender<ProcessorMessage>,
) -> impl Future<Item = (Processors, usize), Error = ()> {
    if processor_count > 0 {
        let fut = poll_fn(move || {
            let storage = storage.clone();

            blocking(move || {
                storage
                    .dequeue_job(processor_count)
                    .map_err(|e| error!("Error dequeuing job, {}", e))
            })
            .map_err(|e| error!("Error blocking, {}", e))
        })
        .and_then(|res| res)
        .then(move |res| match res {
            Ok(jobs) => Ok(jobs.into_iter().fold(
                (processors, processor_count),
                move |(proc, count), job| {
                    let tx = tx.clone();
                    tokio::spawn(proc.process_job(job).and_then(move |job| {
                        tx.send(ProcessorMessage::Job(job))
                            .map(|_| ())
                            .map_err(|e| error!("Error returning job, {}", e))
                    }));

                    (proc, count - 1)
                },
            )),
            Err(_) => Ok((processors, processor_count)),
        });

        Either::A(fut)
    } else {
        Either::B(Ok((processors, processor_count)).into_future())
    }
}

fn process_jobs(
    storage: Storage,
    num_processors: usize,
    processors: Processors,
    tx: Sender<ProcessorMessage>,
    rx: Receiver<ProcessorMessage>,
) -> impl Future<Item = (), Error = ()> {
    Interval::new(tokio::clock::now(), Duration::from_millis(500))
        .map(ProcessorMessage::Time)
        .map_err(|e| error!("Error in timer, {}", e))
        .select(rx)
        .fold(
            (processors, num_processors),
            move |(processors, processor_count), msg| match msg {
                ProcessorMessage::Job(job) => {
                    let tx = tx.clone();

                    Either::A(
                        return_job(storage.clone(), processor_count, processors, job).map(
                            move |values| {
                                tokio::spawn(
                                    tx.send(ProcessorMessage::Time(tokio::clock::now()))
                                        .map(|_| ())
                                        .map_err(|_| ()),
                                );
                                values
                            },
                        ),
                    )
                }
                ProcessorMessage::Time(_) => Either::B(Either::A(try_process_job(
                    storage.clone(),
                    processor_count,
                    processors,
                    tx.clone(),
                ))),
                ProcessorMessage::Stop => {
                    info!("Got stop message");
                    Either::B(Either::B(Err(()).into_future()))
                }
            },
        )
        .map(|_| info!("Terminating processor"))
        .map_err(|_| info!("Terminating processor"))
}

pub struct JobRunner {
    processors: Processors,
    receiver: Receiver<JobInfo>,
    sender: Sender<JobInfo>,
    db_path: PathBuf,
    num_processors: usize,
    runner_id: usize,
}

impl JobRunner {
    pub fn new<P: AsRef<Path>>(runner_id: usize, num_processors: usize, db_path: P) -> Self {
        let (tx, rx) = channel::<JobInfo>(100);

        JobRunner {
            processors: Default::default(),
            receiver: rx,
            sender: tx,
            db_path: db_path.as_ref().to_owned(),
            num_processors,
            runner_id,
        }
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.processors.register_processor(processor);
    }

    pub fn spawn(self) -> ProcessorHandle {
        let spawner = self.sender.clone();

        tokio::spawn(self.runner());

        ProcessorHandle { spawner }
    }

    fn runner(self) -> impl Future<Item = (), Error = ()> {
        let JobRunner {
            processors,
            receiver,
            sender: _,
            db_path,
            num_processors,
            runner_id,
        } = self;

        let (tx, rx) = channel::<ProcessorMessage>(100);
        let tx2 = tx.clone();

        setup_kv(runner_id, db_path)
            .and_then(move |storage| {
                tokio::spawn(process_jobs(
                    storage.clone(),
                    num_processors,
                    processors,
                    tx,
                    rx,
                ));

                receiver.fold(storage, |storage, job| {
                    poll_fn(move || {
                        let job = job.clone();
                        let storage = storage.clone();
                        blocking(|| {
                            storage
                                .store_job(job)
                                .map_err(|e| error!("Error storing job, {}", e))
                                .map(|_| storage)
                        })
                        .map_err(|e| error!("Error blocking, {}", e))
                    })
                    .and_then(|res| res)
                })
            })
            .and_then(|_| {
                tx2.send(ProcessorMessage::Stop)
                    .map(|_| info!("Sent stop message"))
                    .map_err(|e| error!("Error shutting down processor, {}", e))
            })
    }
}
