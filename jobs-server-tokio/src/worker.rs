use std::sync::Arc;

use failure::Error;
use futures::{
    future::{lazy, Either, IntoFuture},
    Future, Stream,
};
use jobs_core::{JobInfo, Processor, Processors};
use tokio_zmq::{prelude::*, Multipart, Pull, Push};
use zmq::{Context, Message};

struct Worker {
    processors: Processors,
    pull: Pull,
    push: Push,
}

impl Worker {
    pub fn init(
        server_host: &str,
        job_port: usize,
        queue_port: usize,
        ctx: Arc<Context>,
    ) -> Result<Self, Error> {
        let pull = Pull::builder(ctx.clone())
            .connect(&format!("tcp://{}:{}", server_host, job_port))
            .build()?;

        let push = Push::builder(ctx.clone())
            .connect(&format!("tcp://{}:{}", server_host, queue_port))
            .build()?;

        let processors = Processors::new();

        let worker = Worker {
            processors,
            push,
            pull,
        };

        Ok(worker)
    }

    fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.processors.register_processor(processor);
    }
}

pub struct WorkerConfig {
    workers: Vec<Worker>,
}

impl WorkerConfig {
    pub fn init(
        num_processors: usize,
        server_host: &str,
        job_port: usize,
        queue_port: usize,
    ) -> Result<Self, Error> {
        let ctx = Arc::new(Context::new());

        let mut workers = Vec::new();

        for _ in 0..num_processors {
            let worker = Worker::init(server_host, job_port, queue_port, ctx.clone())?;

            workers.push(worker);
        }

        let cfg = WorkerConfig { workers };

        Ok(cfg)
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        for worker in self.workers.iter_mut() {
            worker.register_processor(processor.clone());
        }
    }

    pub fn run(self) -> impl Future<Item = (), Error = ()> {
        let WorkerConfig { workers } = self;

        lazy(|| {
            for worker in workers.into_iter() {
                tokio::spawn(worker_future(worker));
            }

            Ok(())
        })
    }
}

fn worker_future(worker: Worker) -> impl Future<Item = (), Error = ()> {
    let Worker {
        push,
        pull,
        processors,
    } = worker;

    pull.stream()
        .from_err()
        .and_then(move |multipart| wrap_processing(multipart, &processors))
        .map(Some)
        .or_else(|e| {
            error!("Error processing job, {}", e);
            Ok(None)
        })
        .filter_map(|item| item)
        .forward(push.sink())
        .map_err(|e: Error| error!("Error pushing job, {}", e))
        .map(|_| ())
}

fn serialize_request(job: JobInfo) -> Result<Multipart, Error> {
    let request = serde_json::to_string(&job)?;
    let msg = Message::from_slice(request.as_ref())?;

    Ok(msg.into())
}

fn parse_multipart(mut multipart: Multipart) -> Result<JobInfo, Error> {
    let message = multipart.pop_front().ok_or(ParseError)?;

    let parsed = serde_json::from_slice(&message)?;

    Ok(parsed)
}

fn wrap_processing(
    multipart: Multipart,
    processors: &Processors,
) -> impl Future<Item = Multipart, Error = Error> {
    let msg = match parse_multipart(multipart) {
        Ok(msg) => msg,
        Err(e) => return Either::A(Err(e).into_future()),
    };

    let fut = process_job(msg, processors).and_then(serialize_request);

    Either::B(fut)
}

fn process_job(
    job: JobInfo,
    processors: &Processors,
) -> impl Future<Item = JobInfo, Error = Error> {
    processors
        .process_job(job.clone())
        .map_err(|_| ProcessError)
        .from_err()
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Error parsing job")]
struct ParseError;

#[derive(Clone, Debug, Fail)]
#[fail(display = "Error processing job")]
struct ProcessError;
