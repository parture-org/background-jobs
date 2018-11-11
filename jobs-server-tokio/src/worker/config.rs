use std::{sync::Arc, time::Duration};

use failure::Error;
use futures::{
    future::{lazy, Either, IntoFuture},
    Future, Stream,
};
use jobs_core::{JobInfo, Processors};
use tokio::timer::Delay;
use tokio_zmq::{prelude::*, Multipart, Pull, Push};
use zmq::{Context, Message};

pub(crate) struct Worker {
    pull: Pull,
    push: Push,
    push_address: String,
    pull_address: String,
    queue: String,
    processors: Arc<Processors>,
    context: Arc<Context>,
}

impl Worker {
    pub(crate) fn init(
        push_address: String,
        pull_address: String,
        queue: String,
        processors: Arc<Processors>,
        context: Arc<Context>,
    ) -> impl Future<Item = (), Error = ()> {
        let cfg = ResetWorker {
            push_address,
            pull_address,
            queue: queue.clone(),
            processors,
            context,
        };

        cfg.build()
            .map_err(move |e| error!("Error starting worker for queue {}, {}", queue, e))
    }

    fn run(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let reset = self.reset();

        let Worker {
            push,
            pull,
            push_address: _,
            pull_address: _,
            queue,
            processors,
            context: _,
        } = self;

        let fut = pull
            .stream()
            .from_err::<Error>()
            .and_then(move |multipart| wrap_processing(multipart, &processors))
            .forward(push.sink(2))
            .map(move |_| info!("worker for queue {} is shutting down", queue))
            .map_err(|e| {
                error!("Error processing job, {}", e);

                tokio::spawn(reset.rebuild());
            });

        Box::new(fut)
    }

    fn reset(&self) -> ResetWorker {
        ResetWorker {
            push_address: self.push_address.clone(),
            pull_address: self.pull_address.clone(),
            queue: self.queue.clone(),
            processors: self.processors.clone(),
            context: self.context.clone(),
        }
    }
}

struct ResetWorker {
    push_address: String,
    pull_address: String,
    queue: String,
    processors: Arc<Processors>,
    context: Arc<Context>,
}

impl ResetWorker {
    fn rebuild(self) -> impl Future<Item = (), Error = ()> {
        let queue = self.queue.clone();

        Delay::new(tokio::clock::now() + Duration::from_secs(5))
            .from_err()
            .and_then(move |_| self.build())
            .map_err(move |e| error!("Error restarting worker for queue {}, {}", queue, e))
    }

    fn build(self) -> impl Future<Item = (), Error = Error> {
        lazy(|| {
            let push = Push::builder(self.context.clone())
                .connect(&self.push_address)
                .build()?;

            let pull = Pull::builder(self.context.clone())
                .connect(&self.pull_address)
                .build()?;

            let config = Worker {
                push,
                pull,
                push_address: self.push_address,
                pull_address: self.pull_address,
                queue: self.queue,
                processors: self.processors,
                context: self.context,
            };

            tokio::spawn(config.run());

            Ok(())
        })
    }
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
