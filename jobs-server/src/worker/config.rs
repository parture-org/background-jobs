/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2018 Riley Trautman
 *
 * Background Jobs is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Background Jobs is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Background Jobs.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::{sync::Arc, time::Duration};

use background_jobs_core::{JobInfo, ProcessorMap};
use failure::{Error, Fail};
use futures::{
    sync::mpsc::{channel, Sender},
    Future, Sink, Stream,
};
#[cfg(feature = "futures-zmq")]
use futures_zmq::{prelude::*, Multipart, Pull, Push};
use log::{error, info};
use tokio::timer::Delay;
#[cfg(feature = "tokio-zmq")]
use tokio_zmq::{prelude::*, Multipart, Pull, Push};
use zmq::{Context, Message};

pub(crate) struct Worker {
    pull: Pull,
    push: Push,
    push2: Push,
    push_address: String,
    pull_address: String,
    queue: String,
    processors: Arc<ProcessorMap>,
    context: Arc<Context>,
}

impl Worker {
    pub(crate) fn init(
        push_address: String,
        pull_address: String,
        queue: String,
        processors: Arc<ProcessorMap>,
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
            push2,
            pull,
            push_address: _,
            pull_address: _,
            queue,
            processors,
            context: _,
        } = self;

        let (tx, rx) = channel(5);

        tokio::spawn(
            rx.map_err(|_| RecvError)
                .from_err::<Error>()
                .and_then(serialize_request)
                .forward(push2.sink(1))
                .map(|_| ())
                .or_else(|_| Ok(())),
        );

        let fut = pull
            .stream()
            .from_err::<Error>()
            .and_then(parse_multipart)
            .and_then(move |job| report_running(job, tx.clone()))
            .and_then(move |job| process_job(job, &processors))
            .and_then(serialize_request)
            .forward(push.sink(1))
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
    processors: Arc<ProcessorMap>,
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
        Push::builder(self.context.clone())
            .connect(&self.push_address)
            .build()
            .join(
                Push::builder(self.context.clone())
                    .connect(&self.push_address)
                    .build(),
            )
            .join(
                Pull::builder(self.context.clone())
                    .connect(&self.pull_address)
                    .build(),
            )
            .map(|((push, push2), pull)| {
                let config = Worker {
                    push,
                    push2,
                    pull,
                    push_address: self.push_address,
                    pull_address: self.pull_address,
                    queue: self.queue,
                    processors: self.processors,
                    context: self.context,
                };

                tokio::spawn(config.run());
            })
            .from_err()
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

fn report_running(
    mut job: JobInfo,
    push: Sender<JobInfo>,
) -> impl Future<Item = JobInfo, Error = Error> {
    job.set_running();

    push.send(job.clone())
        .map(move |_| job)
        .map_err(|_| NotifyError.into())
}

fn process_job(
    job: JobInfo,
    processors: &ProcessorMap,
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

#[derive(Clone, Debug, Fail)]
#[fail(display = "Error notifying running has started")]
struct NotifyError;

#[derive(Clone, Debug, Fail)]
#[fail(display = "Error receiving from mpsc")]
struct RecvError;
