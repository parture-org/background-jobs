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

use background_jobs_core::{JobInfo, Storage};
use failure::Error;
use futures::{future::poll_fn, stream::iter_ok, Future, Stream};
#[cfg(feature = "futures-zmq")]
use futures_zmq::{prelude::*, Multipart, Push};
use log::{error, info};
use tokio::timer::{Delay, Interval};
use tokio_threadpool::blocking;
#[cfg(feature = "tokio-zmq")]
use tokio_zmq::{prelude::*, Multipart, Push};
use zmq::Message;

use crate::server::{coerce, Config};

pub(crate) struct PushConfig {
    pusher: Push,
    address: String,
    queue: String,
    storage: Arc<Storage>,
    config: Arc<Config>,
}

impl PushConfig {
    pub(crate) fn init(
        address: String,
        queue: String,
        storage: Arc<Storage>,
        config: Arc<Config>,
    ) -> impl Future<Item = (), Error = ()> {
        let cfg = ResetPushConfig {
            address,
            queue,
            storage,
            config,
        };

        cfg.build()
            .map_err(|e| error!("Error starting pusher, {}", e))
    }

    fn run(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let reset = self.reset();

        let PushConfig {
            address: _,
            pusher,
            queue,
            storage,
            config: _,
        } = self;

        let queue_2_electric_boogaloo = queue.clone();

        let fut = Interval::new(tokio::clock::now(), Duration::from_millis(250))
            .from_err()
            .and_then(move |_| dequeue_jobs(storage.clone(), queue.clone()))
            .flatten()
            .forward(pusher.sink(25))
            .map(move |_| {
                info!(
                    "Pusher for queue {} is shutting down",
                    queue_2_electric_boogaloo
                )
            })
            .map_err(|e| {
                error!("Error dequeuing job, {}", e);

                tokio::spawn(reset.rebuild());
            });

        Box::new(fut)
    }

    fn reset(&self) -> ResetPushConfig {
        ResetPushConfig {
            address: self.address.clone(),
            queue: self.queue.clone(),
            storage: self.storage.clone(),
            config: self.config.clone(),
        }
    }
}

fn dequeue_jobs(
    storage: Arc<Storage>,
    queue: String,
) -> impl Future<Item = impl Stream<Item = Multipart, Error = Error>, Error = Error> {
    poll_fn(move || {
        let storage = storage.clone();
        let queue = queue.clone();
        blocking(move || wrap_fetch_queue(storage, &queue))
    })
    .then(coerce)
    .map(|jobs| iter_ok(jobs))
    .or_else(|e| {
        error!("Error fetching jobs, {}", e);
        Ok(iter_ok(vec![]))
    })
}

fn wrap_fetch_queue(storage: Arc<Storage>, queue: &str) -> Result<Vec<Multipart>, Error> {
    let response = fetch_queue(storage, queue)?;

    let jobs = response
        .into_iter()
        .map(|job| {
            serde_json::to_string(&job)
                .map_err(Error::from)
                .and_then(|json| Message::from_slice(json.as_ref()).map_err(Error::from))
                .map(Multipart::from)
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(jobs)
}

fn fetch_queue(storage: Arc<Storage>, queue: &str) -> Result<Vec<JobInfo>, Error> {
    storage.stage_jobs(100, queue).map_err(Error::from)
}

struct ResetPushConfig {
    address: String,
    queue: String,
    storage: Arc<Storage>,
    config: Arc<Config>,
}

impl ResetPushConfig {
    fn rebuild(self) -> impl Future<Item = (), Error = ()> {
        Delay::new(tokio::clock::now() + Duration::from_secs(5))
            .from_err()
            .and_then(move |_| self.build())
            .map_err(|e| error!("Error restarting pusher, {}", e))
    }

    fn build(self) -> impl Future<Item = (), Error = Error> {
        info!("Building and spawning new server");
        Push::builder(self.config.context.clone())
            .bind(&self.address)
            .build()
            .map(|pusher| {
                let config = PushConfig {
                    pusher,
                    address: self.address,
                    queue: self.queue,
                    storage: self.storage,
                    config: self.config,
                };

                tokio::spawn(config.run());
            })
            .from_err()
    }
}