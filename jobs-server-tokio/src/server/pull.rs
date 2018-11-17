use std::{sync::Arc, time::Duration};

use failure::{Error, Fail};
use futures::{future::poll_fn, Future, Stream};
#[cfg(feature = "futures-zmq")]
use futures_zmq::{prelude::*, Multipart, Pull};
use jobs_core::{JobInfo, Storage};
use log::{error, info, trace};
use tokio::timer::Delay;
use tokio_threadpool::blocking;
#[cfg(feature = "tokio-zmq")]
use tokio_zmq::{prelude::*, Multipart, Pull};

use crate::server::{coerce, Config};

pub(crate) struct PullConfig {
    puller: Pull,
    address: String,
    storage: Arc<Storage>,
    config: Arc<Config>,
}

impl PullConfig {
    pub(crate) fn init(
        address: String,
        storage: Arc<Storage>,
        config: Arc<Config>,
    ) -> impl Future<Item = (), Error = ()> {
        let cfg = ResetPullConfig {
            address,
            storage,
            config,
        };

        cfg.build()
            .map_err(|e| error!("Error starting puller, {}", e))
    }

    fn run(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let config = self.reset();

        let storage = self.storage.clone();

        let fut = self
            .puller
            .stream()
            .from_err()
            .map(|m| {
                trace!("Handling new message");
                m
            })
            .and_then(parse_job)
            .and_then(move |job| {
                trace!("Storing job, {:?}", job);
                store_job(job, storage.clone())
            })
            .for_each(|_| Ok(()))
            .map(|_| info!("Puller is shutting down"))
            .map_err(|e| {
                error!("Error storing job, {}", e);

                tokio::spawn(config.rebuild());
            });

        Box::new(fut)
    }

    fn reset(&self) -> ResetPullConfig {
        ResetPullConfig {
            address: self.address.clone(),
            storage: self.storage.clone(),
            config: self.config.clone(),
        }
    }
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Message was empty")]
pub struct EmptyMessage;

fn parse_job(mut multipart: Multipart) -> Result<JobInfo, Error> {
    let unparsed_msg = multipart.pop_front().ok_or(EmptyMessage)?;

    let parsed = serde_json::from_slice(&unparsed_msg)?;

    Ok(parsed)
}

fn store_job(job: JobInfo, storage: Arc<Storage>) -> impl Future<Item = (), Error = Error> {
    let storage = storage.clone();

    poll_fn(move || {
        let job = job.clone();
        let storage = storage.clone();

        blocking(move || storage.store_job(job).map_err(Error::from)).map_err(Error::from)
    })
    .then(coerce)
}

struct ResetPullConfig {
    address: String,
    storage: Arc<Storage>,
    config: Arc<Config>,
}

impl ResetPullConfig {
    fn rebuild(self) -> impl Future<Item = (), Error = ()> {
        Delay::new(tokio::clock::now() + Duration::from_secs(5))
            .from_err()
            .and_then(move |_| self.build())
            .map_err(|e| error!("Error restarting puller, {}", e))
    }

    fn build(self) -> impl Future<Item = (), Error = Error> {
        Pull::builder(self.config.context.clone())
            .bind(&self.address)
            .build()
            .map(|puller| {
                let config = PullConfig {
                    puller,
                    address: self.address,
                    storage: self.storage,
                    config: self.config,
                };

                tokio::spawn(config.run());
            })
            .from_err()
    }
}
