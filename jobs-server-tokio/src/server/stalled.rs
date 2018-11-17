use std::{sync::Arc, time::Duration};

use failure::Error;
use futures::{future::poll_fn, Future, Stream};
use jobs_core::Storage;
use log::{error, info};
use tokio::timer::{Delay, Interval};
use tokio_threadpool::blocking;

use crate::server::coerce;

#[derive(Clone)]
pub(crate) struct StalledConfig {
    storage: Arc<Storage>,
}

impl StalledConfig {
    pub(crate) fn init(storage: Arc<Storage>) {
        let cfg = StalledConfig { storage };

        tokio::spawn(cfg.run());
    }

    fn run(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let reset = self.clone();

        let StalledConfig { storage } = self;

        let fut = Interval::new(tokio::clock::now(), Duration::from_secs(60 * 30))
            .from_err::<Error>()
            .and_then(move |_| {
                let storage = storage.clone();
                poll_fn(move || {
                    let storage = storage.clone();
                    blocking(move || storage.check_stalled_jobs().map_err(Error::from))
                })
                .from_err()
            })
            .then(coerce)
            .for_each(|_| Ok(()))
            .map(|_| info!("Stalled Job Checker is shutting down"))
            .map_err(|e| {
                error!("Error checking stalled jobs, {}", e);

                tokio::spawn(reset.rebuild());
            });

        Box::new(fut)
    }

    fn rebuild(self) -> impl Future<Item = (), Error = ()> {
        Delay::new(tokio::clock::now() + Duration::from_secs(5))
            .from_err::<Error>()
            .map(move |_| tokio::spawn(self.run()))
            .map(|_| ())
            .map_err(|e| error!("Error restarting stalled job checker, {}", e))
    }
}
