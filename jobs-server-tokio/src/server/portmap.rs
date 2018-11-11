use std::{collections::BTreeMap, sync::Arc, time::Duration};

use failure::Error;
use futures::{future::lazy, stream::iter_ok, Future, Stream};
use tokio::timer::Delay;
use tokio_zmq::{prelude::*, Multipart, Push};
use zmq::Message;

use crate::server::Config;

pub(crate) struct PortMapConfig {
    pusher: Push,
    address: String,
    port_map: BTreeMap<String, usize>,
    config: Arc<Config>,
}

impl PortMapConfig {
    pub(crate) fn init(
        address: String,
        port_map: BTreeMap<String, usize>,
        config: Arc<Config>,
    ) -> impl Future<Item = (), Error = ()> {
        let cfg = ResetPortMapConfig {
            address,
            port_map,
            config,
        };

        cfg.build()
            .map_err(|e| error!("Error starting pusher, {}", e))
    }

    fn run(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let reset = self.reset();

        let PortMapConfig {
            pusher,
            address: _,
            port_map,
            config: _,
        } = self;

        let fut = iter_ok::<_, Error>(0..)
            .and_then(move |count| {
                trace!("Pushed {} portmaps", count);

                let s = serde_json::to_string(&port_map)?;
                let m = Message::from_slice(s.as_ref())?;

                Ok(Multipart::from(m))
            })
            .forward(pusher.sink())
            .map(move |_| info!("portmap pusher shutting down"))
            .map_err(|e| {
                error!("Error pushing portmap, {}", e);

                tokio::spawn(reset.rebuild());
            });

        Box::new(fut)
    }

    fn reset(&self) -> ResetPortMapConfig {
        ResetPortMapConfig {
            address: self.address.clone(),
            port_map: self.port_map.clone(),
            config: self.config.clone(),
        }
    }
}

struct ResetPortMapConfig {
    address: String,
    port_map: BTreeMap<String, usize>,
    config: Arc<Config>,
}

impl ResetPortMapConfig {
    fn rebuild(self) -> impl Future<Item = (), Error = ()> {
        Delay::new(tokio::clock::now() + Duration::from_secs(5))
            .from_err()
            .and_then(move |_| self.build())
            .map_err(|e| error!("Error restarting pusher, {}", e))
    }

    fn build(self) -> impl Future<Item = (), Error = Error> {
        lazy(|| {
            let pusher = Push::builder(self.config.context.clone())
                .bind(&self.address)
                .build()?;

            let config = PortMapConfig {
                pusher,
                address: self.address,
                port_map: self.port_map,
                config: self.config,
            };

            tokio::spawn(config.run());

            Ok(())
        })
    }
}
