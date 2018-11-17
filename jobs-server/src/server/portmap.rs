use std::{collections::BTreeMap, sync::Arc, time::Duration};

use failure::Error;
use futures::{Future, Stream};
#[cfg(feature = "futures-zmq")]
use futures_zmq::{prelude::*, Multipart, Rep};
use log::{error, info};
use tokio::timer::Delay;
#[cfg(feature = "tokio-zmq")]
use tokio_zmq::{prelude::*, Multipart, Rep};
use zmq::Message;

use crate::server::Config;

pub(crate) struct PortMapConfig {
    rep: Rep,
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

        cfg.build().map_err(|e| error!("Error starting rep, {}", e))
    }

    fn run(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let reset = self.reset();

        let PortMapConfig {
            rep,
            address: _,
            port_map,
            config: _,
        } = self;

        let (sink, stream) = rep.sink_stream(1).split();

        let fut = stream
            .from_err::<Error>()
            .and_then(move |_| {
                let s = serde_json::to_string(&port_map)?;
                let m = Message::from_slice(s.as_ref())?;

                Ok(Multipart::from(m))
            })
            .forward(sink)
            .map(move |_| info!("portmap rep shutting down"))
            .map_err(|e| {
                error!("Error sending portmap, {}", e);

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
            .map_err(|e| error!("Error restarting rep, {}", e))
    }

    fn build(self) -> impl Future<Item = (), Error = Error> {
        Rep::builder(self.config.context.clone())
            .bind(&self.address)
            .build()
            .map(|rep| {
                let config = PortMapConfig {
                    rep,
                    address: self.address,
                    port_map: self.port_map,
                    config: self.config,
                };

                tokio::spawn(config.run());
            })
            .from_err()
    }
}
