use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use failure::{Error, Fail};
use futures::{future::poll_fn, Future};
use jobs_core::Storage;
use log::{error, info};
use tokio_threadpool::blocking;
use zmq::Context;

use crate::coerce;

mod portmap;
mod pull;
mod push;
mod stalled;

use self::{portmap::PortMapConfig, pull::PullConfig, push::PushConfig, stalled::StalledConfig};

#[derive(Clone)]
pub(crate) struct Config {
    ip: String,
    base_port: usize,
    runner_id: usize,
    queues: BTreeSet<String>,
    db_path: PathBuf,
    context: Arc<Context>,
}

impl Config {
    fn create_server(&self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let runner_id = self.runner_id;
        let db_path = self.db_path.clone();
        let base_port = self.base_port;
        let queues = self.queues.clone();

        let config = Arc::new(self.clone());

        let fut = poll_fn(move || {
            let runner_id = runner_id;
            let db_path = db_path.clone();
            let base_port = base_port;
            let queues = queues.clone();

            blocking(move || {
                let storage = Arc::new(Storage::init(runner_id, db_path)?);
                storage.check_stalled_jobs()?;
                let port_map = storage.get_port_mapping(base_port, queues)?;

                Ok((storage, port_map))
            })
        })
        .from_err::<Error>()
        .then(coerce)
        .and_then(|(storage, port_map)| {
            for queue in config.queues.iter() {
                let port = port_map.get(queue).ok_or(MissingQueue(queue.to_owned()))?;

                let address = format!("tcp://{}:{}", config.ip, port);

                info!("Creating queue {} on address {}", queue, address);

                tokio::spawn(PushConfig::init(
                    address,
                    queue.to_owned(),
                    storage.clone(),
                    config.clone(),
                ));
            }

            StalledConfig::init(storage.clone());

            let portmap_address = format!("tcp://{}:{}", config.ip, config.base_port + 1);
            info!("Creating portmap on address {}", portmap_address);

            tokio::spawn(PortMapConfig::init(
                portmap_address,
                port_map,
                config.clone(),
            ));

            let pull_address = format!("tcp://{}:{}", config.ip, config.base_port);
            info!("Creating puller on address {}", pull_address);

            tokio::spawn(PullConfig::init(pull_address, storage, config));

            Ok(())
        })
        .map_err(|e| error!("Error starting server, {}", e));

        Box::new(fut)
    }
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Queue is missing from map, {}", _0)]
struct MissingQueue(String);

pub struct ServerConfig;

impl ServerConfig {
    pub fn init<P: AsRef<Path>>(
        ip: &str,
        base_port: usize,
        runner_id: usize,
        queues: BTreeSet<String>,
        db_path: P,
    ) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let context = Arc::new(Context::new());

        Self::init_with_context(ip, base_port, runner_id, queues, db_path, context)
    }

    pub fn init_with_context<P: AsRef<Path>>(
        ip: &str,
        base_port: usize,
        runner_id: usize,
        queues: BTreeSet<String>,
        db_path: P,
        context: Arc<Context>,
    ) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let config = Config {
            ip: ip.to_owned(),
            base_port,
            runner_id,
            queues,
            db_path: db_path.as_ref().to_owned(),
            context,
        };

        config.create_server()
    }
}
