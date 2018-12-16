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

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use background_jobs_core::Storage;
use failure::{Error, Fail};
use futures::{future::poll_fn, Future};
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
    server_id: usize,
    ip: String,
    base_port: usize,
    queues: BTreeSet<String>,
    db_path: PathBuf,
    context: Arc<Context>,
}

impl Config {
    fn create_server(&self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let db_path = self.db_path.clone();
        let base_port = self.base_port;
        let queues = self.queues.clone();
        let server_id = self.server_id;

        let config = Arc::new(self.clone());

        let fut = poll_fn(move || {
            let db_path = db_path.clone();
            let base_port = base_port;
            let queues = queues.clone();

            blocking(move || {
                let storage = Arc::new(Storage::init(db_path)?);
                storage.requeue_staged_jobs(server_id)?;
                storage.check_stalled_jobs(server_id)?;
                let port_map = storage.get_port_mapping(base_port, queues, server_id)?;

                Ok((storage, port_map))
            })
        })
        .from_err::<Error>()
        .then(coerce)
        .and_then(move |(storage, port_map)| {
            for queue in config.queues.iter() {
                let port = port_map.get(queue).ok_or(MissingQueue(queue.to_owned()))?;

                let address = format!("tcp://{}:{}", config.ip, port);

                info!("Creating queue {} on address {}", queue, address);

                tokio::spawn(PushConfig::init(
                    server_id,
                    address,
                    queue.to_owned(),
                    storage.clone(),
                    config.clone(),
                ));
            }

            StalledConfig::init(server_id, storage.clone());

            let portmap_address = format!("tcp://{}:{}", config.ip, config.base_port + 1);
            info!("Creating portmap on address {}", portmap_address);

            tokio::spawn(PortMapConfig::init(
                portmap_address,
                port_map,
                config.clone(),
            ));

            let pull_address = format!("tcp://{}:{}", config.ip, config.base_port);
            info!("Creating puller on address {}", pull_address);

            tokio::spawn(PullConfig::init(server_id, pull_address, storage, config));

            Ok(())
        })
        .map_err(|e| error!("Error starting server, {}", e));

        Box::new(fut)
    }
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Queue is missing from map, {}", _0)]
struct MissingQueue(String);

/// The entry point for creating a background-jobs server
///
/// `ServerConfig` is used to spin up the infrastructure to manage queueing and storing jobs, but
/// it does not provide functionality to execute jobs. For that, you must create a
/// [`Worker`](https://docs.rs/background-jobs-server/0.4.0/background_jobs_server/struct.WorkerConfig.html)
/// that will connect to the running server.
///
/// This type doesn't have any associated data, but is used as a proxy for starting the
/// background-jobs runtime.
///
/// ```rust
/// use std::collections::BTreeSet;
/// use background_jobs_server::ServerConfig;
/// use failure::Error;
///
/// fn main() -> Result<(), Error> {
///     let mut queue_set = BTreeSet::new();
///     queue_set.insert("default".to_owned());
///
///     let start_server = ServerConfig::init(
///         1,
///         "127.0.0.1",
///         5555,
///         queue_set,
///         "example-db",
///     );
///
///     # let _ = start_server;
///     // Comment out the start so we don't run the full server in doctests
///     // tokio::run(start_server)
///
///     Ok(())
/// }
/// ```
pub struct ServerConfig;

impl ServerConfig {
    /// Create a new background-jobs Server that binds to the provided `ip` with ports starting at
    /// `base_port`.
    ///
    /// The smallest background-jobs server will bind to 3 ports. Each port serves a different
    /// purpose:
    ///  - `base_port` is the port that jobs are sent to the server on
    ///  - `base_port` + 1 is the port that the server uses to advertise which queues are available
    ///  - `base_port` + n is bound for an individual queue of jobs that the server pushes to
    ///    workers.
    ///
    /// This method returns a future that, when run, spawns all of the server's required futures
    /// onto tokio. Therefore, this can only be used from tokio.
    pub fn init<P: AsRef<Path>>(
        server_id: usize,
        ip: &str,
        base_port: usize,
        queues: BTreeSet<String>,
        db_path: P,
    ) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let context = Arc::new(Context::new());

        Self::init_with_context(server_id, ip, base_port, queues, db_path, context)
    }

    /// The same as `ServerConfig::init()`, but with a provided ZeroMQ Context.
    ///
    /// This can be useful if you have other uses of ZeroMQ in your application, and want to share
    /// a context with your dependencies.
    ///
    /// If you're running the Server, Worker, and Spawner in the same application, you should share
    /// a ZeroMQ context between them.
    pub fn init_with_context<P: AsRef<Path>>(
        server_id: usize,
        ip: &str,
        base_port: usize,
        queues: BTreeSet<String>,
        db_path: P,
        context: Arc<Context>,
    ) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let config = Config {
            server_id,
            ip: ip.to_owned(),
            base_port,
            queues,
            db_path: db_path.as_ref().to_owned(),
            context,
        };

        config.create_server()
    }
}
