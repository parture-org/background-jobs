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

use std::{collections::BTreeMap, sync::Arc};

use background_jobs_core::{Processor, ProcessorMap};
use failure::Fail;
use futures::Future;
use log::{error, info};
use zmq::Context;

mod config;
mod portmap;

use self::{config::Worker, portmap::PortMap};

/// The entry point for creating a background-jobs worker.
///
/// A worker handles the processing of jobs, but not the queueing or storing of jobs. It connects
/// to a server (crated with
/// [`ServerConfig`](https://docs.rs/background-jobs-server/0.1.0/background_jobs_server/struct.ServerConfig))
/// and receives work from there.
///
/// ```rust
/// use std::collections::BTreeMap;
/// use background_jobs_server::WorkerConfig;
/// use failure::Error;
///
/// fn main() -> Result<(), Error> {
///     let mut queue_map = BTreeMap::new();
///     queue_map.insert("default".to_owned(), 10);
///
///     let mut worker = WorkerConfig::new("localhost".to_owned(), 5555, queue_map);
///
///     // Register a processor
///     // worker.register_processor(MyProcessor);
///
///     // Run the workers
///     // tokio::run(worker.run());
///
///     Ok(())
/// }
/// ```
pub struct WorkerConfig {
    processors: ProcessorMap,
    queues: BTreeMap<String, usize>,
    server_host: String,
    base_port: usize,
    context: Arc<Context>,
}

impl WorkerConfig {
    /// Create a new worker
    ///
    /// This method takes three arguments
    ///  - `server_host` is the hostname, or IP address, of the background-jobs server.
    ///  - `base_port` is the same value from the `ServerConfig` initialization. It dictates the
    ///    port the worker uses to return jobs to the server. The worker is guaranteed to connect
    ///    to at least 2 other ports on the server when functioning properly, `base_port` + 1, and
    ///    `base_port` + n.
    ///  - queues is a mapping between the name of a queue, and the number of workers that should
    ///    be started to process jobs in that queue.
    pub fn new(server_host: String, base_port: usize, queues: BTreeMap<String, usize>) -> Self {
        let context = Arc::new(Context::new());

        Self::new_with_context(server_host, base_port, queues, context)
    }

    /// The same as `WorkerConfig::new()`, but with a provided ZeroMQ Context.
    ///
    /// This can be useful if you have other uses of ZeroMQ in your application, and want to share
    /// a context with your dependencies.
    ///
    /// If you're running the Server, Worker, and Spawner in the same application, you should share
    /// a ZeroMQ context between them.
    pub fn new_with_context(
        server_host: String,
        base_port: usize,
        queues: BTreeMap<String, usize>,
        context: Arc<Context>,
    ) -> Self {
        WorkerConfig {
            processors: ProcessorMap::new(),
            server_host,
            base_port,
            queues,
            context,
        }
    }

    /// Register a processor with this worker
    ///
    /// For more information, see
    /// [`Processor`](https://docs.rs/background-jobs/0.1.0/background_jobs/struct.Processor).
    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.processors.register_processor(processor);
    }

    /// Start the workers
    ///
    /// This method returns a future that, when run, spawns all of the worker's required futures
    /// onto tokio. Therefore, this can only be used from tokio.
    pub fn run(self) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        let WorkerConfig {
            processors,
            server_host,
            base_port,
            queues,
            context,
        } = self;

        info!("Starting workers");

        let processors = Arc::new(processors);

        let push_address = format!("tcp://{}:{}", server_host, base_port);
        let portmap_address = format!("tcp://{}:{}", server_host, base_port + 1);

        info!("push address, {}", push_address);
        info!("portmap address, {}", portmap_address);

        let fut = PortMap::init(portmap_address, context.clone())
            .and_then(move |port_map| {
                info!("Got port_map, {:?}", port_map);

                for (queue, num) in queues.iter() {
                    let port = port_map.get(queue).ok_or(MissingQueue(queue.to_owned()))?;

                    let pull_address = format!("tcp://{}:{}", server_host, port);

                    for _ in 0..*num {
                        tokio::spawn(Worker::init(
                            push_address.clone(),
                            pull_address.clone(),
                            queue.to_owned(),
                            processors.clone(),
                            context.clone(),
                        ));
                    }
                }

                Ok(())
            })
            .map_err(|e| error!("Error starting worker, {}", e));

        Box::new(fut)
    }
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Could not find queue, {}", _0)]
struct MissingQueue(String);
