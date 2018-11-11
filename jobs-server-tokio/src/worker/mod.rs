use std::{collections::BTreeMap, sync::Arc};

use futures::Future;
use jobs_core::{Processor, Processors};
use zmq::Context;

mod config;
mod portmap;

use self::{config::Worker, portmap::PortMap};

pub struct WorkerConfig {
    processors: Processors,
    queues: BTreeMap<String, usize>,
    server_host: String,
    base_port: usize,
    context: Arc<Context>,
}

impl WorkerConfig {
    pub fn new(server_host: String, base_port: usize, queues: BTreeMap<String, usize>) -> Self {
        let context = Arc::new(Context::new());

        Self::new_with_context(server_host, base_port, queues, context)
    }

    pub fn new_with_context(
        server_host: String,
        base_port: usize,
        queues: BTreeMap<String, usize>,
        context: Arc<Context>,
    ) -> Self {
        WorkerConfig {
            processors: Processors::new(),
            server_host,
            base_port,
            queues,
            context,
        }
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.processors.register_processor(processor);
    }

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
