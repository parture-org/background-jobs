use std::sync::Arc;

use failure::Error;
use futures::{future::IntoFuture, Future};
use jobs_core::JobInfo;
use tokio_zmq::{prelude::*, Push};
use zmq::{Context, Message};

pub struct SpawnerConfig {
    server: String,
    ctx: Arc<Context>,
}

impl SpawnerConfig {
    pub fn new(server_host: &str, queue_port: usize) -> Self {
        let ctx = Arc::new(Context::new());

        SpawnerConfig {
            server: format!("tcp://{}:{}", server_host, queue_port),
            ctx,
        }
    }

    pub fn queue(&self, job: JobInfo) -> impl Future<Item = (), Error = Error> {
        let msg = serde_json::to_string(&job)
            .map_err(Error::from)
            .and_then(|s| {
                Message::from_slice(s.as_ref())
                    .map(|m| m.into())
                    .map_err(Error::from)
            })
            .into_future();

        Push::builder(self.ctx.clone())
            .connect(&self.server)
            .build()
            .into_future()
            .from_err()
            .join(msg)
            .and_then(move |(req, msg)| req.send(msg).from_err().map(|_| ()))
    }
}
