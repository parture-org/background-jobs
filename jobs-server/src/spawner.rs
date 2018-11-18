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

use std::sync::Arc;

use background_jobs_core::Processor;
use failure::Error;
use futures::{future::IntoFuture, Future};
#[cfg(feature = "futures-zmq")]
use futures_zmq::{prelude::*, Push};
use log::{debug, trace};
#[cfg(feature = "tokio-zmq")]
use tokio_zmq::{prelude::*, Push};
use zmq::{Context, Message};

/// SpawnerConfig is the only part of this library required to actually exist in your application.
///
/// This type is used to queue new jobs into the `background-jobs` server.
///
/// ```rust,ignore
/// let spawner = SpawnerConfig::new("localhost", 5555);
///
/// tokio::spawn(
///     spawner
///         .queue::<MyProcessor>(job)
///         .map_err(|_| ()),
/// );
/// ```
pub struct SpawnerConfig {
    server: String,
    ctx: Arc<Context>,
}

impl SpawnerConfig {
    /// Create a `SpawnerConfig`
    ///
    ///  - `server_host` is the hostname or IP address of the host that the server is running on
    ///  - `base_port` is the same `base_port` from the server config. The spawner will only ever
    ///     need to communicate over `base_port`
    pub fn new(server_host: &str, base_port: usize) -> Self {
        let ctx = Arc::new(Context::new());

        Self::new_with_context(server_host, base_port, ctx)
    }

    /// The same as `SpawnerConfig::new()`, but with a provided ZeroMQ Context.
    ///
    /// This can be useful if you have other uses of ZeroMQ in your application, and want to share
    /// a context with your dependencies.
    ///
    /// If you're running the Server, Worker, and Spawner in the same application, you should share
    /// a ZeroMQ context between them.
    pub fn new_with_context(server_host: &str, base_port: usize, ctx: Arc<Context>) -> Self {
        SpawnerConfig {
            server: format!("tcp://{}:{}", server_host, base_port),
            ctx,
        }
    }

    /// Queue a job to be executed in the background
    pub fn queue<P>(&self, job: P::Job) -> impl Future<Item = (), Error = Error>
    where
        P: Processor,
    {
        let msg = P::new_job(job)
            .map_err(Error::from)
            .and_then(|job_info| serde_json::to_string(&job_info).map_err(Error::from))
            .and_then(|s| {
                Message::from_slice(s.as_ref())
                    .map(|m| m.into())
                    .map_err(Error::from)
            })
            .into_future();

        debug!("Sending message to {}", self.server);

        Push::builder(self.ctx.clone())
            .connect(&self.server)
            .build()
            .from_err()
            .join(msg)
            .and_then(move |(push, msg)| {
                trace!("Sending");
                push.send(msg).from_err().map(|_| trace!("sent"))
            })
    }

    /// `queue_sync` is the same as Queue, except that it blocks the current thread while it is
    /// sending the message to the jobs server.
    ///
    /// If you have a tokio-based application, you should use `queue` instead.
    pub fn queue_sync<P>(&self, job: P::Job) -> Result<(), Error>
    where
        P: Processor,
    {
        use zmq::PUSH;

        let job_info = P::new_job(job)?;

        let msg_string = serde_json::to_string(&job_info)?;

        let msg = Message::from_slice(msg_string.as_ref())?;

        let socket = self.ctx.socket(PUSH)?;
        socket.connect(&self.server)?;
        trace!("Sending");
        socket.send_msg(msg, 0)?;
        trace!("Sent");

        Ok(())
    }
}
