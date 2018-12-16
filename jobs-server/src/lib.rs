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

use std::marker::PhantomData;

use background_jobs_core::{Backoff, Job, MaxRetries};
use failure::Error;
use futures::{future::poll_fn, Future};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_derive::{Deserialize, Serialize};
use tokio_threadpool::blocking;

mod server;
mod spawner;
mod worker;

pub use crate::{server::ServerConfig, spawner::SpawnerConfig, worker::WorkerConfig};

fn coerce<T, F>(res: Result<Result<T, Error>, F>) -> Result<T, Error>
where
    F: Into<Error>,
{
    match res {
        Ok(res) => res,
        Err(e) => Err(e.into()),
    }
}

/// The SyncJob trait defines parameters pertaining to a synchronous instance of background job
///
/// This trait should be implemented sparingly, but is provided so that synchronous tasks may be
/// executed. If you have the ability to implement the
/// [`Job`](https://docs.rs/background-jobs/0.4.0/background_jobs/trait.Job.html) trait directly,
/// you should.
///
/// ### Example
///
/// ```rust
/// use background_jobs_server::SyncJob;
/// use failure::Error;
/// use log::info;
/// use serde_derive::{Deserialize, Serialize};
///
/// #[derive(Clone, Deserialize, Serialize)]
/// struct MyJob {
///     count: i32,
/// }
///
/// impl SyncJob for MyJob {
///     fn run(self, _state: ()) -> Result<(), Error> {
///         info!("Processing {}", self.count);
///
///         // Perform some synchronous operation, like a DB action with r2d2 and diesel
///
///         Ok(())
///     }
/// }
///
/// fn main() {
///     let sync_job = MyJob { count: 0 };
///     let job = sync_job.to_job();
/// }
/// ```
pub trait SyncJob<S = ()>: Clone {
    /// Users of this library must define what it means to run a job.
    ///
    /// This should contain all the logic needed to complete a job. If that means queuing more
    /// jobs, sending an email, shelling out (don't shell out), or doing otherwise lengthy
    /// processes, that logic should all be called from inside this method.
    ///
    /// The state passed into this job is initialized at the start of the application. The state
    /// argument could be useful for containing a hook into something like r2d2, or the address of
    /// an actor in an actix-based system.
    fn run(self, state: S) -> Result<(), Error>;

    /// If this job should not use the default queue for its processor, this can be overridden in
    /// user-code.
    ///
    /// Jobs will only be processed by processors that are registered, and if a queue is supplied
    /// here that is not associated with a valid processor for this job, it will never be
    /// processed.
    fn queue(&self) -> Option<&str> {
        None
    }

    /// If this job should not use the default maximum retry count for its processor, this can be
    /// overridden in user-code.
    fn max_retries(&self) -> Option<MaxRetries> {
        None
    }

    /// If this job should not use the default backoff strategy for its processor, this can be
    /// overridden in user-code.
    fn backoff_strategy(&self) -> Option<Backoff> {
        None
    }

    /// Wrap this type in a SyncJobWrapper so it implements Job
    fn to_job(self) -> SyncJobWrapper<Self, S> {
        SyncJobWrapper {
            inner: self,
            phantom: PhantomData,
        }
    }
}

/// A wrapper around synchronous jobs
#[derive(Clone, Deserialize, Serialize)]
pub struct SyncJobWrapper<J, S = ()>
where
    J: SyncJob<S>,
{
    inner: J,
    phantom: PhantomData<S>,
}

impl<J, S> Job<S> for SyncJobWrapper<J, S>
where
    J: SyncJob<S> + Serialize + DeserializeOwned + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn queue(&self) -> Option<&str> {
        self.inner.queue()
    }

    fn max_retries(&self) -> Option<MaxRetries> {
        self.inner.max_retries()
    }

    fn backoff_strategy(&self) -> Option<Backoff> {
        self.inner.backoff_strategy()
    }

    fn run(self, state: S) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        let fut = poll_fn(move || {
            let job = self.inner.clone();
            let state = state.clone();

            blocking(move || job.run(state.clone()))
        })
        .then(coerce);

        Box::new(fut)
    }
}
