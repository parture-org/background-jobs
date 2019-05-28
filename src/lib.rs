/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2019 Riley Trautman
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

//! # Background Jobs
//!
//! This crate provides tooling required to run some processes asynchronously from a usually
//! synchronous application. The standard example of this is Web Services, where certain things
//! need to be processed, but processing them while a user is waiting for their browser to respond
//! might not be the best experience.
//!
//! ### Usage
//! #### Add Background Jobs to your project
//! ```toml
//! [dependencies]
//! actix = "0.8"
//! background-jobs = "0.5.1"
//! failure = "0.1"
//! futures = "0.1"
//! serde = "1.0"
//! serde_drive = "1.0"
//! sled = "0.24"
//! ```
//!
//! #### To get started with Background Jobs, first you should define a job.
//! Jobs are a combination of the data required to perform an operation, and the logic of that
//! operation. They implment the `Job`, `serde::Serialize`, and `serde::DeserializeOwned`.
//!
//! ```rust,ignore
//! use background_jobs::Job;
//! use serde_derive::{Deserialize, Serialize};
//! use failure::Error;
//!
//! #[derive(Clone, Debug, Deserialize, Serialize)]
//! pub struct MyJob {
//!     some_usize: usize,
//!     other_usize: usize,
//! }
//!
//! impl MyJob {
//!     pub fn new(some_usize: usize, other_usize: usize) -> Self {
//!         MyJob {
//!             some_usize,
//!             other_usize,
//!         }
//!     }
//! }
//!
//! impl Job for MyJob {
//!     fn run(self, _: ()) -> Box<dyn Future<Item = (), Error = Error> + Send> {
//!         println!("args: {:?}", self);
//!
//!         Box::new(Ok(()).into_future())
//!     }
//! }
//! ```
//!
//! The run method for a job takes an additional argument, which is the state the job expects to
//! use. The state for all jobs defined in an application must be the same. By default, the state
//! is an empty tuple, but it's likely you'll want to pass in some Actix address, or something
//! else.
//!
//! Let's re-define the job to care about some application state.
//!
//! ```rust,ignore
//! # use failure::Error;
//! #[derive(Clone, Debug)]
//! pub struct MyState {
//!     pub app_name: String,
//! }
//!
//! impl MyState {
//!     pub fn new(app_name: &str) -> Self {
//!         MyState {
//!             app_name: app_name.to_owned(),
//!         }
//!     }
//! }
//!
//! impl Job<MyState> for MyJob {
//!     fn run(self, state: MyState) -> Box<dyn Future<Item = (), Error = Error> + Send> {
//!         info!("{}: args, {:?}", state.app_name, self);
//!
//!         Box::new(Ok(()).into_future())
//!     }
//! }
//! ```
//!
//! #### Next, define a Processor.
//! Processors are types that define default attributes for jobs, as well as containing some logic
//! used internally to perform the job. Processors must implement `Proccessor` and `Clone`.
//!
//! ```rust,ignore
//! use background_jobs::{Backoff, MaxRetries, Processor};
//!
//! const DEFAULT_QUEUE: &'static str = "default";
//!
//! #[derive(Clone, Debug)]
//! pub struct MyProcessor;
//!
//! impl Processor<MyState> for MyProcessor {
//!     // The kind of job this processor should execute
//!     type Job = MyJob;
//!
//!     // The name of the processor. It is super important that each processor has a unique name,
//!     // because otherwise one processor will overwrite another processor when they're being
//!     // registered.
//!     const NAME: &'static str = "MyProcessor";
//!
//!     // The queue that this processor belongs to
//!     //
//!     // Workers have the option to subscribe to specific queues, so this is important to
//!     // determine which worker will call the processor
//!     //
//!     // Jobs can optionally override the queue they're spawned on
//!     const QUEUE: &'static str = DEFAULT_QUEUE;
//!
//!     // The number of times background-jobs should try to retry a job before giving up
//!     //
//!     // Jobs can optionally override this value
//!     const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);
//!
//!     // The logic to determine how often to retry this job if it fails
//!     //
//!     // Jobs can optionally override this value
//!     const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);
//! }
//! ```
//!
//! #### Running jobs
//! By default, this crate ships with the `background-jobs-actix` feature enabled. This uses the
//! `background-jobs-actix` crate to spin up a Server and Workers, and provides a mechanism for
//! spawning new jobs.
//!
//! `background-jobs-actix` on it's own doesn't have a mechanism for storing worker state. This
//! can be implemented manually by implementing the `Storage` trait from `background-jobs-core`,
//! or the `background-jobs-sled-storage` crate can be used to provide a
//! [Sled](https://github.com/spacejam/sled)-backed jobs store.
//!
//! With that out of the way, back to the examples:
//!
//! ##### Main
//! ```rust,ignore
//! use actix::System;
//! use background_jobs::{ServerConfig, SledStorage, WorkerConfig};
//! use failure::Error;
//!
//! fn main() -> Result<(), Error> {
//!     // First set up the Actix System to ensure we have a runtime to spawn jobs on.
//!     let sys = System::new("my-actix-system");
//!
//!     // Set up our Storage
//!     let db = Db::start_default("my-sled-db")?;
//!     let storage = SledStorage::new(db)?;
//!
//!     // Start the application server. This guards access to to the jobs store
//!     let queue_handle = ServerConfig::new(storage).start();
//!
//!     // Configure and start our workers
//!     let mut worker_config = WorkerConfig::new(move || MyState::new("My App"));
//!     worker_config.register(MyProcessor);
//!     worker_config.set_processor_count(DEFAULT_QUEUE, 16);
//!     worker_config.start(queue_handle.clone());
//!
//!     // Queue our jobs
//!     queue_handle.queue::<MyProcessor>(MyJob::new(1, 2))?;
//!     queue_handle.queue::<MyProcessor>(MyJob::new(3, 4))?;
//!     queue_handle.queue::<MyProcessor>(MyJob::new(5, 6))?;
//!
//!     // Block on Actix
//!     sys.run()?;
//!     Ok(())
//! }
//! ```
//!
//! ##### Complete Example
//! For the complete example project, see
//! [the examples folder](https://git.asonix.dog/Aardwolf/background-jobs/src/branch/master/examples/actix-example)
//!
//! #### Bringing your own server/worker implementation
//! If you want to create your own jobs processor based on this idea, you can depend on the
//! `background-jobs-core` crate, which provides the Processor and Job traits, as well as some
//! other useful types for implementing a jobs processor and job store.

pub use background_jobs_core::{
    memory_storage, Backoff, Job, JobStat, MaxRetries, Processor, Stats,
};

#[cfg(feature = "background-jobs-actix")]
pub use background_jobs_actix::{Every, QueueHandle, ServerConfig, WorkerConfig};

#[cfg(feature = "background-jobs-sled-storage")]
pub mod sled_storage {
    pub use background_jobs_sled_storage::{Error, SledStorage as Storage};
}
