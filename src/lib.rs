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
//! background-jobs = "0.6.0"
//! anyhow = "1.0"
//! futures = "0.1"
//! serde = { version = "1.0", features = ["derive"] }
//! ```
//!
//! #### To get started with Background Jobs, first you should define a job.
//! Jobs are a combination of the data required to perform an operation, and the logic of that
//! operation. They implment the `Job`, `serde::Serialize`, and `serde::DeserializeOwned`.
//!
//! ```rust,ignore
//! use anyhow::Error;
//! use background_jobs::Job;
//! use futures::future::{ok, Ready};
//!
//! #[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
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
//!     type State = ();
//!     type Future = Ready<Result<(), Error>>;
//!
//!     const NAME: &'static str = "MyJob";
//!
//!     fn run(self, _: Self::State) -> Self::Future {
//!         println!("args: {:?}", self);
//!
//!         ok(())
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
//! use anyhow::Error;
//! use background_jobs::Job;
//! use futures::future::{ok, Ready};
//!
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
//! impl Job for MyJob {
//!     type State = MyState;
//!     type Future = Ready<Result<(), Error>>;
//!
//!     const NAME: &'static str = "MyJob";
//!
//!     fn run(self, state: Self::State) -> Self::Future {
//!         info!("{}: args, {:?}", state.app_name, self);
//!
//!         ok(())
//!     }
//! }
//! ```
//!
//! #### Running jobs
//! By default, this crate ships with the `background-jobs-actix` feature enabled. This uses the
//! `background-jobs-actix` crate to spin up a Server and Workers, and provides a mechanism for
//! spawning new jobs.
//!
//! `background-jobs-actix` on it's own doesn't have a mechanism for storing worker state. This
//! can be implemented manually by implementing the `Storage` trait from `background-jobs-core`, or the provided in-memory store can be used.
//!
//! With that out of the way, back to the examples:
//!
//! ##### Main
//! ```rust,ignore
//! use anyhow::Error;
//! use background_jobs::{ServerConfig, memory_storage::Storage, WorkerConfig};
//!
//! #[actix_rt::main]
//! async fn main() -> Result<(), Error> {
//!     // Set up our Storage
//!     let storage = Storage::new();
//!
//!     // Start the application server. This guards access to to the jobs store
//!     let queue_handle = ServerConfig::new(storage).start();
//!
//!     // Configure and start our workers
//!     WorkerConfig::new(move || MyState::new("My App"))
//!         .register::<MyJob>()
//!         .set_processor_count(DEFAULT_QUEUE, 16)
//!         .start(queue_handle.clone());
//!
//!     // Queue our jobs
//!     queue_handle.queue(MyJob::new(1, 2))?;
//!     queue_handle.queue(MyJob::new(3, 4))?;
//!     queue_handle.queue(MyJob::new(5, 6))?;
//!
//!     // Block on Actix
//!     actix_rt::signal::ctrl_c().await?;
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
//! `background-jobs-core` crate, which provides the Job trait, as well as some
//! other useful types for implementing a jobs processor and job store.

pub use background_jobs_core::{memory_storage, Backoff, Job, JobStat, MaxRetries, Stats};

pub mod dev {
    //! Useful types and methods for developing Storage and Processor implementations.
    pub use background_jobs_core::{
        new_job, new_scheduled_job, process, CachedProcessorMap, JobInfo, NewJobInfo, ProcessorMap,
        ReturnJobInfo, Storage,
    };
}

#[cfg(feature = "background-jobs-actix")]
pub use background_jobs_actix::{
    create_server, create_server_in_arbiter, create_server_in_arbiter_handle, ActixJob,
    QueueHandle, WorkerConfig,
};
