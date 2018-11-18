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

use std::collections::HashMap;

use futures::future::{Either, Future, IntoFuture};
use serde_json::Value;

use crate::{JobError, JobInfo, Processor};

/// A generic function that processes a job
///
/// Instead of storing
/// [`Processor`](https://docs.rs/background-jobs/0.1.0/background_jobs/struct.Processor) type
/// directly, the
/// [`ProcessorMap`](https://docs.rs/background-jobs/0.1.0/background_jobs/struct.ProcessorMap)
/// struct stores these `ProcessFn` types that don't expose differences in Job types.
pub type ProcessFn =
    Box<dyn Fn(Value) -> Box<dyn Future<Item = (), Error = JobError> + Send> + Send + Sync>;

/// A type for storing the relationships between processor names and the processor itself
///
/// [`Processor`s](https://docs.rs/background-jobs/0.1.0/background_jobs/struct.Processor) must be
/// registered with  the `ProcessorMap` in the initialization phase of an application before
/// workers are spawned in order to handle queued jobs.
pub struct ProcessorMap {
    inner: HashMap<String, ProcessFn>,
}

impl ProcessorMap {
    /// Intialize a `ProcessorMap`
    pub fn new() -> Self {
        Default::default()
    }

    /// Register a
    /// [`Processor`](https://docs.rs/background-jobs/0.1.0/background_jobs/struct.Processor) with
    /// this `ProcessorMap`.
    ///
    /// `ProcessorMap`s are useless if no processors are registerd before workers are spawned, so
    /// make sure to register all your processors up-front.
    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.inner.insert(
            P::name().to_owned(),
            Box::new(move |value| processor.process(value)),
        );
    }

    /// Process a given job
    ///
    /// This should not be called from outside implementations of a backgoround-jobs runtime. It is
    /// intended for internal use.
    pub fn process_job(&self, job: JobInfo) -> impl Future<Item = JobInfo, Error = ()> {
        let opt = self
            .inner
            .get(job.processor())
            .map(|processor| process(processor, job.clone()));

        if let Some(fut) = opt {
            Either::A(fut)
        } else {
            error!("Processor {} not present", job.processor());
            Either::B(Ok(job).into_future())
        }
    }
}

impl Default for ProcessorMap {
    fn default() -> Self {
        ProcessorMap {
            inner: Default::default(),
        }
    }
}

fn process(process_fn: &ProcessFn, mut job: JobInfo) -> impl Future<Item = JobInfo, Error = ()> {
    let args = job.args();

    let processor = job.processor().to_owned();

    process_fn(args).then(move |res| match res {
        Ok(_) => {
            info!("Job completed, {}", processor);
            job.pass();
            Ok(job)
        }
        Err(e) => {
            error!("Job errored, {}, {}", processor, e);
            job.fail();
            Ok(job)
        }
    })
}
