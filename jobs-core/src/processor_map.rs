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
use log::{error, info};
use serde_json::Value;

use crate::{JobError, JobInfo, Processor};

/// A generic function that processes a job
///
/// Instead of storing
/// [`Processor`](https://docs.rs/background-jobs/0.4.0/background_jobs/trait.Processor.html) type
/// directly, the
/// [`ProcessorMap`](https://docs.rs/background-jobs-core/0.4.0/background_jobs_core/struct.ProcessorMap.html)
/// struct stores these `ProcessFn` types that don't expose differences in Job types.
pub type ProcessFn =
    Box<dyn Fn(Value) -> Box<dyn Future<Item = (), Error = JobError> + Send> + Send>;

/// A type for storing the relationships between processor names and the processor itself
///
/// [`Processor`s](https://docs.rs/background-jobs/0.4.0/background_jobs/trait.Processor.html) must
/// be registered with  the `ProcessorMap` in the initialization phase of an application before
/// workers are spawned in order to handle queued jobs.
pub struct ProcessorMap<S>
where
    S: Clone,
{
    inner: HashMap<String, ProcessFn>,
    state: S,
}

impl<S> ProcessorMap<S>
where
    S: Clone + Send + 'static,
{
    /// Intialize a `ProcessorMap`
    ///
    /// The state passed into this method will be passed to all jobs executed through this
    /// ProcessorMap. The state argument could be useful for containing a hook into something like
    /// r2d2, or the address of an actor in an actix-based system.
    pub fn new(state: S) -> Self {
        ProcessorMap {
            inner: HashMap::new(),
            state,
        }
    }

    /// Register a
    /// [`Processor`](https://docs.rs/background-jobs/0.4.0/background_jobs/trait.Processor.html) with
    /// this `ProcessorMap`.
    ///
    /// `ProcessorMap`s are useless if no processors are registerd before workers are spawned, so
    /// make sure to register all your processors up-front.
    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor<S> + Send + 'static,
    {
        let state = self.state.clone();

        self.inner.insert(
            P::NAME.to_owned(),
            Box::new(move |value| processor.process(value, state.clone())),
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

fn process(process_fn: &ProcessFn, mut job: JobInfo) -> impl Future<Item = JobInfo, Error = ()> {
    let args = job.args();

    let processor = job.processor().to_owned();

    process_fn(args).then(move |res| match res {
        Ok(_) => {
            info!("Job {} completed, {}", job.id(), processor);
            job.pass();
            Ok(job)
        }
        Err(e) => {
            error!("Job {} errored, {}, {}", job.id(), processor, e);
            job.fail();
            Ok(job)
        }
    })
}
