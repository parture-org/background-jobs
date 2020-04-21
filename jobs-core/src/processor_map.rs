use crate::{Job, JobError, JobInfo, ReturnJobInfo};
use log::{error, info};
use serde_json::Value;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

/// A generic function that processes a job
///
/// ProcessorMap stores these `ProcessFn` types that don't expose differences in Job types.
pub type ProcessFn<S> = Arc<
    dyn Fn(Value, S) -> Pin<Box<dyn Future<Output = Result<(), JobError>> + Send>> + Send + Sync,
>;

pub type StateFn<S> = Arc<dyn Fn() -> S + Send + Sync>;

/// A type for storing the relationships between job names and the job itself
///
/// [`Job`]s must be registered with  the `ProcessorMap` in the initialization phase of an
/// application before workers are spawned in order to handle queued jobs.
#[derive(Clone)]
pub struct ProcessorMap<S> {
    inner: HashMap<String, ProcessFn<S>>,
    state_fn: StateFn<S>,
}

/// A type for storing the relationships between job names and the job itself, with the
/// state pre-cached instead of being generated from the state function each time
///
/// [`Job`]s must be registered with  the `ProcessorMap` in the initialization phase of an
/// application before workers are spawned in order to handle queued jobs.
pub struct CachedProcessorMap<S> {
    inner: HashMap<String, ProcessFn<S>>,
    state: S,
}

impl<S> ProcessorMap<S>
where
    S: Clone + 'static,
{
    /// Intialize a `ProcessorMap`
    ///
    /// The state passed into this method will be passed to all jobs executed through this
    /// ProcessorMap. The state argument could be useful for containing a hook into something like
    /// r2d2, or the address of an actor in an actix-based system.
    pub fn new(state_fn: StateFn<S>) -> Self {
        ProcessorMap {
            inner: HashMap::new(),
            state_fn,
        }
    }

    /// Register a [`Job`] with this `ProcessorMap`.
    ///
    /// `ProcessorMap`s are useless if no jobs are registerd before workers are spawned, so
    /// make sure to register all your processors up-front.
    pub fn register<J>(&mut self)
    where
        J: Job<State = S>,
    {
        self.inner.insert(
            J::NAME.to_owned(),
            Arc::new(move |value, state| crate::process::<J>(value, state)),
        );
    }

    /// Initialize the State from the State Function
    pub fn cached(&self) -> CachedProcessorMap<S> {
        CachedProcessorMap {
            inner: self.inner.clone(),
            state: (self.state_fn)(),
        }
    }

    /// Process a given job
    ///
    /// This should not be called from outside implementations of a backgoround-jobs runtime. It is
    /// intended for internal use.
    pub async fn process(&self, job: JobInfo) -> ReturnJobInfo {
        let opt = self
            .inner
            .get(job.name())
            .map(|name| process(name, (self.state_fn)(), job.clone()));

        if let Some(fut) = opt {
            fut.await
        } else {
            error!("Job {} not registered", job.name());
            ReturnJobInfo::unregistered(job.id())
        }
    }
}

impl<S> CachedProcessorMap<S>
where
    S: Clone + 'static,
{
    /// Process a given job
    ///
    /// This should not be called from outside implementations of a backgoround-jobs runtime. It is
    /// intended for internal use.
    pub async fn process(&self, job: JobInfo) -> ReturnJobInfo {
        if let Some(name) = self.inner.get(job.name()) {
            process(name, self.state.clone(), job).await
        } else {
            error!("Job {} not registered", job.name());
            ReturnJobInfo::unregistered(job.id())
        }
    }
}

async fn process<S>(process_fn: &ProcessFn<S>, state: S, job: JobInfo) -> ReturnJobInfo {
    let args = job.args();
    let id = job.id();
    let name = job.name().to_owned();

    match process_fn(args, state).await {
        Ok(_) => {
            info!("Job {} completed, {}", id, name);
            ReturnJobInfo::pass(id)
        }
        Err(e) => {
            info!("Job {} errored, {}, {}", id, name, e);
            ReturnJobInfo::fail(id)
        }
    }
}
