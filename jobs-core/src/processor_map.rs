use crate::{catch_unwind::catch_unwind, Job, JobError, JobInfo, ReturnJobInfo};
use chrono::Utc;
use serde_json::Value;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tracing::{error, Span};
use tracing_futures::Instrument;
use uuid::Uuid;

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
        let span = job_span(&job);

        let fut = async move {
            let opt = self
                .inner
                .get(job.name())
                .map(|name| process(Arc::clone(name), (self.state_fn)(), job.clone()));

            let res = if let Some(fut) = opt {
                fut.await
            } else {
                let span = Span::current();
                span.record(
                    "exception.message",
                    &tracing::field::display("Not registered"),
                );
                span.record(
                    "exception.details",
                    &tracing::field::display("Not registered"),
                );
                error!("Not registered");
                ReturnJobInfo::unregistered(job.id())
            };

            res
        };

        fut.instrument(span).await
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
        let span = job_span(&job);

        let fut = async move {
            let res = if let Some(name) = self.inner.get(job.name()) {
                process(Arc::clone(name), self.state.clone(), job).await
            } else {
                let span = Span::current();
                span.record(
                    "exception.message",
                    &tracing::field::display("Not registered"),
                );
                span.record(
                    "exception.details",
                    &tracing::field::display("Not registered"),
                );
                error!("Not registered");
                ReturnJobInfo::unregistered(job.id())
            };

            res
        };

        fut.instrument(span).await
    }
}

fn job_span(job: &JobInfo) -> Span {
    tracing::info_span!(
        "Job",
        execution_id = tracing::field::display(&Uuid::new_v4()),
        job.id = tracing::field::display(&job.id()),
        job.name = tracing::field::display(&job.name()),
        job.args = tracing::field::debug(&job.args()),
        job.execution_time = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        exception.details = tracing::field::Empty,
    )
}

async fn process<S>(process_fn: ProcessFn<S>, state: S, job: JobInfo) -> ReturnJobInfo
where
    S: Clone,
{
    let args = job.args();
    let id = job.id();

    let start = Utc::now();

    let state_mtx = std::sync::Mutex::new(state);
    let process_mtx = std::sync::Mutex::new(process_fn);

    let res = match std::panic::catch_unwind(|| {
        let state = state_mtx.lock().unwrap().clone();
        (process_mtx.lock().unwrap())(args, state)
    }) {
        Ok(fut) => catch_unwind(fut).await,
        Err(e) => Err(e),
    };
    let end = Utc::now();

    let duration = end - start;
    let microseconds = duration.num_microseconds();
    let seconds: f64 = if let Some(m) = microseconds {
        m as f64 / 1_000_000_f64
    } else {
        0_f64
    };

    let span = Span::current();
    span.record("job.execution_time", &tracing::field::display(&seconds));

    match res {
        Ok(Ok(_)) => {
            #[cfg(feature = "completion-logging")]
            tracing::info!("Job completed");

            ReturnJobInfo::pass(id)
        }
        Ok(Err(e)) => {
            let display = format!("{}", e);
            let debug = format!("{:?}", e);
            span.record("exception.message", &tracing::field::display(&display));
            span.record("exception.details", &tracing::field::display(&debug));
            #[cfg(feature = "error-logging")]
            tracing::warn!("Job errored: {:?}", e);
            ReturnJobInfo::fail(id)
        }
        Err(_) => {
            span.record(
                "exception.message",
                &tracing::field::display("Job panicked"),
            );
            span.record(
                "exception.details",
                &tracing::field::display("Job panicked"),
            );
            #[cfg(feature = "error-logging")]
            tracing::warn!("Job panicked");
            ReturnJobInfo::fail(id)
        }
    }
}
