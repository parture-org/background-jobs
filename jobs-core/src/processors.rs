use std::collections::HashMap;

use futures::future::{Either, Future, IntoFuture};
use serde_json::Value;

use crate::{JobError, JobInfo, Processor};

pub type ProcessFn =
    Box<dyn Fn(Value) -> Box<dyn Future<Item = (), Error = JobError> + Send> + Send + Sync>;

pub struct Processors {
    inner: HashMap<String, ProcessFn>,
}

impl Processors {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.inner.insert(
            P::name().to_owned(),
            Box::new(move |value| processor.do_processing(value)),
        );
    }

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

impl Default for Processors {
    fn default() -> Self {
        Processors {
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
