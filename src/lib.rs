#[macro_use]
extern crate failure;
extern crate futures;
#[macro_use]
extern crate log;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::collections::{HashMap, VecDeque};

use failure::Error;
use futures::future::{Future, IntoFuture};
use serde::{de::DeserializeOwned, ser::Serialize};
use serde_json::Value;

#[derive(Debug, Fail)]
pub enum JobError {
    #[fail(display = "Error performing job: {}", _0)]
    Processing(Error),
    #[fail(display = "Could not make JSON value from processable")]
    Json,
    #[fail(display = "No processor available for job")]
    MissingProcessor,
}

pub trait Processable: DeserializeOwned + Serialize {
    fn processor(&self) -> &'static str;
}

pub trait Processor {
    fn name(&self) -> &'static str;

    fn process(&self, processable: Value) -> Box<Future<Item = (), Error = Error> + Send>;
}

#[derive(Deserialize, Serialize)]
pub struct JobInfo {
    args: Value,
    retry_count: usize,
}

impl JobInfo {
    fn increment(mut self) -> Self {
        self.retry_count += 1;
        self
    }
}

#[derive(Deserialize, Serialize)]
pub struct Jobs {
    inner: HashMap<String, VecDeque<JobInfo>>,
}

impl Jobs {
    fn queue<P>(&mut self, processable: P) -> Result<(), JobError>
    where
        P: Processable + 'static,
    {
        let processor = processable.processor().to_owned();
        let v = serde_json::to_value(processable).map_err(|_| JobError::Json)?;

        let queue = self.inner.entry(processor).or_insert(VecDeque::new());
        queue.push_back(JobInfo {
            args: v,
            retry_count: 0,
        });

        Ok(())
    }

    fn requeue(&mut self, name: &str, job: JobInfo) {
        let queue = self.inner.entry(name.to_owned()).or_insert(VecDeque::new());

        queue.push_back(job.increment());
    }
}

pub struct Processors {
    inner: HashMap<String, Box<Processor + Send + Sync + 'static>>,
    jobs: Option<Jobs>,
}

impl Processors {
    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        self.inner
            .insert(processor.name().to_owned(), Box::new(processor));
    }

    pub fn queue<P>(&mut self, processable: P) -> Result<(), JobError>
    where
        P: Processable + 'static,
    {
        if let Some(ref mut jobs) = self.jobs {
            jobs.queue(processable)?;
        }

        Ok(())
    }

    pub fn process(
        mut self,
        name: String,
        job: JobInfo,
    ) -> impl Future<Item = Self, Error = JobError> {
        let mut jobs = self.jobs.take().unwrap();
        let args = job.args.clone();

        let processor = self.inner.remove(&name);

        processor
            .ok_or(JobError::MissingProcessor)
            .into_future()
            .and_then(move |processor| {
                let local_name = name.clone();
                let local_name_2 = name.clone();

                let fut = processor.process(args).then(move |res| match res {
                    Ok(_) => Ok(info!("Job completed, {}", name)),
                    Err(e) => {
                        error!("Job errored, {}, {}", name, e);

                        Err(JobError::Processing(e))
                    }
                });

                self.inner.insert(local_name, processor);

                fut.then(move |res| {
                    match res {
                        Ok(_) => (),
                        Err(_) => {
                            jobs.requeue(&local_name_2, job);
                        }
                    }

                    self.jobs = Some(jobs);

                    Ok(self)
                })
            })
    }
}
