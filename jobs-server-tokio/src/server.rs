use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use failure::Error;
use futures::{
    future::{lazy, poll_fn},
    stream::iter_ok,
    Future, Stream,
};
use jobs_core::{JobInfo, Storage};
use tokio::timer::Interval;
use tokio_threadpool::blocking;
use tokio_zmq::{prelude::*, Multipart, Pull, Push};
use zmq::{Context, Message};

use crate::coerce;

#[derive(Clone)]
struct Config {
    ip: String,
    job_port: usize,
    queue_port: usize,
    runner_id: usize,
    db_path: PathBuf,
    context: Arc<Context>,
}

impl Config {
    fn create_server(&self) -> Result<ServerConfig, Error> {
        let pusher = Push::builder(self.context.clone())
            .bind(&format!("tcp://{}:{}", self.ip, self.job_port))
            .build()?;

        let puller = Pull::builder(self.context.clone())
            .bind(&format!("tcp://{}:{}", self.ip, self.queue_port))
            .build()?;

        let storage = Storage::init(self.runner_id, self.db_path.clone())?;

        let server = ServerConfig {
            pusher,
            puller,
            storage,
            config: self.clone(),
        };

        Ok(server)
    }
}

pub struct ServerConfig {
    pusher: Push,
    puller: Pull,
    storage: Storage,
    // TODO: Recover from failure
    #[allow(dead_code)]
    config: Config,
}

impl ServerConfig {
    pub fn init<P: AsRef<Path>>(
        ip: &str,
        job_port: usize,
        queue_port: usize,
        runner_id: usize,
        db_path: P,
    ) -> Result<Self, Error> {
        let context = Arc::new(Context::new());

        Self::init_with_context(ip, job_port, queue_port, runner_id, db_path, context)
    }

    pub fn init_with_context<P: AsRef<Path>>(
        ip: &str,
        job_port: usize,
        queue_port: usize,
        runner_id: usize,
        db_path: P,
        context: Arc<Context>,
    ) -> Result<Self, Error> {
        let config = Config {
            ip: ip.to_owned(),
            job_port,
            queue_port,
            runner_id,
            db_path: db_path.as_ref().to_owned(),
            context,
        };

        config.create_server()
    }

    pub fn run(self) -> impl Future<Item = (), Error = ()> {
        lazy(|| {
            let ServerConfig {
                pusher,
                puller,
                storage,
                config: _,
            } = self;

            let storage2 = storage.clone();

            let fut = Interval::new(tokio::clock::now(), Duration::from_millis(250))
                .from_err()
                .and_then(move |_| dequeue_jobs(storage.clone()))
                .flatten()
                .fold(pusher, move |pusher, multipart| {
                    Box::new(push_job(pusher, multipart))
                });

            tokio::spawn(
                fut.map(|_| ())
                    .map_err(move |e| error!("Error in server, {}", e)),
            );

            puller
                .stream()
                .from_err()
                .and_then(parse_job)
                .and_then(move |job| store_job(job, storage2.clone()))
                .or_else(|e| Ok(error!("Error storing job, {}", e)))
                .for_each(|_| Ok(()))
        })
    }
}

fn dequeue_jobs(
    storage: Storage,
) -> impl Future<Item = impl Stream<Item = Multipart, Error = Error>, Error = Error> {
    poll_fn(move || {
        let storage = storage.clone();
        blocking(move || wrap_fetch_queue(storage))
    })
    .then(coerce)
    .map(|jobs| iter_ok(jobs))
    .or_else(|e| {
        error!("Error fetching jobs, {}", e);
        Ok(iter_ok(vec![]))
    })
}

fn push_job(pusher: Push, message: Multipart) -> impl Future<Item = Push, Error = Error> {
    pusher.send(message).map_err(Error::from)
}

fn store_job(job: JobInfo, storage: Storage) -> impl Future<Item = (), Error = Error> {
    let storage = storage.clone();

    poll_fn(move || {
        let job = job.clone();
        let storage = storage.clone();

        blocking(move || storage.store_job(job).map_err(Error::from)).map_err(Error::from)
    })
    .then(coerce)
}

fn wrap_fetch_queue(storage: Storage) -> Result<Vec<Multipart>, Error> {
    let response = fetch_queue(storage)?;

    let jobs = response
        .into_iter()
        .map(|job| {
            serde_json::to_string(&job)
                .map_err(Error::from)
                .and_then(|json| Message::from_slice(json.as_ref()).map_err(Error::from))
                .map(Multipart::from)
        })
        .collect::<Result<Vec<_>, Error>>()?;

    Ok(jobs)
}

fn fetch_queue(storage: Storage) -> Result<Vec<JobInfo>, Error> {
    storage.dequeue_job(100).map_err(Error::from)
}

fn parse_job(mut multipart: Multipart) -> Result<JobInfo, Error> {
    let unparsed_msg = multipart.pop_front().ok_or(EmptyMessage)?;

    let parsed = serde_json::from_slice(&unparsed_msg)?;

    Ok(parsed)
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Message was empty")]
pub struct EmptyMessage;
