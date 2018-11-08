use std::{path::Path, sync::Arc};

use failure::Error;
use futures::{
    future::{lazy, poll_fn},
    Future, Stream,
};
use jobs_core::{JobInfo, Storage};
use tokio_threadpool::blocking;
use tokio_zmq::{prelude::*, Dealer, Multipart, Rep, Router};
use zmq::{Context, Message};

use crate::coerce;

/// Messages from the client to the server
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ServerRequest {
    /// Request a number of jobs from the server
    FetchJobs(usize),

    /// Return a processed job to the server
    ReturnJob(JobInfo),
}

/// How the server responds to the client
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ServerResponse {
    /// Send a list of jobs to the client
    FetchJobs(Vec<JobInfo>),

    /// Send an OK to the client after a job is returned
    JobReturned,

    /// Could not parse the client's message
    Unparsable,

    /// Server experienced error
    InternalServerError,
}

pub struct ServerConfig {
    servers: Vec<Rep>,
    dealer: Dealer,
    router: Router,
    storage: Storage,
}

impl ServerConfig {
    pub fn init<P: AsRef<Path>>(
        ip: &str,
        port: usize,
        runner_id: usize,
        server_count: usize,
        db_path: P,
    ) -> Result<Self, Error> {
        let context = Arc::new(Context::new());

        let inproc_name = "inproc://jobs-server-tokio";

        let dealer = Dealer::builder(context.clone()).bind(inproc_name).build()?;

        let router = Router::builder(context.clone())
            .bind(&format!("tcp://{}:{}", ip, port))
            .build()?;

        let mut servers = Vec::new();

        for _ in 0..server_count {
            servers.push(Rep::builder(context.clone()).connect(inproc_name).build()?);
        }

        let storage = Storage::init(runner_id, db_path.as_ref().to_owned())?;

        let cfg = ServerConfig {
            servers,
            dealer,
            router,
            storage,
        };

        Ok(cfg)
    }

    pub fn run(self) -> impl Future<Item = (), Error = ()> {
        lazy(|| {
            let ServerConfig {
                servers,
                dealer,
                router,
                storage,
            } = self;

            for server in servers {
                let (sink, stream) = server.sink_stream().split();
                let storage = storage.clone();

                let fut = stream
                    .from_err()
                    .and_then(move |multipart| {
                        let storage = storage.clone();
                        let res = parse_multipart(multipart);

                        poll_fn(move || {
                            let res = res.clone();
                            let storage = storage.clone();
                            blocking(move || wrap_request(res, storage))
                        })
                        .then(coerce)
                    })
                    .forward(sink);

                tokio::spawn(
                    fut.map(|_| ())
                        .map_err(|e| error!("Error in server, {}", e)),
                );
            }

            let (deal_sink, deal_stream) = dealer.sink_stream().split();
            let (rout_sink, rout_stream) = router.sink_stream().split();

            deal_stream
                .forward(rout_sink)
                .join(rout_stream.forward(deal_sink))
                .map_err(|e| error!("Error in broker, {}", e))
                .map(|_| ())
        })
    }
}

fn wrap_request(
    res: Result<ServerRequest, ServerResponse>,
    storage: Storage,
) -> Result<Multipart, Error> {
    let res = res.map(move |msg| process_request(msg, storage));

    let response = match res {
        Ok(response) => response,
        Err(response) => response,
    };

    Ok(Message::from_slice(serde_json::to_string(&response)?.as_ref())?.into())
}

fn parse_multipart(mut multipart: Multipart) -> Result<ServerRequest, ServerResponse> {
    let unparsed_msg = match multipart.pop_front() {
        Some(msg) => msg,
        None => return Err(ServerResponse::Unparsable),
    };

    match serde_json::from_slice(&unparsed_msg) {
        Ok(msg) => Ok(msg),
        Err(_) => Err(ServerResponse::Unparsable),
    }
}

fn process_request(request: ServerRequest, storage: Storage) -> ServerResponse {
    match request {
        ServerRequest::FetchJobs(limit) => storage
            .dequeue_job(limit)
            .map(ServerResponse::FetchJobs)
            .map_err(|e| error!("Error fetching jobs, {}", e))
            .unwrap_or(ServerResponse::InternalServerError),
        ServerRequest::ReturnJob(job) => storage
            .store_job(job)
            .map(|_| ServerResponse::JobReturned)
            .map_err(|e| error!("Error returning job, {}", e))
            .unwrap_or(ServerResponse::InternalServerError),
    }
}
