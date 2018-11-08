use std::sync::Arc;

use failure::Error;
use futures::{
    future::{lazy, Either, IntoFuture},
    Future, Stream,
};
use jobs_core::{Processor, Processors};
use tokio_zmq::{prelude::*, Multipart, Req};
use zmq::{Context, Message};

use crate::{ServerRequest, ServerResponse};

pub struct ClientConfig {
    processors: Vec<Processors>,
    clients: Vec<Req>,
}

impl ClientConfig {
    pub fn init(
        num_processors: usize,
        server_host: &str,
        server_port: usize,
    ) -> Result<Self, Error> {
        let ctx = Arc::new(Context::new());

        let mut clients = Vec::new();

        let processors = (0..num_processors).map(|_| Processors::new()).collect();

        for _ in 0..num_processors {
            clients.push(
                Req::builder(ctx.clone())
                    .connect(&format!("tcp://{}:{}", server_host, server_port))
                    .build()?,
            );
        }

        let cfg = ClientConfig {
            processors,
            clients,
        };

        Ok(cfg)
    }

    pub fn register_processor<P>(&mut self, processor: P)
    where
        P: Processor + Send + Sync + 'static,
    {
        for processors in self.processors.iter_mut() {
            processors.register_processor(processor.clone());
        }
    }

    pub fn run(self) -> impl Future<Item = (), Error = ()> {
        let ClientConfig {
            processors,
            clients,
        } = self;

        lazy(|| {
            for (client, processors) in clients.into_iter().zip(processors) {
                tokio::spawn(client_future(client, processors));
            }

            Ok(())
        })
    }
}

fn client_future(req: Req, processors: Processors) -> impl Future<Item = (), Error = ()> {
    request_one_job()
        .into_future()
        .and_then(|multipart| req.send(multipart).from_err())
        .and_then(|req| {
            let (sink, stream) = req.sink_stream().split();

            stream
                .from_err()
                .and_then(move |multipart| wrap_response(multipart, &processors))
                .forward(sink)
        })
        .map_err(|e| error!("Error in client, {}", e))
        .map(|_| ())
}

fn request_one_job() -> Result<Multipart, Error> {
    serialize_request(ServerRequest::FetchJobs(1))
}

fn serialize_request(request: ServerRequest) -> Result<Multipart, Error> {
    let request = serde_json::to_string(&request)?;
    let msg = Message::from_slice(request.as_ref())?;

    Ok(msg.into())
}

fn parse_multipart(mut multipart: Multipart) -> Result<ServerResponse, Error> {
    let message = multipart.pop_front().ok_or(ParseError)?;

    let parsed = serde_json::from_slice(&message)?;

    Ok(parsed)
}

fn wrap_response(
    multipart: Multipart,
    processors: &Processors,
) -> impl Future<Item = Multipart, Error = Error> {
    let default_request = Either::A(request_one_job().into_future());

    let msg = match parse_multipart(multipart) {
        Ok(msg) => msg,
        Err(e) => {
            error!("Error parsing response, {}", e);
            return default_request;
        }
    };

    let fut = process_response(msg, processors).then(move |res| match res {
        Ok(request) => serialize_request(request),
        Err(e) => {
            error!("Error processing response, {}", e);
            request_one_job()
        }
    });

    Either::B(fut)
}

fn process_response(
    response: ServerResponse,
    processors: &Processors,
) -> impl Future<Item = ServerRequest, Error = Error> {
    match response {
        ServerResponse::FetchJobs(jobs) => {
            let job = match jobs.into_iter().next() {
                Some(job) => job,
                None => return Either::A(Ok(ServerRequest::FetchJobs(1)).into_future()),
            };

            let fut = processors
                .process_job(job)
                .map(ServerRequest::ReturnJob)
                .or_else(|_| Ok(ServerRequest::FetchJobs(1)));

            Either::B(fut)
        }
        _ => return Either::A(Ok(ServerRequest::FetchJobs(1)).into_future()),
    }
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Error parsing response")]
struct ParseError;
