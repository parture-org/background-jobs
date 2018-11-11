use std::{collections::BTreeMap, sync::Arc};

use failure::Error;
use futures::{future::lazy, Future};
use tokio_zmq::{prelude::*, Req};
use zmq::{Context, Message};

pub(crate) struct PortMap;

impl PortMap {
    pub(crate) fn init(
        address: String,
        context: Arc<Context>,
    ) -> impl Future<Item = BTreeMap<String, usize>, Error = Error> {
        lazy(move || {
            let req = Req::builder(context.clone()).connect(&address).build()?;

            Ok(req)
        })
        .and_then(|req| {
            Message::from_slice(b"h")
                .map_err(Error::from)
                .map(move |msg| (req, msg.into()))
        })
        .and_then(|(req, msg)| req.send(msg).and_then(|req| req.recv()).from_err())
        .and_then(|(mut multipart, _)| {
            let msg = multipart.pop_front().ok_or(EmptyMessage)?;

            let map = serde_json::from_slice(&msg)?;

            Ok(map)
        })
    }
}

#[derive(Clone, Debug, Fail)]
#[fail(display = "Message was empty")]
struct EmptyMessage;
