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

use std::{collections::BTreeMap, sync::Arc};

use failure::{Error, Fail};
use futures::Future;
#[cfg(feature = "futures-zmq")]
use futures_zmq::{prelude::*, Req};
#[cfg(feature = "tokio-zmq")]
use tokio_zmq::{prelude::*, Req};
use zmq::{Context, Message};

pub(crate) struct PortMap;

impl PortMap {
    pub(crate) fn init(
        address: String,
        context: Arc<Context>,
    ) -> impl Future<Item = BTreeMap<String, usize>, Error = Error> {
        Req::builder(context.clone())
            .connect(&address)
            .build()
            .from_err()
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
