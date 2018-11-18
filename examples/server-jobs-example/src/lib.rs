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

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::collections::{BTreeMap, BTreeSet};

use background_jobs::{Backoff, Job, MaxRetries, Processor};
use failure::Error;
use futures::{future::IntoFuture, Future};

const DEFAULT_QUEUE: &'static str = "default";

pub fn queue_map() -> BTreeMap<String, usize> {
    let mut map = BTreeMap::new();
    map.insert(DEFAULT_QUEUE.to_owned(), 18);

    map
}

pub fn queue_set() -> BTreeSet<String> {
    queue_map().keys().cloned().collect()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MyJob {
    some_usize: usize,
    other_usize: usize,
}

impl MyJob {
    pub fn new(some_usize: usize, other_usize: usize) -> Self {
        MyJob {
            some_usize,
            other_usize,
        }
    }
}

impl Job for MyJob {
    fn run(self, _: ()) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        info!("args: {:?}", self);

        Box::new(Ok(()).into_future())
    }
}

#[derive(Clone, Debug)]
pub struct MyProcessor;

impl Processor for MyProcessor {
    type Job = MyJob;

    const NAME: &'static str = "MyProcessor";
    const QUEUE: &'static str = DEFAULT_QUEUE;
    const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);
    const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);
}
