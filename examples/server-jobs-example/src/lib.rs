#[macro_use]
extern crate serde_derive;

use failure::Error;
use futures::{future::IntoFuture, Future};
use jobs::{Backoff, MaxRetries, Processor};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MyJobArguments {
    some_usize: usize,
    other_usize: usize,
}

impl MyJobArguments {
    pub fn new(some_usize: usize, other_usize: usize) -> Self {
        MyJobArguments {
            some_usize,
            other_usize,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MyProcessor;

impl Processor for MyProcessor {
    type Arguments = MyJobArguments;

    fn name() -> &'static str {
        "MyProcessor"
    }

    fn max_retries() -> MaxRetries {
        MaxRetries::Count(1)
    }

    fn backoff_strategy() -> Backoff {
        Backoff::Exponential(2)
    }

    fn process(&self, args: Self::Arguments) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        println!("args: {:?}", args);

        Box::new(Ok(()).into_future())
    }
}
