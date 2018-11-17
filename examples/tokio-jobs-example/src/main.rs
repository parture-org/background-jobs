#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::time::Duration;

use background_jobs::{Backoff, JobRunner, MaxRetries, Processor};
use failure::Error;
use futures::{
    future::{lazy, IntoFuture},
    Future,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MyJobArguments {
    some_usize: usize,
    other_usize: usize,
}

#[derive(Clone, Debug)]
struct MyProcessor;

impl Processor for MyProcessor {
    type Arguments = MyJobArguments;

    fn name() -> &'static str {
        "MyProcessor"
    }

    fn queue() -> &'static str {
        "default"
    }

    fn max_retries() -> MaxRetries {
        MaxRetries::Count(1)
    }

    fn backoff_strategy() -> Backoff {
        Backoff::Exponential(2)
    }

    fn process(&self, args: Self::Arguments) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        info!("args: {:?}", args);

        Box::new(Ok(()).into_future())
    }
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let (_, _, jobs) = (1..18).fold((0, 1, Vec::new()), |(x, y, mut acc), _| {
        acc.push(MyJobArguments {
            some_usize: x,
            other_usize: y,
        });

        (y, x + y, acc)
    });

    tokio::run(lazy(move || {
        let mut runner = JobRunner::new(1234, 4, "example-db");
        runner.register_processor(MyProcessor);

        let handle = runner.spawn();

        for job in jobs {
            tokio::spawn(
                handle
                    .queue(MyProcessor::new_job(job, None, None).unwrap())
                    .then(|_| Ok(())),
            );
        }

        tokio::timer::Delay::new(tokio::clock::now() + Duration::from_secs(1))
            .map(move |_| {
                let _ = handle;
                ()
            })
            .map_err(|_| ())
    }));
}
