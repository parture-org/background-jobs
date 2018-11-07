#[macro_use]
extern crate serde_derive;

use std::time::Duration;

use failure::Error;
use futures::{
    future::{lazy, IntoFuture},
    Future,
};
use jobs::{Backoff, JobRunner, MaxRetries, Processor};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MyJobArguments {
    some_usize: usize,
    other_usize: usize,
}

struct MyProcessor;

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

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    tokio::run(
        lazy(|| {
            let mut runner = JobRunner::new(1234, 4, "example-db");
            runner.register_processor(MyProcessor);

            let handle = runner.spawn();

            let jobs = vec![
                MyJobArguments {
                    some_usize: 0,
                    other_usize: 1,
                },
                MyJobArguments {
                    some_usize: 1,
                    other_usize: 2,
                },
                MyJobArguments {
                    some_usize: 3,
                    other_usize: 5,
                },
                MyJobArguments {
                    some_usize: 8,
                    other_usize: 13,
                },
                MyJobArguments {
                    some_usize: 21,
                    other_usize: 34,
                },
                MyJobArguments {
                    some_usize: 55,
                    other_usize: 89,
                },
                MyJobArguments {
                    some_usize: 144,
                    other_usize: 233,
                },
                MyJobArguments {
                    some_usize: 377,
                    other_usize: 610,
                },
                MyJobArguments {
                    some_usize: 987,
                    other_usize: 1597,
                },
            ];

            let _: Vec<_> = jobs
                .into_iter()
                .map(|job| {
                    tokio::spawn(
                        handle
                            .queue(MyProcessor::new_job(job, None, None).unwrap())
                            .then(|_| Ok(())),
                    );
                })
                .collect();

            Ok(handle)
        })
        .and_then(|handle| {
            tokio::timer::Delay::new(tokio::clock::now() + Duration::from_secs(2))
                .map(move |_| {
                    let _ = handle;
                    ()
                })
                .map_err(|_| ())
        }),
    );
}
