#[macro_use]
extern crate serde_derive;

use failure::Error;
use futures::{
    future::{lazy, IntoFuture},
    Future,
};
use jobs::{JobRunner, MaxRetries, Processor};

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

    fn process(&self, args: Self::Arguments) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        println!("args: {:?}", args);

        Box::new(Ok(()).into_future())
    }
}

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    tokio::run(lazy(|| {
        let mut runner = JobRunner::new(1234, 8, "example-db");
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
        ];

        let _: Vec<_> = jobs
            .into_iter()
            .map(|job| {
                tokio::spawn(
                    handle
                        .queue(MyProcessor::new_job(job, None).unwrap())
                        .then(|_| Ok(())),
                );
            })
            .collect();

        Ok(())
    }));
}
