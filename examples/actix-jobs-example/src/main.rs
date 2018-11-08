#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use failure::Error;
use futures::{future::IntoFuture, Future};
use jobs::{Backoff, JobsBuilder, MaxRetries, Processor, QueueJob};

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

fn main() -> Result<(), Error> {
    dotenv::dotenv().ok();
    env_logger::init();

    let sys = actix::System::new("jobs-system");

    let mut builder = JobsBuilder::new(1234, 4, "example-db");

    builder.register_processor(MyProcessor);

    let jobs_actor = builder.build()?;

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

    for job in jobs {
        jobs_actor.do_send(QueueJob(MyProcessor::new_job(job, None, None)?));
    }

    let _ = sys.run();

    Ok(())
}
