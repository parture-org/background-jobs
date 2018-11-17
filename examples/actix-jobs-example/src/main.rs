#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use background_jobs::{Backoff, JobsBuilder, MaxRetries, Processor, QueueJob};
use failure::Error;
use futures::{future::IntoFuture, Future};

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

fn main() -> Result<(), Error> {
    dotenv::dotenv().ok();
    env_logger::init();

    let sys = actix::System::new("jobs-system");

    let mut builder = JobsBuilder::new(1234, 4, "example-db");

    builder.register_processor(MyProcessor);

    let jobs_actor = builder.build()?;

    let (_, _, jobs) = (1..18).fold((0, 1, Vec::new()), |(x, y, mut acc), _| {
        acc.push(MyJobArguments {
            some_usize: x,
            other_usize: y,
        });

        (y, x + y, acc)
    });

    for job in jobs {
        jobs_actor.do_send(QueueJob(MyProcessor::new_job(job, None, None)?));
    }

    let _ = sys.run();

    Ok(())
}
