use background_jobs::{Processor, SpawnerConfig};
use futures::{future::lazy, Future};
use server_jobs_example::{MyJobArguments, MyProcessor};

fn main() {
    dotenv::dotenv().ok();
    env_logger::init();

    let (_, _, jobs) = (1..50).fold((0, 1, Vec::new()), |(x, y, mut acc), _| {
        acc.push(MyJobArguments::new(x, y));

        (y, x + y, acc)
    });

    let spawner = SpawnerConfig::new("localhost", 5555);

    tokio::run(lazy(move || {
        for job in jobs {
            tokio::spawn(
                spawner
                    .queue(MyProcessor::new_job(job, None, None).unwrap())
                    .map_err(|_| ()),
            );
        }

        Ok(())
    }));
}
