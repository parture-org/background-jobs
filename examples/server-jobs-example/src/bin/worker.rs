use background_jobs::WorkerConfig;
use failure::Error;
use server_jobs_example::{queue_map, MyProcessor};

fn main() -> Result<(), Error> {
    dotenv::dotenv().ok();
    env_logger::init();

    let mut worker = WorkerConfig::new("localhost".to_owned(), 5555, queue_map());

    worker.register_processor(MyProcessor);

    tokio::run(worker.run());

    Ok(())
}
