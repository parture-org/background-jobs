use failure::Error;
use jobs::WorkerConfig;
use server_jobs_example::MyProcessor;

fn main() -> Result<(), Error> {
    let mut worker = WorkerConfig::init(16, "localhost", 5555, 5556)?;

    worker.register_processor(MyProcessor);

    tokio::run(worker.run());

    Ok(())
}
