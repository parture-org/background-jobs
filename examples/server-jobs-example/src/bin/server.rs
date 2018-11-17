use background_jobs::ServerConfig;
use failure::Error;
use server_jobs_example::queue_set;

fn main() -> Result<(), Error> {
    dotenv::dotenv().ok();
    env_logger::init();

    tokio::run(ServerConfig::init(
        "127.0.0.1",
        5555,
        1,
        queue_set(),
        "example-db",
    ));

    Ok(())
}
