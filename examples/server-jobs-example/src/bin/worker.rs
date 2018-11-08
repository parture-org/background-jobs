use failure::Error;
use jobs::ClientConfig;
use server_jobs_example::MyProcessor;

fn main() -> Result<(), Error> {
    let mut client = ClientConfig::init(16, "localhost", 5555)?;

    client.register_processor(MyProcessor);

    tokio::run(client.run());

    Ok(())
}
