use failure::Error;
use jobs::ServerConfig;

fn main() -> Result<(), Error> {
    dotenv::dotenv().ok();
    env_logger::init();

    let config = ServerConfig::init("127.0.0.1", 5555, 5556, 1, "example-db")?;

    tokio::run(config.run());

    Ok(())
}
