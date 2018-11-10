#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;

use failure::Error;

mod server;
mod spawner;
mod worker;

pub use crate::{server::ServerConfig, spawner::SpawnerConfig, worker::WorkerConfig};

fn coerce<T, F>(res: Result<Result<T, Error>, F>) -> Result<T, Error>
where
    F: Into<Error>,
{
    match res {
        Ok(res) => res,
        Err(e) => Err(e.into()),
    }
}
