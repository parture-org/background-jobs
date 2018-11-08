#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use failure::Error;

mod client;
mod server;
mod spawner;

pub use crate::{
    client::ClientConfig,
    server::{ServerConfig, ServerRequest, ServerResponse},
    spawner::SpawnerConfig,
};

fn coerce<T, F>(res: Result<Result<T, Error>, F>) -> Result<T, Error>
where
    F: Into<Error>,
{
    match res {
        Ok(res) => res,
        Err(e) => Err(e.into()),
    }
}
