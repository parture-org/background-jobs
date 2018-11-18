/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2018 Riley Trautman
 *
 * Background Jobs is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Background Jobs is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Background Jobs.  If not, see <http://www.gnu.org/licenses/>.
 */

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
