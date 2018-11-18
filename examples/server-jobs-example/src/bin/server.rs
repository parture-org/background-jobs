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
