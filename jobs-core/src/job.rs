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
use futures::Future;
use serde::{de::DeserializeOwned, ser::Serialize};

use crate::{Backoff, MaxRetries};

/// The Job trait defines parameters pertaining to an instance of background job
pub trait Job: Serialize + DeserializeOwned {
    /// Users of this library must define what it means to run a job.
    ///
    /// This should contain all the logic needed to complete a job. If that means queuing more
    /// jobs, sending an email, shelling out (don't shell out), or doing otherwise lengthy
    /// processes, that logic should all be called from inside this method.
    fn run(self) -> Box<dyn Future<Item = (), Error = Error> + Send>;

    /// If this job should not use the default queue for its processor, this can be overridden in
    /// user-code.
    ///
    /// Jobs will only be processed by processors that are registered, and if a queue is supplied
    /// here that is not associated with a valid processor for this job, it will never be
    /// processed.
    fn queue(&self) -> Option<&str> {
        None
    }

    /// If this job should not use the default maximum retry count for its processor, this can be
    /// overridden in user-code.
    fn max_retries(&self) -> Option<MaxRetries> {
        None
    }

    /// If this job should not use the default backoff strategy for its processor, this can be
    /// overridden in user-code.
    fn backoff_strategy(&self) -> Option<Backoff> {
        None
    }
}