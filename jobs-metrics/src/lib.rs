/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2023 Riley Trautman
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

//! Types for collecting stats from background-jobs

mod recorder;

pub use metrics::SetRecorderError;

pub use recorder::{JobStat, Stats, StatsHandle, StatsRecorder};

/// Install the stats recorder into the process
///
/// ```rust
/// background_jobs_metrics::install().expect("Failed to install recorder");
/// ```
pub fn install() -> Result<StatsHandle, SetRecorderError<StatsRecorder>> {
    StatsRecorder::install()
}

/// Build the stats recorder and fetch the handle.
///
/// This can be used in conjunction with `metrics_util::layers::FanoutBuilder` to add it in
/// addition to another recorder
///
/// ```rust
/// let (jobs_recorder, handle) = background_jobs_metrics::build();
///
/// let recorder = metrics_util::layers::FanoutBuilder::default()
///     .add_recorder(jobs_recorder)
///     .build();
///
/// metrics::set_boxed_recorder(Box::new(recorder)).expect("Failed to set recorder");
///
/// println!("{:?}", handle.get());
/// ```
pub fn build() -> (StatsRecorder, StatsHandle) {
    StatsRecorder::build()
}
