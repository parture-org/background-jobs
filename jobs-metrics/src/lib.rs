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

#[derive(Clone, Debug)]
/// A wrapper for any Storage type adding metrics
pub struct MetricsStorage<S>(S);

impl<S> MetricsStorage<S> {
    /// Add metrics to a provided Storage
    pub const fn wrap(storage: S) -> MetricsStorage<S>
    where
        S: background_jobs_core::Storage,
    {
        Self(storage)
    }
}

#[async_trait::async_trait]
impl<S> background_jobs_core::Storage for MetricsStorage<S>
where
    S: background_jobs_core::Storage + Sync,
{
    type Error = S::Error;

    async fn info(
        &self,
        job_id: uuid::Uuid,
    ) -> Result<Option<background_jobs_core::JobInfo>, Self::Error> {
        self.0.info(job_id).await
    }

    async fn push(&self, job: background_jobs_core::NewJobInfo) -> Result<uuid::Uuid, Self::Error> {
        let queue = job.queue().to_string();
        let name = job.name().to_string();

        let uuid = self.0.push(job).await?;

        metrics::counter!("background-jobs.job.created", "queue" => queue, "name" => name)
            .increment(1);

        Ok(uuid)
    }

    async fn pop(
        &self,
        queue: &str,
        runner_id: uuid::Uuid,
    ) -> Result<background_jobs_core::JobInfo, Self::Error> {
        let job_info = self.0.pop(queue, runner_id).await?;

        metrics::counter!("background-jobs.job.started", "queue" => job_info.queue.clone(), "name" => job_info.name.clone()).increment(1);

        Ok(job_info)
    }

    async fn heartbeat(
        &self,
        job_id: uuid::Uuid,
        runner_id: uuid::Uuid,
    ) -> Result<(), Self::Error> {
        self.0.heartbeat(job_id, runner_id).await
    }

    async fn complete(
        &self,
        return_job_info: background_jobs_core::ReturnJobInfo,
    ) -> Result<bool, Self::Error> {
        let info = if let Some(info) = self.0.info(return_job_info.id).await? {
            Some(info)
        } else {
            tracing::warn!("Returned non-existant job");
            metrics::counter!("background-jobs.job.missing").increment(1);
            None
        };

        let result = return_job_info.result;

        let completed = self.0.complete(return_job_info).await?;

        if let Some(info) = info {
            metrics::counter!("background-jobs.job.finished", "queue" => info.queue.clone(), "name" => info.name.clone()).increment(1);

            match result {
                background_jobs_core::JobResult::Success => {
                    metrics::counter!("background-jobs.job.completed", "queue" => info.queue, "name" => info.name).increment(1);
                }
                background_jobs_core::JobResult::Failure if completed => {
                    metrics::counter!("background-jobs.job.dead", "queue" => info.queue, "name" => info.name).increment(1);
                }
                background_jobs_core::JobResult::Failure => {
                    metrics::counter!("background-jobs.job.failed", "queue" => info.queue, "name" => info.name).increment(1);
                }
                background_jobs_core::JobResult::Unexecuted
                | background_jobs_core::JobResult::Unregistered => {
                    metrics::counter!("background-jobs.job.returned", "queue" => info.queue, "name" => info.name).increment(1);
                }
            }
        }

        Ok(completed)
    }
}
