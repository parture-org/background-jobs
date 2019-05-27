/*
 * This file is part of Background Jobs.
 *
 * Copyright © 2019 Riley Trautman
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

use chrono::offset::Utc;
use failure::Fail;
use log::error;

use crate::{JobInfo, NewJobInfo, ReturnJobInfo, Stats};

pub trait Storage: Clone + Send {
    type Error: Fail;

    /// This method generates unique IDs for jobs
    fn generate_id(&mut self) -> Result<u64, Self::Error>;

    /// This method should store the supplied job
    ///
    /// The supplied job _may already be present_. The implementation should overwrite the stored
    /// job with the new job so that future calls to `fetch_job` return the new one.
    fn save_job(&mut self, job: JobInfo) -> Result<(), Self::Error>;

    /// This method should return the job with the given ID regardless of what state the job is in.
    fn fetch_job(&mut self, id: u64) -> Result<Option<JobInfo>, Self::Error>;

    /// This should fetch a job ready to be processed from the queue
    ///
    /// If a job is not ready, is currently running, or is not in the requested queue, this method
    /// should not return it. If no jobs meet these criteria, this method should return Ok(None)
    fn fetch_job_from_queue(&mut self, queue: &str) -> Result<Option<JobInfo>, Self::Error>;

    /// This method tells the storage mechanism to mark the given job as being in the provided
    /// queue
    fn queue_job(&mut self, queue: &str, id: u64) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to mark a given job as running
    fn run_job(&mut self, id: u64, runner_id: u64) -> Result<(), Self::Error>;

    /// This method tells the storage mechanism to remove the job
    ///
    /// This happens when a job has been completed or has failed too many times
    fn delete_job(&mut self, id: u64) -> Result<(), Self::Error>;

    /// This method returns the current statistics, or Stats::default() if none exists.
    fn get_stats(&self) -> Result<Stats, Self::Error>;

    /// This method fetches the existing statistics or Stats::default(), and stores the result of
    /// calling `update_stats` on it.
    fn update_stats<F>(&mut self, f: F) -> Result<(), Self::Error>
    where
        F: Fn(Stats) -> Stats;

    fn new_job(&mut self, job: NewJobInfo) -> Result<u64, Self::Error> {
        let id = self.generate_id()?;

        let job = job.with_id(id);

        let queue = job.queue().to_owned();
        self.save_job(job)?;
        self.queue_job(&queue, id)?;
        self.update_stats(Stats::new_job)?;

        Ok(id)
    }

    fn request_job(&mut self, queue: &str, runner_id: u64) -> Result<Option<JobInfo>, Self::Error> {
        match self.fetch_job_from_queue(queue)? {
            Some(mut job) => {
                if job.is_pending() && job.is_ready(Utc::now()) && job.is_in_queue(queue) {
                    job.run();
                    self.run_job(job.id(), runner_id)?;
                    self.save_job(job.clone())?;
                    self.update_stats(Stats::run_job)?;

                    Ok(Some(job))
                } else {
                    error!(
                        "Not fetching job {}, it is not ready for processing",
                        job.id()
                    );
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    fn return_job(
        &mut self,
        ReturnJobInfo { id, result }: ReturnJobInfo,
    ) -> Result<(), Self::Error> {
        if result.is_failure() {
            if let Some(mut job) = self.fetch_job(id)? {
                if job.needs_retry() {
                    self.queue_job(job.queue(), id)?;
                    self.save_job(job)?;
                    self.update_stats(Stats::retry_job)
                } else {
                    self.delete_job(id)?;
                    self.update_stats(Stats::fail_job)
                }
            } else {
                Ok(())
            }
        } else if result.is_missing_processor() {
            if let Some(mut job) = self.fetch_job(id)? {
                job.pending();
                self.queue_job(job.queue(), id)?;
                self.save_job(job)?;
                self.update_stats(Stats::retry_job)
            } else {
                Ok(())
            }
        } else {
            self.delete_job(id)?;
            self.update_stats(Stats::complete_job)
        }
    }
}
