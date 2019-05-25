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

use chrono::{offset::Utc, DateTime, Datelike, Timelike};
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Stats {
    pub pending: usize,
    pub running: usize,
    pub dead: JobStat,
    pub complete: JobStat,
}

impl Stats {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn new_job(mut self) -> Self {
        self.pending += 1;
        self
    }

    pub(crate) fn run_job(mut self) -> Self {
        if self.pending > 0 {
            self.pending -= 1;
        }
        self.running += 1;
        self
    }

    pub(crate) fn retry_job(mut self) -> Self {
        self.pending += 1;
        if self.running > 0 {
            self.running -= 1;
        }
        self
    }

    pub(crate) fn fail_job(mut self) -> Self {
        if self.running > 0 {
            self.running -= 1;
        }
        self.dead.increment();
        self
    }

    pub(crate) fn complete_job(mut self) -> Self {
        if self.running > 0 {
            self.running -= 1;
        }
        self.complete.increment();
        self
    }
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            pending: 0,
            running: 0,
            dead: JobStat::default(),
            complete: JobStat::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JobStat {
    this_hour: usize,
    today: usize,
    this_month: usize,
    all_time: usize,
    updated_at: DateTime<Utc>,
}

impl JobStat {
    pub fn new() -> Self {
        Self::default()
    }

    fn increment(&mut self) {
        self.tick();

        self.this_hour += 1;
        self.today += 1;
        self.this_month += 1;
        self.all_time += 1;
    }

    fn tick(&mut self) {
        let now = Utc::now();

        if now.month() != self.updated_at.month() {
            self.next_month();
        } else if now.day() != self.updated_at.day() {
            self.next_day();
        } else if now.hour() != self.updated_at.hour() {
            self.next_hour();
        }

        self.updated_at = now;
    }

    fn next_hour(&mut self) {
        self.this_hour = 0;
    }

    fn next_day(&mut self) {
        self.next_hour();
        self.today = 0;
    }

    fn next_month(&mut self) {
        self.next_day();
        self.this_month = 0;
    }

    pub fn this_hour(&self) -> usize {
        self.this_hour
    }

    pub fn today(&self) -> usize {
        self.today
    }

    pub fn this_month(&self) -> usize {
        self.this_month
    }

    pub fn all_time(&self) -> usize {
        self.all_time
    }
}

impl Default for JobStat {
    fn default() -> Self {
        JobStat {
            this_hour: 0,
            today: 0,
            this_month: 0,
            all_time: 0,
            updated_at: Utc::now(),
        }
    }
}
