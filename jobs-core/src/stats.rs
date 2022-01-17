use time::OffsetDateTime;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
/// Statistics about the jobs processor
pub struct Stats {
    /// How many jobs are pending execution
    pub pending: usize,

    /// How many jobs are currently executing
    pub running: usize,

    /// How many jobs are permanently failed
    pub dead: JobStat,

    /// How many jobs have completed successfully
    pub complete: JobStat,
}

impl Stats {
    /// A new, empty stats struct
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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
/// A time-based overview of job completion and failures
pub struct JobStat {
    this_hour: usize,
    today: usize,
    this_month: usize,
    all_time: usize,
    updated_at: OffsetDateTime,
}

impl JobStat {
    /// A new, empty job statistic
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
        let now = OffsetDateTime::now_utc();

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

    /// A count from the last hour
    pub fn this_hour(&self) -> usize {
        self.this_hour
    }

    /// A count from the last day
    pub fn today(&self) -> usize {
        self.today
    }

    /// A count from the last month
    pub fn this_month(&self) -> usize {
        self.this_month
    }

    /// A total count
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
            updated_at: OffsetDateTime::now_utc(),
        }
    }
}
