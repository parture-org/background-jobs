mod bucket;

use self::bucket::Buckets;
use metrics::{CounterFn, GaugeFn, HistogramFn, Key, Metadata, Recorder, SetRecorderError};
use metrics_util::registry::{Registry, Storage};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

/// Handle into the stats store
pub struct StatsHandle {
    storage: Arc<StatsStorageInner>,
}

/// Recorder type for recording background jobs metrics
pub struct StatsRecorder {
    registry: Registry<Key, StatsStorage>,
}

pub struct StatsStorage {
    inner: Arc<StatsStorageInner>,
}

struct StatsStorageInner {
    queued: Arc<AtomicU64>,
    started: Arc<AtomicU64>,
    finished: Arc<AtomicU64>,
    complete: Arc<Mutex<JobStatStorage>>,
    dead: Arc<Mutex<JobStatStorage>>,
}

pub struct JobStatStorage {
    hour: Buckets,
    day: Buckets,
    month: Buckets,
    total: u64,
}

#[derive(Clone)]
pub enum Counter {
    Buckets { buckets: Arc<Mutex<JobStatStorage>> },
    Atomic { atomic: Arc<AtomicU64> },
    Empty,
}

#[derive(Clone)]
pub struct Gauge;

#[derive(Clone)]
pub struct Histogram;

/// Stats about a particular field
#[derive(Clone, Debug)]
pub struct JobStat {
    /// Occurrences in the last hour
    pub hour: u64,

    /// Occurrences in the last day
    pub day: u64,

    /// Occurrences in the last month
    pub month: u64,

    /// Total occurrences
    pub total: u64,
}

/// Stats about the background jobs processor
#[derive(Clone, Debug)]
pub struct Stats {
    /// Jobs that have not yet executed
    pub pending: u64,

    /// Jobs that are currently executing
    pub running: u64,

    /// Jobs that have completed
    pub complete: JobStat,

    /// Jobs that are dead
    pub dead: JobStat,
}

impl StatsHandle {
    /// Get the current stats about the background jobs processing
    ///
    /// ```rust
    /// # let (recorder, handle) = background_jobs_metrics::StatsRecorder::build();
    /// println!("{:?}", handle.get());
    /// ```
    pub fn get(&self) -> Stats {
        self.storage.snapshot()
    }
}

impl StatsRecorder {
    /// Install the stats recorder into the process
    ///
    /// ```rust
    /// # use background_jobs_metrics::StatsRecorder;
    /// StatsRecorder::install().expect("Failed to install recorder");
    /// ```
    pub fn install() -> Result<StatsHandle, SetRecorderError<StatsRecorder>> {
        let (recorder, handle) = Self::build();

        metrics::set_global_recorder(recorder)?;

        Ok(handle)
    }

    /// Build the stats recorder and fetch the handle.
    ///
    /// This can be used in conjunction with `metrics_util::layers::FanoutBuilder` to add it in
    /// addition to another recorder
    ///
    /// ```rust
    /// # use background_jobs_metrics::StatsRecorder;
    /// let (jobs_recorder, handle) = StatsRecorder::build();
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
        let storage = Arc::new(StatsStorageInner::new());

        let registry = Registry::new(StatsStorage {
            inner: Arc::clone(&storage),
        });

        (StatsRecorder { registry }, StatsHandle { storage })
    }
}

impl JobStatStorage {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> JobStat {
        JobStat {
            hour: self.hour.sum(),
            day: self.day.sum(),
            month: self.month.sum(),
            total: self.total,
        }
    }
}

impl StatsStorageInner {
    fn new() -> Self {
        Self::default()
    }

    fn snapshot(&self) -> Stats {
        let complete = self.complete.lock().unwrap().snapshot();
        let dead = self.dead.lock().unwrap().snapshot();
        let queued = self.queued.load(Ordering::Relaxed);

        let started = self.started.load(Ordering::Relaxed);
        let finished = self.finished.load(Ordering::Relaxed);

        let running = started.saturating_sub(finished);

        let pending = queued
            .saturating_sub(complete.total)
            .saturating_sub(dead.total)
            .saturating_sub(running);

        Stats {
            pending,
            running,
            complete,
            dead,
        }
    }
}

impl Recorder for StatsRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> metrics::Counter {
        self.registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> metrics::Gauge {
        self.registry.get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> metrics::Histogram {
        self.registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}

impl Storage<Key> for StatsStorage {
    type Counter = Arc<Counter>;
    type Gauge = Arc<Gauge>;
    type Histogram = Arc<Histogram>;

    fn counter(&self, key: &Key) -> Self::Counter {
        match key.name() {
            "background-jobs.job.created" => Arc::new(Counter::Atomic {
                atomic: Arc::clone(&self.inner.queued),
            }),
            "background-jobs.job.started" => Arc::new(Counter::Atomic {
                atomic: Arc::clone(&self.inner.started),
            }),
            "background-jobs.job.finished" => Arc::new(Counter::Atomic {
                atomic: Arc::clone(&self.inner.finished),
            }),
            "background-jobs.job.completed" => Arc::new(Counter::Buckets {
                buckets: Arc::clone(&self.inner.complete),
            }),
            "background-jobs.job.dead" => Arc::new(Counter::Buckets {
                buckets: Arc::clone(&self.inner.dead),
            }),
            _ => Arc::new(Counter::Empty),
        }
    }

    fn gauge(&self, _: &Key) -> Self::Gauge {
        Arc::new(Gauge)
    }

    fn histogram(&self, _: &Key) -> Self::Histogram {
        Arc::new(Histogram)
    }
}

impl CounterFn for Counter {
    fn increment(&self, value: u64) {
        match self {
            Self::Buckets { ref buckets } => {
                let timestamp = Instant::now();

                let mut guard = buckets.lock().unwrap();
                guard.hour.count(value, timestamp);
                guard.day.count(value, timestamp);
                guard.month.count(value, timestamp);
                guard.total = guard.total.saturating_add(value);
            }
            Self::Atomic { ref atomic } => {
                let mut current = atomic.load(Ordering::Acquire);
                loop {
                    match atomic.compare_exchange_weak(
                        current,
                        current + value,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(x) => current = x,
                    }
                }
            }
            Self::Empty => {}
        }
    }

    fn absolute(&self, value: u64) {
        match self {
            Self::Buckets { ref buckets } => {
                let mut guard = buckets.lock().unwrap();
                if guard.total < value {
                    guard.total = value;
                }
            }
            Self::Atomic { ref atomic } => {
                let mut current = atomic.load(Ordering::Acquire);
                loop {
                    match atomic.compare_exchange_weak(
                        current,
                        value,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(x) => current = x,
                    }
                }
            }
            Self::Empty => {}
        }
    }
}

impl GaugeFn for Gauge {
    fn increment(&self, _: f64) {}

    fn decrement(&self, _: f64) {}

    fn set(&self, _: f64) {}
}

impl HistogramFn for Histogram {
    fn record(&self, _: f64) {}
}

const SECONDS: u64 = 1;
const MINUTES: u64 = 60 * SECONDS;
const HOURS: u64 = 60 * MINUTES;
const DAYS: u64 = 24 * HOURS;
const MONTHS: u64 = 30 * DAYS;

impl Default for JobStatStorage {
    fn default() -> Self {
        JobStatStorage {
            hour: Buckets::new(
                Duration::from_secs(1 * HOURS),
                Duration::from_secs(3 * MINUTES),
                20,
            ),
            day: Buckets::new(
                Duration::from_secs(1 * DAYS),
                Duration::from_secs(1 * HOURS),
                24,
            ),
            month: Buckets::new(
                Duration::from_secs(1 * MONTHS),
                Duration::from_secs(1 * DAYS),
                30,
            ),
            total: 0,
        }
    }
}

impl Default for StatsStorageInner {
    fn default() -> Self {
        Self {
            queued: Arc::new(AtomicU64::new(0)),
            started: Arc::new(AtomicU64::new(0)),
            finished: Arc::new(AtomicU64::new(0)),
            complete: Arc::new(Mutex::new(JobStatStorage::new())),
            dead: Arc::new(Mutex::new(JobStatStorage::new())),
        }
    }
}
