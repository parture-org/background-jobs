#![deny(missing_docs)]

//! # Background Jobs Sled Storage
//! _An implementation of the Background Jobs Storage trait based on the Sled embedded database_
//!
//! ### Usage
//! ```rust,ignore
//! use background_jobs::{ServerConfig, sled_storage::Storage};
//! use sled_extensions::{ConfigBuilder, Db};
//!
//! let db = Db::start(ConfigBuilder::default().temporary(true).build())?;
//! let storage = Storage::new(db)?;
//! let queue_handle = ServerConfig::new(storage).thread_count(8).start();
//! ```

use actix_threadpool::{run, BlockingError};
use background_jobs_core::{JobInfo, Stats, Storage};
use chrono::offset::Utc;
use sled_extensions::{bincode::Tree, cbor, Db, DbExt};

/// The error produced by sled storage calls
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error in the database
    #[error("Error in sled extensions, {0}")]
    Sled(sled_extensions::Error),

    /// Error executing db operation
    #[error("Blocking operation was canceled")]
    Canceled,
}

/// A simple alias for Result<T, Error>
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
/// The Sled-backed storage implementation
pub struct SledStorage {
    jobinfo: cbor::Tree<JobInfo>,
    running: Tree<u64>,
    running_inverse: Tree<u64>,
    queue: Tree<String>,
    stats: Tree<Stats>,
    lock: Tree<u64>,
    db: Db,
}

#[async_trait::async_trait]
impl Storage for SledStorage {
    type Error = Error;

    async fn generate_id(&self) -> Result<u64> {
        let this = self.clone();

        Ok(run(move || Ok(this.db.generate_id()?) as sled_extensions::Result<u64>).await?)
    }

    async fn save_job(&self, job: JobInfo) -> Result<()> {
        let this = self.clone();

        Ok(run(move || {
            this.jobinfo
                .insert(job_key(job.id()).as_bytes(), job)
                .map(|_| ())
        })
        .await?)
    }

    async fn fetch_job(&self, id: u64) -> Result<Option<JobInfo>> {
        let this = self.clone();

        Ok(run(move || this.jobinfo.get(job_key(id))).await?)
    }

    async fn fetch_job_from_queue(&self, queue: &str) -> Result<Option<JobInfo>> {
        let this = self.clone();
        let queue = queue.to_owned();

        Ok(run(move || {
            let queue_tree = this.queue.clone();
            let job_tree = this.jobinfo.clone();
            let queue2 = queue.clone();

            this.lock_queue(&queue2, move || {
                let now = Utc::now();

                let job = queue_tree
                    .iter()
                    .filter_map(|res| res.ok())
                    .filter_map(
                        |(id, in_queue)| {
                            if queue == in_queue {
                                Some(id)
                            } else {
                                None
                            }
                        },
                    )
                    .filter_map(|id| job_tree.get(id).ok())
                    .filter_map(|opt| opt)
                    .filter(|job| job.is_ready(now) && job.is_pending(now))
                    .next();

                if let Some(ref job) = job {
                    queue_tree.remove(&job_key(job.id()))?;
                }

                Ok(job) as sled_extensions::Result<Option<JobInfo>>
            })
        })
        .await?)
    }

    async fn queue_job(&self, queue: &str, id: u64) -> Result<()> {
        let this = self.clone();
        let queue = queue.to_owned();

        Ok(run(move || {
            if let Some(runner_id) = this.running_inverse.remove(&job_key(id))? {
                this.running.remove(&runner_key(runner_id))?;
            }

            this.queue.insert(job_key(id).as_bytes(), queue).map(|_| ())
        })
        .await?)
    }

    async fn run_job(&self, id: u64, runner_id: u64) -> Result<()> {
        let this = self.clone();

        Ok(run(move || {
            this.queue.remove(job_key(id))?;
            this.running.insert(runner_key(runner_id).as_bytes(), id)?;
            this.running_inverse
                .insert(job_key(id).as_bytes(), runner_id)?;

            Ok(()) as Result<()>
        })
        .await?)
    }

    async fn delete_job(&self, id: u64) -> Result<()> {
        let this = self.clone();

        Ok(run(move || {
            this.jobinfo.remove(&job_key(id))?;
            this.queue.remove(&job_key(id))?;

            if let Some(runner_id) = this.running_inverse.remove(&job_key(id))? {
                this.running.remove(&runner_key(runner_id))?;
            }

            Ok(()) as Result<()>
        })
        .await?)
    }

    async fn get_stats(&self) -> Result<Stats> {
        let this = self.clone();

        Ok(
            run(move || Ok(this.stats.get("stats")?.unwrap_or(Stats::default())) as Result<Stats>)
                .await?,
        )
    }

    async fn update_stats<F>(&self, f: F) -> Result<()>
    where
        F: Fn(Stats) -> Stats + Send + 'static,
    {
        let this = self.clone();

        Ok(run(move || {
            this.stats.fetch_and_update("stats", move |opt| {
                let stats = match opt {
                    Some(stats) => stats,
                    None => Stats::default(),
                };

                Some((f)(stats))
            })?;

            Ok(()) as Result<()>
        })
        .await?)
    }
}

impl SledStorage {
    /// Create a new Storage struct
    pub fn new(db: Db) -> Result<Self> {
        Ok(SledStorage {
            jobinfo: db.open_cbor_tree("background-jobs-jobinfo")?,
            running: db.open_bincode_tree("background-jobs-running")?,
            running_inverse: db.open_bincode_tree("background-jobs-running-inverse")?,
            queue: db.open_bincode_tree("background-jobs-queue")?,
            stats: db.open_bincode_tree("background-jobs-stats")?,
            lock: db.open_bincode_tree("background-jobs-lock")?,
            db,
        })
    }

    fn lock_queue<T, F>(&self, queue: &str, f: F) -> sled_extensions::Result<T>
    where
        F: Fn() -> sled_extensions::Result<T>,
    {
        let id = self.db.generate_id()?;

        let mut prev;
        while {
            prev = self.lock.fetch_and_update(queue, move |opt| match opt {
                Some(_) => opt,
                None => Some(id),
            })?;

            prev.is_some()
        } {}

        let res = (f)();

        self.lock.fetch_and_update(queue, |_| None)?;

        res
    }
}

fn job_key(id: u64) -> String {
    format!("job-{}", id)
}

fn runner_key(runner_id: u64) -> String {
    format!("runner-{}", runner_id)
}

impl<T> From<BlockingError<T>> for Error
where
    Error: From<T>,
    T: std::fmt::Debug,
{
    fn from(e: BlockingError<T>) -> Self {
        match e {
            BlockingError::Error(e) => e.into(),
            BlockingError::Canceled => Error::Canceled,
        }
    }
}

impl From<sled_extensions::Error> for Error {
    fn from(e: sled_extensions::Error) -> Self {
        Error::Sled(e)
    }
}
