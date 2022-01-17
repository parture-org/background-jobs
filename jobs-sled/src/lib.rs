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

use actix_rt::task::{spawn_blocking, JoinError};
use background_jobs_core::{JobInfo, Stats};
use sled::{Db, Tree};
use std::time::SystemTime;
use uuid::Uuid;

/// The error produced by sled storage calls
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error in the database
    #[error("Error in sled extensions, {0}")]
    Sled(#[from] sled::Error),

    /// Error storing or retrieving job info
    #[error("Error transforming job info, {0}")]
    Cbor(#[from] serde_cbor::Error),

    /// Error executing db operation
    #[error("Blocking operation was canceled")]
    Canceled,
}

/// A simple alias for Result<T, Error>
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
/// The Sled-backed storage implementation
pub struct Storage {
    id: Tree,
    jobinfo: Tree,
    running: Tree,
    running_inverse: Tree,
    queue: Tree,
    stats: Tree,
    db: Db,
}

#[async_trait::async_trait]
impl background_jobs_core::Storage for Storage {
    type Error = Error;

    async fn generate_id(&self) -> Result<Uuid> {
        let this = self.clone();

        Ok(spawn_blocking(move || {
            let mut uuid;
            while {
                uuid = Uuid::new_v4();

                this.id
                    .compare_and_swap(
                        uuid.as_bytes(),
                        None as Option<&[u8]>,
                        Some(uuid.as_bytes()),
                    )?
                    .is_err()
            } {}

            Ok(uuid) as Result<Uuid>
        })
        .await??)
    }

    async fn save_job(&self, job: JobInfo) -> Result<()> {
        let this = self.clone();

        Ok(spawn_blocking(move || {
            let job_vec = serde_cbor::to_vec(&job)?;

            this.jobinfo.insert(job.id().as_bytes(), job_vec)?;

            Ok(()) as Result<_>
        })
        .await??)
    }

    async fn fetch_job(&self, id: Uuid) -> Result<Option<JobInfo>> {
        let this = self.clone();

        Ok(spawn_blocking(move || {
            if let Some(job_ivec) = this.jobinfo.get(id.as_bytes())? {
                let job: JobInfo = serde_cbor::from_slice(&job_ivec)?;
                Ok(Some(job)) as Result<_>
            } else {
                Ok(None)
            }
        })
        .await??)
    }

    async fn fetch_job_from_queue(&self, queue: &str) -> Result<Option<JobInfo>> {
        let this = self.clone();
        let queue = queue.to_owned();

        Ok(spawn_blocking(move || {
            let mut job;

            let now = SystemTime::now();

            while {
                let job_opt = this
                    .queue
                    .iter()
                    .filter_map(|res| res.ok())
                    .filter_map(|(id, in_queue)| {
                        if queue.as_bytes() == in_queue.as_ref() {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .filter_map(|id| this.jobinfo.get(id).ok())
                    .flatten()
                    .filter_map(|ivec| serde_cbor::from_slice(&ivec).ok())
                    .find(|job: &JobInfo| job.is_ready(now) && job.is_pending(now));

                job = if let Some(job) = job_opt {
                    job
                } else {
                    return Ok(None);
                };

                this.queue.remove(job.id().as_bytes())?.is_none()
            } {}

            Ok(Some(job)) as Result<Option<JobInfo>>
        })
        .await??)
    }

    async fn queue_job(&self, queue: &str, id: Uuid) -> Result<()> {
        let this = self.clone();
        let queue = queue.to_owned();

        Ok(spawn_blocking(move || {
            if let Some(runner_id) = this.running_inverse.remove(id.as_bytes())? {
                this.running.remove(runner_id)?;
            }

            this.queue.insert(id.as_bytes(), queue.as_bytes())?;

            Ok(()) as Result<_>
        })
        .await??)
    }

    async fn run_job(&self, id: Uuid, runner_id: Uuid) -> Result<()> {
        let this = self.clone();

        Ok(spawn_blocking(move || {
            this.queue.remove(id.as_bytes())?;
            this.running.insert(runner_id.as_bytes(), id.as_bytes())?;
            this.running_inverse
                .insert(id.as_bytes(), runner_id.as_bytes())?;

            Ok(()) as Result<()>
        })
        .await??)
    }

    async fn delete_job(&self, id: Uuid) -> Result<()> {
        let this = self.clone();

        Ok(spawn_blocking(move || {
            this.jobinfo.remove(id.as_bytes())?;
            this.queue.remove(id.as_bytes())?;
            this.id.remove(id.as_bytes())?;

            if let Some(runner_id) = this.running_inverse.remove(id.as_bytes())? {
                this.running.remove(runner_id)?;
            }

            Ok(()) as Result<()>
        })
        .await??)
    }

    async fn get_stats(&self) -> Result<Stats> {
        let this = self.clone();

        let stats = spawn_blocking(move || {
            let stats = if let Some(stats_ivec) = this.stats.get("stats")? {
                bincode::deserialize(&stats_ivec).unwrap_or_default()
            } else {
                Stats::default()
            };

            Ok(stats) as Result<Stats>
        })
        .await??;

        Ok(stats)
    }

    async fn update_stats<F>(&self, f: F) -> Result<()>
    where
        F: Fn(Stats) -> Stats + Send + 'static,
    {
        let this = self.clone();

        Ok(spawn_blocking(move || {
            this.stats.fetch_and_update("stats", move |opt| {
                let stats = if let Some(stats_ivec) = opt {
                    bincode::deserialize(stats_ivec).unwrap_or_default()
                } else {
                    Stats::default()
                };

                let new_stats = (f)(stats);

                let stats_vec = bincode::serialize(&new_stats).ok()?;
                Some(stats_vec)
            })?;

            Ok(()) as Result<()>
        })
        .await??)
    }
}

impl Storage {
    /// Create a new Storage struct
    pub fn new(db: Db) -> Result<Self> {
        Ok(Storage {
            id: db.open_tree("background-jobs-id")?,
            jobinfo: db.open_tree("background-jobs-jobinfo")?,
            running: db.open_tree("background-jobs-running")?,
            running_inverse: db.open_tree("background-jobs-running-inverse")?,
            queue: db.open_tree("background-jobs-queue")?,
            stats: db.open_tree("background-jobs-stats")?,
            db,
        })
    }
}

impl From<JoinError> for Error {
    fn from(_: JoinError) -> Self {
        Error::Canceled
    }
}
