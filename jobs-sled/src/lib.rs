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

use actix_rt::task::JoinError;
use background_jobs_core::{JobInfo, JobResult, NewJobInfo, ReturnJobInfo};
use sled::{Db, Tree};
use std::{
    collections::HashMap,
    ops::Bound,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;
use uuid::{NoContext, Timestamp, Uuid};

/// The error produced by sled storage calls
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error in the database
    #[error("Error in sled extensions")]
    Sled(#[from] sled::Error),

    /// Error in cbor
    #[error("Error in cbor")]
    Cbor(#[from] serde_cbor::Error),

    /// Conflict while updating record
    #[error("Conflict while updating record")]
    Conflict,

    /// Missing record
    #[error("Missing record")]
    Missing,

    /// Error executing db operation
    #[error("Blocking operation was canceled")]
    Canceled,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct JobMeta {
    id: Uuid,
    state: Option<JobState>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct JobState {
    runner_id: Uuid,
    heartbeat: time::OffsetDateTime,
}

struct JobKey {
    queue: String,
    next_queue_id: Uuid,
}

fn encode_key(key: &JobKey) -> Vec<u8> {
    let mut v = Vec::with_capacity(key.queue.len() + 17);
    v.extend_from_slice(key.queue.as_bytes());
    v.push(b',');
    v.extend_from_slice(key.next_queue_id.as_bytes());
    v
}

/// A simple alias for Result<T, Error>
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
/// The Sled-backed storage implementation
pub struct Storage {
    jobs: Tree,
    queue_jobs: Tree,
    queues: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    _db: Db,
}

#[async_trait::async_trait]
impl background_jobs_core::Storage for Storage {
    type Error = Error;

    async fn info(&self, job_id: Uuid) -> Result<Option<JobInfo>> {
        self.get(job_id)
    }

    async fn push(&self, job: NewJobInfo) -> Result<Uuid> {
        self.insert(job.build())
    }

    async fn pop(&self, queue: &str, runner_id: Uuid) -> Result<JobInfo> {
        loop {
            let notifier = self.notifier(queue.to_string());

            if let Some(job) = self.try_pop(queue.to_string(), runner_id)? {
                return Ok(job);
            }

            let duration = self
                .next_duration(queue.to_string())
                .unwrap_or(Duration::from_secs(5));

            match tokio::time::timeout(duration, notifier.notified()).await {
                Ok(()) => {
                    // notified
                }
                Err(_) => {
                    // timeout
                }
            }
        }
    }

    async fn heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<()> {
        self.set_heartbeat(job_id, runner_id)
    }

    async fn complete(&self, ReturnJobInfo { id, result }: ReturnJobInfo) -> Result<bool> {
        let mut job = if let Some(job) = self.remove_job(id)? {
            job
        } else {
            return Ok(true);
        };

        match result {
            // successful jobs are removed
            JobResult::Success => Ok(true),
            // Unregistered or Unexecuted jobs are restored as-is
            JobResult::Unexecuted | JobResult::Unregistered => {
                self.insert(job)?;
                Ok(false)
            }
            // retryable failed jobs are restored
            JobResult::Failure if job.prepare_retry() => {
                self.insert(job)?;
                Ok(false)
            }
            // dead jobs are removed
            JobResult::Failure => Ok(true),
        }
    }
}

impl Storage {
    /// Create a new Storage struct
    pub fn new(db: Db) -> Result<Self> {
        Ok(Storage {
            jobs: db.open_tree("background-jobs-jobs")?,
            queue_jobs: db.open_tree("background-jobs-queue-jobs")?,
            queues: Arc::new(Mutex::new(HashMap::new())),
            _db: db,
        })
    }

    fn get(&self, job_id: Uuid) -> Result<Option<JobInfo>> {
        if let Some(ivec) = self.jobs.get(job_id.as_bytes())? {
            let job_info = serde_cbor::from_slice(&ivec)?;

            Ok(Some(job_info))
        } else {
            Ok(None)
        }
    }

    fn notifier(&self, queue: String) -> Arc<Notify> {
        self.queues
            .lock()
            .unwrap()
            .entry(queue)
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone()
    }

    fn notify(&self, queue: String) {
        self.queues
            .lock()
            .unwrap()
            .entry(queue)
            .or_insert_with(|| Arc::new(Notify::new()))
            .notify_one();
    }

    fn try_pop(&self, queue: String, runner_id: Uuid) -> Result<Option<JobInfo>> {
        let lower_bound = encode_key(&JobKey {
            queue: queue.clone(),
            next_queue_id: Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0)),
        });
        let upper_bound = encode_key(&JobKey {
            queue: queue.clone(),
            next_queue_id: Uuid::now_v7(),
        });
        let now = time::OffsetDateTime::now_utc();

        for res in self
            .queue_jobs
            .range((Bound::Excluded(lower_bound), Bound::Included(upper_bound)))
        {
            let (key, ivec) = res?;

            if let Ok(JobMeta { id, state }) = serde_cbor::from_slice(&ivec) {
                if state.is_none()
                    || state.is_some_and(|JobState { heartbeat, .. }| {
                        heartbeat + time::Duration::seconds(30) < now
                    })
                {
                    let new_bytes = serde_cbor::to_vec(&JobMeta {
                        id,
                        state: Some(JobState {
                            runner_id,
                            heartbeat: now,
                        }),
                    })?;

                    match self
                        .queue_jobs
                        .compare_and_swap(key, Some(ivec), Some(new_bytes))?
                    {
                        Ok(()) => {
                            // success
                            if let Some(job) = self.jobs.get(id.as_bytes())? {
                                return Ok(Some(serde_cbor::from_slice(&job)?));
                            }
                        }
                        Err(_) => {
                            // conflict
                        }
                    }

                    break;
                }
            }
        }

        Ok(None)
    }

    fn set_heartbeat(&self, job_id: Uuid, runner_id: Uuid) -> Result<()> {
        let queue = if let Some(job) = self.jobs.get(job_id.as_bytes())? {
            let job: JobInfo = serde_cbor::from_slice(&job)?;
            job.queue
        } else {
            return Ok(());
        };

        let lower_bound = encode_key(&JobKey {
            queue: queue.clone(),
            next_queue_id: Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0)),
        });
        let upper_bound = encode_key(&JobKey {
            queue,
            next_queue_id: Uuid::now_v7(),
        });

        for res in self
            .queue_jobs
            .range((Bound::Excluded(lower_bound), Bound::Included(upper_bound)))
        {
            let (key, ivec) = res?;

            if let Ok(JobMeta { id, .. }) = serde_cbor::from_slice(&ivec) {
                if id == job_id {
                    let new_bytes = serde_cbor::to_vec(&JobMeta {
                        id,
                        state: Some(JobState {
                            runner_id,
                            heartbeat: time::OffsetDateTime::now_utc(),
                        }),
                    })?;

                    match self
                        .queue_jobs
                        .compare_and_swap(key, Some(ivec), Some(new_bytes))?
                    {
                        Ok(()) => {
                            // success
                            return Ok(());
                        }
                        Err(_) => {
                            // conflict
                            return Err(Error::Conflict);
                        }
                    }
                }
            }
        }

        Err(Error::Missing)
    }

    fn remove_job(&self, job_id: Uuid) -> Result<Option<JobInfo>> {
        let job: JobInfo = if let Some(job) = self.jobs.remove(job_id.as_bytes())? {
            serde_cbor::from_slice(&job)?
        } else {
            return Ok(None);
        };

        let lower_bound = encode_key(&JobKey {
            queue: job.queue.clone(),
            next_queue_id: Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0)),
        });
        let upper_bound = encode_key(&JobKey {
            queue: job.queue.clone(),
            next_queue_id: Uuid::now_v7(),
        });

        for res in self
            .queue_jobs
            .range((Bound::Excluded(lower_bound), Bound::Included(upper_bound)))
        {
            let (key, ivec) = res?;

            if let Ok(JobMeta { id, .. }) = serde_cbor::from_slice(&ivec) {
                if id == job_id {
                    self.queue_jobs.remove(key)?;
                    return Ok(Some(job));
                }
            }
        }

        Err(Error::Missing)
    }

    fn next_duration(&self, pop_queue: String) -> Option<Duration> {
        let lower_bound = encode_key(&JobKey {
            queue: pop_queue.clone(),
            next_queue_id: Uuid::new_v7(Timestamp::from_unix(NoContext, 0, 0)),
        });

        let now = time::OffsetDateTime::now_utc();

        self.queue_jobs
            .range((Bound::Excluded(lower_bound), Bound::Unbounded))
            .values()
            .filter_map(|res| res.ok())
            .filter_map(|ivec| serde_cbor::from_slice(&ivec).ok())
            .filter(|JobMeta { state, .. }| state.is_none())
            .filter_map(|JobMeta { id, .. }| self.jobs.get(id.as_bytes()).ok()?)
            .filter_map(|ivec| serde_cbor::from_slice::<JobInfo>(&ivec).ok())
            .take_while(|JobInfo { queue, .. }| queue.as_str() == pop_queue.as_str())
            .map(|JobInfo { next_queue, .. }| {
                if next_queue > now {
                    next_queue - now
                } else {
                    time::Duration::seconds(0)
                }
            })
            .find_map(|d| d.try_into().ok())
    }

    fn insert(&self, job: JobInfo) -> Result<Uuid> {
        let id = job.id;
        let queue = job.queue.clone();
        let next_queue_id = job.next_queue_id();

        let job_bytes = serde_cbor::to_vec(&job)?;

        self.jobs.insert(id.as_bytes(), job_bytes)?;

        let key_bytes = encode_key(&JobKey {
            queue: queue.clone(),
            next_queue_id,
        });

        let job_meta_bytes = serde_cbor::to_vec(&JobMeta { id, state: None })?;

        self.queue_jobs.insert(key_bytes, job_meta_bytes)?;

        self.notify(queue);

        Ok(id)
    }
}

impl From<JoinError> for Error {
    fn from(_: JoinError) -> Self {
        Error::Canceled
    }
}
