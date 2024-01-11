mod embedded;
mod future;
mod schema;

use future::{Metrics as _, Timeout as _};

use std::{
    collections::{BTreeSet, VecDeque},
    error::Error,
    future::Future,
    ops::Deref,
    sync::Arc,
    time::Duration,
};

use background_jobs_core::{Backoff, JobInfo, JobResult, MaxRetries, NewJobInfo, ReturnJobInfo};
use dashmap::DashMap;
use diesel::{
    data_types::PgInterval,
    dsl::IntervalDsl,
    prelude::*,
    sql_types::{Interval, Timestamp},
};
use diesel_async::{
    pooled_connection::{
        deadpool::{BuildError, Hook, Pool, PoolError},
        AsyncDieselConnectionManager, ManagerConfig,
    },
    AsyncPgConnection, RunQueryDsl,
};
use futures_core::future::BoxFuture;
use serde_json::Value;
use time::PrimitiveDateTime;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_postgres::{tls::NoTlsStream, AsyncMessage, Connection, NoTls, Notification, Socket};
use tracing::Instrument;
use url::Url;
use uuid::Uuid;

type ConfigFn =
    Box<dyn Fn(&str) -> BoxFuture<'_, ConnectionResult<AsyncPgConnection>> + Send + Sync + 'static>;

#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
    #[allow(dead_code)]
    drop_handle: Arc<DropHandle<()>>,
}

struct Inner {
    pool: Pool<AsyncPgConnection>,
    queue_notifications: DashMap<String, Arc<Notify>>,
}

struct DropHandle<T> {
    handle: JoinHandle<T>,
}

fn spawn<F: Future + Send + 'static>(name: &str, future: F) -> DropHandle<F::Output>
where
    F::Output: Send,
{
    DropHandle {
        handle: tokio::task::Builder::new()
            .name(name)
            .spawn(future)
            .expect("Spawned task"),
    }
}

#[derive(Debug)]
pub enum ConnectPostgresError {
    /// Error connecting to postgres to run migrations
    ConnectForMigration(tokio_postgres::Error),

    /// Error running migrations
    Migration(refinery::Error),

    /// Error constructing the connection pool
    BuildPool(BuildError),
}

#[derive(Debug)]
pub enum PostgresError {
    Pool(PoolError),

    Diesel(diesel::result::Error),

    DbTimeout,
}

struct JobNotifierState<'a> {
    inner: &'a Inner,
    capacity: usize,
    jobs: BTreeSet<Uuid>,
    jobs_ordered: VecDeque<Uuid>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, diesel_derive_enum::DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::JobStatus"]
enum JobStatus {
    New,
    Running,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, diesel_derive_enum::DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::BackoffStrategy"]
enum BackoffStrategy {
    Linear,
    Exponential,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, diesel_derive_enum::DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::RetryStrategy"]
enum RetryStrategy {
    Infinite,
    Count,
}

#[derive(diesel::Insertable, diesel::Queryable, diesel::Selectable)]
#[diesel(table_name = crate::schema::job_queue)]
struct PostgresJob {
    id: Uuid,
    name: String,
    queue: String,
    args: Value,
    retry_count: i32,
    max_retries: i32,
    retry: RetryStrategy,
    backoff_multiplier: i32,
    backoff: BackoffStrategy,
    next_queue: PrimitiveDateTime,
    heartbeat_interval: PgInterval,
}

impl From<JobInfo> for PostgresJob {
    fn from(value: JobInfo) -> Self {
        let JobInfo {
            id,
            name,
            queue,
            args,
            retry_count,
            max_retries,
            backoff_strategy,
            next_queue,
            heartbeat_interval,
        } = value;

        let next_queue = next_queue.to_offset(time::UtcOffset::UTC);

        PostgresJob {
            id,
            name,
            queue,
            args,
            retry_count: retry_count as _,
            max_retries: match max_retries {
                MaxRetries::Count(count) => count as _,
                MaxRetries::Infinite => 0,
            },
            retry: match max_retries {
                MaxRetries::Infinite => RetryStrategy::Infinite,
                MaxRetries::Count(_) => RetryStrategy::Count,
            },
            backoff_multiplier: match backoff_strategy {
                Backoff::Linear(multiplier) => multiplier as _,
                Backoff::Exponential(multiplier) => multiplier as _,
            },
            backoff: match backoff_strategy {
                Backoff::Linear(_) => BackoffStrategy::Linear,
                Backoff::Exponential(_) => BackoffStrategy::Exponential,
            },
            next_queue: PrimitiveDateTime::new(next_queue.date(), next_queue.time()),
            heartbeat_interval: (heartbeat_interval as i32).milliseconds(),
        }
    }
}

impl From<PostgresJob> for JobInfo {
    fn from(value: PostgresJob) -> Self {
        let PostgresJob {
            id,
            name,
            queue,
            args,
            retry_count,
            max_retries,
            retry,
            backoff_multiplier,
            backoff,
            next_queue,
            heartbeat_interval,
        } = value;

        JobInfo {
            id,
            name,
            queue,
            args,
            retry_count: retry_count as _,
            max_retries: match retry {
                RetryStrategy::Count => MaxRetries::Count(max_retries as _),
                RetryStrategy::Infinite => MaxRetries::Infinite,
            },
            backoff_strategy: match backoff {
                BackoffStrategy::Linear => Backoff::Linear(backoff_multiplier as _),
                BackoffStrategy::Exponential => Backoff::Exponential(backoff_multiplier as _),
            },
            next_queue: next_queue.assume_utc(),
            heartbeat_interval: (heartbeat_interval.microseconds / 1_000) as _,
        }
    }
}

#[async_trait::async_trait]
impl background_jobs_core::Storage for Storage {
    type Error = PostgresError;

    async fn info(
        &self,
        job_id: Uuid,
    ) -> Result<Option<background_jobs_core::JobInfo>, Self::Error> {
        let mut conn = self.inner.pool.get().await.map_err(PostgresError::Pool)?;

        let opt = {
            use schema::job_queue::dsl::*;

            job_queue
                .select(PostgresJob::as_select())
                .filter(id.eq(job_id))
                .get_result(&mut conn)
                .metrics("background-jobs.postgres.info")
                .timeout(Duration::from_secs(5))
                .await
                .map_err(|_| PostgresError::DbTimeout)?
                .optional()
                .map_err(PostgresError::Diesel)?
        };

        if let Some(postgres_job) = opt {
            Ok(Some(postgres_job.into()))
        } else {
            Ok(None)
        }
    }

    async fn push(&self, job: NewJobInfo) -> Result<Uuid, Self::Error> {
        self.insert(job.build()).await
    }

    async fn pop(&self, in_queue: &str, in_runner_id: Uuid) -> Result<JobInfo, Self::Error> {
        loop {
            tracing::trace!("pop: looping");

            let mut conn = self.inner.pool.get().await.map_err(PostgresError::Pool)?;

            let notifier: Arc<Notify> = self
                .inner
                .queue_notifications
                .entry(String::from(in_queue))
                .or_insert_with(|| Arc::new(Notify::const_new()))
                .clone();

            diesel::sql_query("LISTEN queue_status_channel;")
                .execute(&mut conn)
                .metrics("background-jobs.postgres.listen")
                .timeout(Duration::from_secs(5))
                .await
                .map_err(|_| PostgresError::DbTimeout)?
                .map_err(PostgresError::Diesel)?;

            let count = {
                use schema::job_queue::dsl::*;

                diesel::update(job_queue)
                    .filter(heartbeat.is_not_null().and(heartbeat.assume_not_null().le(
                        // not allowed to multiply heartbeat_interval. thanks diesel
                        diesel::dsl::sql::<Timestamp>("NOW() - heartbeat_interval * 5"),
                    )))
                    .set((
                        heartbeat.eq(Option::<PrimitiveDateTime>::None),
                        status.eq(JobStatus::New),
                        runner_id.eq(Option::<Uuid>::None),
                    ))
                    .execute(&mut conn)
                    .metrics("background-jobs.postgres.requeue")
                    .await
                    .map_err(PostgresError::Diesel)?
            };

            if count > 0 {
                tracing::info!("Reset {count} jobs");
            }

            let id_query = {
                use schema::job_queue::dsl::*;

                let queue_alias = diesel::alias!(schema::job_queue as queue_alias);

                queue_alias
                    .select(queue_alias.field(id))
                    .filter(
                        queue_alias
                            .field(status)
                            .eq(JobStatus::New)
                            .and(queue_alias.field(queue).eq(in_queue))
                            .and(queue_alias.field(next_queue).le(diesel::dsl::now)),
                    )
                    .order(queue_alias.field(next_queue))
                    .for_update()
                    .skip_locked()
                    .single_value()
            };

            let opt = {
                use schema::job_queue::dsl::*;

                diesel::update(job_queue)
                    .filter(id.nullable().eq(id_query))
                    .filter(status.eq(JobStatus::New))
                    .set((
                        heartbeat.eq(diesel::dsl::now),
                        status.eq(JobStatus::Running),
                        runner_id.eq(in_runner_id),
                    ))
                    .returning(PostgresJob::as_returning())
                    .get_result(&mut conn)
                    .metrics("background-jobs.postgres.claim")
                    .timeout(Duration::from_secs(5))
                    .await
                    .map_err(|_| PostgresError::DbTimeout)?
                    .optional()
                    .map_err(PostgresError::Diesel)?
            };

            if let Some(postgres_job) = opt {
                return Ok(postgres_job.into());
            }

            let sleep_duration = {
                use schema::job_queue::dsl::*;

                job_queue
                    .filter(queue.eq(in_queue).and(status.eq(JobStatus::New)))
                    .select(diesel::dsl::sql::<Interval>("NOW() - next_queue"))
                    .get_result::<PgInterval>(&mut conn)
                    .metrics("background-jobs.postgres.next-queue")
                    .timeout(Duration::from_secs(5))
                    .await
                    .map_err(|_| PostgresError::DbTimeout)?
                    .optional()
                    .map_err(PostgresError::Diesel)?
                    .map(|interval| {
                        if interval.microseconds < 0 {
                            Duration::from_micros(interval.microseconds.abs_diff(0))
                        } else {
                            Duration::from_secs(0)
                        }
                    })
                    .unwrap_or(Duration::from_secs(5))
            };

            drop(conn);
            if notifier.notified().timeout(sleep_duration).await.is_ok() {
                tracing::debug!("Notified");
            } else {
                tracing::debug!("Timed out");
            }
        }
    }

    async fn heartbeat(&self, job_id: Uuid, in_runner_id: Uuid) -> Result<(), Self::Error> {
        let mut conn = self.inner.pool.get().await.map_err(PostgresError::Pool)?;

        {
            use schema::job_queue::dsl::*;

            diesel::update(job_queue)
                .filter(id.eq(job_id))
                .set((heartbeat.eq(diesel::dsl::now), runner_id.eq(in_runner_id)))
                .execute(&mut conn)
                .metrics("background-jobs.postgres.heartbeat")
                .timeout(Duration::from_secs(5))
                .await
                .map_err(|_| PostgresError::DbTimeout)?
                .map_err(PostgresError::Diesel)?;
        }

        Ok(())
    }

    async fn complete(&self, return_job_info: ReturnJobInfo) -> Result<bool, Self::Error> {
        let mut conn = self.inner.pool.get().await.map_err(PostgresError::Pool)?;

        let job = {
            use schema::job_queue::dsl::*;

            diesel::delete(job_queue)
                .filter(id.eq(return_job_info.id))
                .returning(PostgresJob::as_returning())
                .get_result(&mut conn)
                .metrics("background-jobs.postgres.complete")
                .timeout(Duration::from_secs(5))
                .await
                .map_err(|_| PostgresError::DbTimeout)?
                .optional()
                .map_err(PostgresError::Diesel)?
        };

        let mut job: JobInfo = if let Some(job) = job {
            job.into()
        } else {
            return Ok(true);
        };

        match return_job_info.result {
            // successful jobs are removed
            JobResult::Success => Ok(true),
            // Unregistered or Unexecuted jobs are restored as-is
            JobResult::Unexecuted | JobResult::Unregistered => {
                self.insert(job).await?;

                Ok(false)
            }
            // retryable failed jobs are restored
            JobResult::Failure if job.prepare_retry() => {
                self.insert(job).await?;

                Ok(false)
            }
            // dead jobs are removed
            JobResult::Failure => Ok(true),
        }
    }
}

impl Storage {
    pub async fn connect(postgres_url: Url) -> Result<Self, ConnectPostgresError> {
        let (mut client, conn) = tokio_postgres::connect(postgres_url.as_str(), NoTls)
            .await
            .map_err(ConnectPostgresError::ConnectForMigration)?;

        let handle = spawn("postgres-migrations", conn);

        embedded::migrations::runner()
            .run_async(&mut client)
            .await
            .map_err(ConnectPostgresError::Migration)?;

        handle.abort();
        let _ = handle.await;

        let parallelism = std::thread::available_parallelism()
            .map(|u| u.into())
            .unwrap_or(1_usize);

        let (tx, rx) = flume::bounded(10);

        let mut config = ManagerConfig::default();
        config.custom_setup = build_handler(tx);

        let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(
            postgres_url,
            config,
        );

        let pool = Pool::builder(mgr)
            .runtime(deadpool::Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_secs(10)))
            .create_timeout(Some(Duration::from_secs(2)))
            .recycle_timeout(Some(Duration::from_secs(2)))
            .post_create(Hook::sync_fn(|_, _| {
                metrics::counter!("background-jobs.postgres.pool.connection.create").increment(1);
                Ok(())
            }))
            .post_recycle(Hook::sync_fn(|_, _| {
                metrics::counter!("background-jobs.postgres.pool.connection.recycle").increment(1);
                Ok(())
            }))
            .max_size(parallelism * 8)
            .build()
            .map_err(ConnectPostgresError::BuildPool)?;

        let inner = Arc::new(Inner {
            pool,
            queue_notifications: DashMap::new(),
        });

        let handle = spawn(
            "postgres-delegate-notifications",
            delegate_notifications(rx, inner.clone(), parallelism * 8),
        );

        let drop_handle = Arc::new(handle);

        Ok(Storage { inner, drop_handle })
    }

    async fn insert(&self, job_info: JobInfo) -> Result<Uuid, PostgresError> {
        let postgres_job: PostgresJob = job_info.into();
        let id = postgres_job.id;

        let mut conn = self.inner.pool.get().await.map_err(PostgresError::Pool)?;

        {
            use schema::job_queue::dsl::*;

            postgres_job
                .insert_into(job_queue)
                .execute(&mut conn)
                .metrics("background-jobs.postgres.insert")
                .timeout(Duration::from_secs(5))
                .await
                .map_err(|_| PostgresError::DbTimeout)?
                .map_err(PostgresError::Diesel)?;
        }

        Ok(id)
    }
}

impl<'a> JobNotifierState<'a> {
    fn handle(&mut self, payload: &str) {
        let Some((job_id, queue_name)) = payload.split_once(' ') else {
            tracing::warn!("Invalid queue payload {payload}");
            return;
        };

        let Ok(job_id) = job_id.parse::<Uuid>() else {
            tracing::warn!("Invalid job ID {job_id}");
            return;
        };

        if !self.jobs.insert(job_id) {
            // duplicate job
            return;
        }

        self.jobs_ordered.push_back(job_id);

        if self.jobs_ordered.len() > self.capacity {
            if let Some(job_id) = self.jobs_ordered.pop_front() {
                self.jobs.remove(&job_id);
            }
        }

        self.inner
            .queue_notifications
            .entry(queue_name.to_string())
            .or_insert_with(|| Arc::new(Notify::const_new()))
            .notify_one();

        metrics::counter!("pict-rs.postgres.job-notifier.notified", "queue" => queue_name.to_string()).increment(1);
    }
}

async fn delegate_notifications(
    receiver: flume::Receiver<Notification>,
    inner: Arc<Inner>,
    capacity: usize,
) {
    let mut job_notifier_state = JobNotifierState {
        inner: &inner,
        capacity,
        jobs: BTreeSet::new(),
        jobs_ordered: VecDeque::new(),
    };

    while let Ok(notification) = receiver.recv_async().await {
        tracing::trace!("delegate_notifications: looping");
        metrics::counter!("pict-rs.postgres.notification").increment(1);

        match notification.channel() {
            "queue_status_channel" => {
                // new job inserted for queue
                job_notifier_state.handle(notification.payload());
            }
            channel => {
                tracing::info!(
                    "Unhandled postgres notification: {channel}: {}",
                    notification.payload()
                );
            }
        }
    }

    tracing::warn!("Notification delegator shutting down");
}

fn build_handler(sender: flume::Sender<Notification>) -> ConfigFn {
    Box::new(
        move |config: &str| -> BoxFuture<'_, ConnectionResult<AsyncPgConnection>> {
            let sender = sender.clone();

            let connect_span = tracing::trace_span!(parent: None, "connect future");

            Box::pin(
                async move {
                    let (client, conn) =
                        tokio_postgres::connect(config, tokio_postgres::tls::NoTls)
                            .await
                            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

                    // not very cash money (structured concurrency) of me
                    spawn_db_notification_task(sender, conn)
                        .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

                    AsyncPgConnection::try_from(client).await
                }
                .instrument(connect_span),
            )
        },
    )
}

fn spawn_db_notification_task(
    sender: flume::Sender<Notification>,
    mut conn: Connection<Socket, NoTlsStream>,
) -> std::io::Result<()> {
    tokio::task::Builder::new().name("postgres-notifications").spawn(async move {
        while let Some(res) = std::future::poll_fn(|cx| conn.poll_message(cx)).await {
            tracing::trace!("db_notification_task: looping");

            match res {
                Err(e) => {
                    tracing::error!("Database Connection {e:?}");
                    return;
                }
                Ok(AsyncMessage::Notice(e)) => {
                    tracing::warn!("Database Notice {e:?}");
                }
                Ok(AsyncMessage::Notification(notification)) => {
                    if sender.send_async(notification).await.is_err() {
                        tracing::warn!("Missed notification. Are we shutting down?");
                    }
                }
                Ok(_) => {
                    tracing::warn!("Unhandled AsyncMessage!!! Please contact the developer of this application");
                }
            }
        }
    })?;

    Ok(())
}

impl<T> Future for DropHandle<T> {
    type Output = <JoinHandle<T> as Future>::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.get_mut().handle).poll(cx)
    }
}

impl<T> Drop for DropHandle<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl<T> Deref for DropHandle<T> {
    type Target = JoinHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage").finish()
    }
}

impl From<refinery::Error> for ConnectPostgresError {
    fn from(value: refinery::Error) -> Self {
        Self::Migration(value)
    }
}

impl From<tokio_postgres::Error> for ConnectPostgresError {
    fn from(value: tokio_postgres::Error) -> Self {
        Self::ConnectForMigration(value)
    }
}

impl From<BuildError> for ConnectPostgresError {
    fn from(value: BuildError) -> Self {
        Self::BuildPool(value)
    }
}

impl std::fmt::Display for ConnectPostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BuildPool(_) => write!(f, "Failed to build postgres connection pool"),
            Self::ConnectForMigration(_) => {
                write!(f, "Failed to connect to postgres for migrations")
            }
            Self::Migration(_) => write!(f, "Failed to run migrations"),
        }
    }
}

impl std::error::Error for ConnectPostgresError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BuildPool(e) => Some(e),
            Self::ConnectForMigration(e) => Some(e),
            Self::Migration(e) => Some(e),
        }
    }
}

impl std::fmt::Display for PostgresError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pool(_) => write!(f, "Error in db pool"),
            Self::Diesel(_) => write!(f, "Error in database"),
            Self::DbTimeout => write!(f, "Timed out waiting for postgres"),
        }
    }
}

impl Error for PostgresError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Pool(e) => Some(e),
            Self::Diesel(e) => Some(e),
            Self::DbTimeout => None,
        }
    }
}
