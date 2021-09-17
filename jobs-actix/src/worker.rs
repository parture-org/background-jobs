use crate::Server;
use actix_rt::spawn;
use background_jobs_core::{CachedProcessorMap, JobInfo};
use tokio::sync::mpsc::{channel, Sender};
use tracing::{debug, error, info, warn, Span};
use tracing_futures::Instrument;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait Worker {
    async fn process(&self, job: JobInfo) -> Result<(), JobInfo>;

    fn id(&self) -> Uuid;

    fn queue(&self) -> &str;
}

#[derive(Clone)]
pub(crate) struct LocalWorkerHandle {
    tx: Sender<JobInfo>,
    id: Uuid,
    queue: String,
}

#[async_trait::async_trait]
impl Worker for LocalWorkerHandle {
    async fn process(&self, job: JobInfo) -> Result<(), JobInfo> {
        match self.tx.clone().send(job).await {
            Err(e) => {
                error!("Unable to send job");
                Err(e.0)
            }
            _ => Ok(()),
        }
    }

    fn id(&self) -> Uuid {
        self.id
    }

    fn queue(&self) -> &str {
        &self.queue
    }
}

pub(crate) fn local_worker<State>(
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Server,
) where
    State: Clone + 'static,
{
    let id = Uuid::new_v4();

    let span = tracing::info_span!(
        "Worker",
        worker.id = tracing::field::display(&id),
        worker.queue = tracing::field::display(&queue),
        exception.message = tracing::field::Empty,
        exception.details = tracing::field::Empty,
    );

    spawn(
        async move {
            let (tx, mut rx) = channel(16);

            let handle = LocalWorkerHandle { tx, id, queue };

            let span = Span::current();

            debug!("Beginning worker loop for {}", id);
            if let Err(e) = server.request_job(Box::new(handle.clone())).await {
                let display = format!("{}", e);
                let debug = format!("{:?}", e);
                span.record("exception.message", &tracing::field::display(&display));
                span.record("exception.details", &tracing::field::display(&debug));
                error!("Failed to notify server of new worker, {}", e);
                return;
            }
            while let Some(job) = rx.recv().await {
                let return_job = processors.process(job).await;

                if let Err(e) = server.return_job(return_job).await {
                    warn!("Failed to return completed job, {}", e);
                }
                if let Err(e) = server.request_job(Box::new(handle.clone())).await {
                    let display = format!("{}", e);
                    let debug = format!("{:?}", e);
                    span.record("exception.message", &tracing::field::display(&display));
                    span.record("exception.details", &tracing::field::display(&debug));
                    error!("Failed to notify server of ready worker, {}", e);
                    break;
                }
            }
            info!("Worker closing");
        }
        .instrument(span),
    );
}
