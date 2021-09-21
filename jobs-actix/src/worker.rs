use crate::Server;
use actix_rt::spawn;
use background_jobs_core::{CachedProcessorMap, JobInfo};
use tokio::sync::mpsc::{channel, Sender};
use tracing::{error, info, warn, Span};
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

impl LocalWorkerHandle {
    fn span(&self, operation: &str) -> Span {
        tracing::info_span!(
            "Worker",
            worker.id = tracing::field::display(&self.id),
            worker.queue = tracing::field::display(&self.queue),
            worker.operation.id = tracing::field::display(&Uuid::new_v4()),
            worker.operation.name = tracing::field::display(operation),
            exception.message = tracing::field::Empty,
            exception.details = tracing::field::Empty,
        )
    }
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
    spawn(async move {
        let id = Uuid::new_v4();
        let (tx, mut rx) = channel(16);

        let handle = LocalWorkerHandle { tx, id, queue };

        loop {
            let span = handle.span("request");
            if let Err(e) = server
                .request_job(Box::new(handle.clone()))
                .instrument(span.clone())
                .await
            {
                let display = format!("{}", e);
                let debug = format!("{:?}", e);
                span.record("exception.message", &tracing::field::display(&display));
                span.record("exception.details", &tracing::field::display(&debug));
                span.in_scope(|| error!("Failed to notify server of ready worker, {}", e));
                break;
            }
            drop(span);

            if let Some(job) = rx.recv().await {
                let return_job = processors
                    .process(job)
                    .instrument(handle.span("process"))
                    .await;

                let span = handle.span("return");
                if let Err(e) = server.return_job(return_job).instrument(span.clone()).await {
                    let display = format!("{}", e);
                    let debug = format!("{:?}", e);
                    span.record("exception.message", &tracing::field::display(&display));
                    span.record("exception.details", &tracing::field::display(&debug));
                    span.in_scope(|| warn!("Failed to return completed job, {}", e));
                }

                continue;
            }

            break;
        }
        handle.span("closing").in_scope(|| info!("Worker closing"));
    });
}
