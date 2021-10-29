use crate::Server;
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

pub(crate) struct LocalWorkerStarter<State: Clone + 'static, Extras: 'static> {
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Server,
    extras: Option<Extras>,
}

impl<State: Clone + 'static, Extras: 'static> LocalWorkerStarter<State, Extras> {
    pub(super) fn new(
        queue: String,
        processors: CachedProcessorMap<State>,
        server: Server,
        extras: Extras,
    ) -> Self {
        LocalWorkerStarter {
            queue,
            processors,
            server,
            extras: Some(extras),
        }
    }
}

impl<State: Clone + 'static, Extras: 'static> Drop for LocalWorkerStarter<State, Extras> {
    fn drop(&mut self) {
        let res = std::panic::catch_unwind(|| actix_rt::Arbiter::current().spawn(async move {}));

        let extras = self.extras.take().unwrap();

        if let Ok(true) = res {
            actix_rt::spawn(local_worker(
                self.queue.clone(),
                self.processors.clone(),
                self.server.clone(),
                extras,
            ));
        } else {
            warn!("Not restarting worker, Arbiter is dead");
            drop(extras);
        }
    }
}

struct LogOnDrop<F>(F)
where
    F: Fn() -> Span;

impl<F> Drop for LogOnDrop<F>
where
    F: Fn() -> Span,
{
    fn drop(&mut self) {
        (self.0)().in_scope(|| info!("Worker closing"));
    }
}

async fn local_worker<State, Extras>(
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Server,
    extras: Extras,
) where
    State: Clone + 'static,
    Extras: 'static,
{
    let starter = LocalWorkerStarter {
        queue: queue.clone(),
        processors: processors.clone(),
        server: server.clone(),
        extras: Some(extras),
    };

    let id = Uuid::new_v4();
    let (tx, mut rx) = channel(16);

    let handle = LocalWorkerHandle { tx, id, queue };

    let log_on_drop = LogOnDrop(|| handle.span("closing"));

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

    drop(log_on_drop);
    drop(starter);
}
