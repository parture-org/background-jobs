use crate::Server;
use background_jobs_core::{CachedProcessorMap, JobInfo};
use log::{debug, error, warn};
use tokio::sync::mpsc::{channel, Sender};
use uuid::Uuid;

#[async_trait::async_trait]
pub trait Worker {
    async fn process_job(&self, job: JobInfo) -> Result<(), JobInfo>;

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
    async fn process_job(&self, job: JobInfo) -> Result<(), JobInfo> {
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

    let (tx, mut rx) = channel(16);

    let handle = LocalWorkerHandle {
        tx: tx.clone(),
        id,
        queue: queue.clone(),
    };

    actix::spawn(async move {
        debug!("Beginning worker loop for {}", id);
        if let Err(e) = server.request_job(Box::new(handle.clone())).await {
            error!("Couldn't request first job, bailing, {}", e);
            return;
        }
        while let Some(job) = rx.recv().await {
            let return_job = processors.process_job(job).await;

            if let Err(e) = server.return_job(return_job).await {
                error!("Error returning job, {}", e);
            }
            if let Err(e) = server.request_job(Box::new(handle.clone())).await {
                error!("Error requesting job, {}", e);
                break;
            }
        }
        warn!("Worker {} closing", id);
    });
}
