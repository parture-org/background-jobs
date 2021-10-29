use crate::Server;
use actix_rt::{spawn, Arbiter};
use background_jobs_core::{CachedProcessorMap, JobInfo};
use log::{debug, error, info, warn};
use tokio::sync::mpsc::{channel, Sender};
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

struct LocalWorkerStarter<State: Clone + 'static> {
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Server,
}

impl<State: Clone + 'static> Drop for LocalWorkerStarter<State> {
    fn drop(&mut self) {
        let res = std::panic::catch_unwind(|| {
            let handle = Arbiter::current();

            handle.spawn(async move {})
        });

        if let Ok(true) = res {
            local_worker(
                self.queue.clone(),
                self.processors.clone(),
                self.server.clone(),
            )
        } else {
            warn!("Not restarting worker, arbiter has died");
        }
    }
}

struct WarnOnDrop(Uuid);

impl Drop for WarnOnDrop {
    fn drop(&mut self) {
        warn!("Worker {} closing", self.0);
    }
}

pub(crate) fn local_worker<State>(
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Server,
) where
    State: Clone + 'static,
{
    let starter = LocalWorkerStarter {
        queue: queue.clone(),
        processors: processors.clone(),
        server: server.clone(),
    };
    let id = Uuid::new_v4();

    let (tx, mut rx) = channel(16);

    let handle = LocalWorkerHandle { tx, id, queue };

    spawn(async move {
        info!("Starting worker {}", id);
        let warn_on_drop = WarnOnDrop(id);
        debug!("Beginning worker loop for {}", id);
        if let Err(e) = server.request_job(Box::new(handle.clone())).await {
            error!("Couldn't request first job, bailing, {}", e);
            return;
        }
        while let Some(job) = rx.recv().await {
            let return_job = processors.process(job).await;

            if let Err(e) = server.return_job(return_job).await {
                error!("Error returning job, {}", e);
            }
            if let Err(e) = server.request_job(Box::new(handle.clone())).await {
                error!("Error requesting job, {}", e);
                break;
            }
        }
        drop(warn_on_drop);
        drop(starter);
    });
}
