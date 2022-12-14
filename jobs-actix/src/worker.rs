use crate::Server;
use background_jobs_core::CachedProcessorMap;
use std::future::Future;
use tracing::Span;
use tracing_futures::Instrument;
use uuid::Uuid;

struct LocalWorkerStarter<State: Clone + 'static, Extras: 'static> {
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Server,
    extras: Option<Extras>,
}

impl<State: Clone + 'static, Extras: 'static> Drop for LocalWorkerStarter<State, Extras> {
    fn drop(&mut self) {
        metrics::counter!("background-jobs.worker.finished", 1, "queue" => self.queue.clone());

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
            tracing::warn!("Not restarting worker, Arbiter is dead");
            drop(extras);
        }
    }
}

struct RunOnDrop<F>(F)
where
    F: Fn();

impl<F> Drop for RunOnDrop<F>
where
    F: Fn(),
{
    fn drop(&mut self) {
        (self.0)();
    }
}

async fn time_job<F: Future + Unpin>(mut future: F, job_id: Uuid) -> <F as Future>::Output {
    let mut interval = actix_rt::time::interval(std::time::Duration::from_secs(5));
    interval.tick().await;
    let mut count = 0;

    loop {
        tokio::select! {
            output = &mut future => { break output }
            _ = interval.tick() => {
                count += 5;

                if count > (60 * 60) {
                    if count % (60 * 20) == 0 {
                        tracing::warn!("Job {} is taking a long time: {} hours", job_id, count / 60 / 60);
                    }
                } else if count > 60 {
                    if count % 20 == 0 {
                        tracing::warn!("Job {} is taking a long time: {} minutes", job_id, count / 60);
                    }
                } else {
                    tracing::info!("Job {} is taking a long time: {} seconds", job_id, count);
                }
            }
        }
    }
}

pub(crate) async fn local_worker<State, Extras>(
    queue: String,
    processors: CachedProcessorMap<State>,
    server: Server,
    extras: Extras,
) where
    State: Clone + 'static,
    Extras: 'static,
{
    metrics::counter!("background-jobs.worker.started", 1, "queue" => queue.clone());

    let starter = LocalWorkerStarter {
        queue: queue.clone(),
        processors: processors.clone(),
        server: server.clone(),
        extras: Some(extras),
    };

    let id = Uuid::new_v4();

    let log_on_drop = RunOnDrop(|| {
        make_span(id, &queue, "closing").in_scope(|| tracing::warn!("Worker closing"));
    });

    loop {
        let request_span = make_span(id, &queue, "request");

        let job = match request_span
            .in_scope(|| server.request_job(id, &queue))
            .instrument(request_span.clone())
            .await
        {
            Ok(job) => job,
            Err(e) => {
                metrics::counter!("background-jobs.worker.failed-request", 1);

                let display_val = format!("{}", e);
                let debug = format!("{:?}", e);
                request_span.record("exception.message", &tracing::field::display(&display_val));
                request_span.record("exception.details", &tracing::field::display(&debug));
                request_span
                    .in_scope(|| tracing::error!("Failed to notify server of ready worker"));
                break;
            }
        };
        drop(request_span);

        let process_span = make_span(id, &queue, "process");
        let job_id = job.id();
        let return_job = process_span
            .in_scope(|| time_job(Box::pin(processors.process(job)), job_id))
            .instrument(process_span)
            .await;

        let return_span = make_span(id, &queue, "return");
        if let Err(e) = return_span
            .in_scope(|| server.return_job(return_job))
            .instrument(return_span.clone())
            .await
        {
            metrics::counter!("background-jobs.worker.failed-return", 1);

            let display_val = format!("{}", e);
            let debug = format!("{:?}", e);
            return_span.record("exception.message", &tracing::field::display(&display_val));
            return_span.record("exception.details", &tracing::field::display(&debug));
            return_span.in_scope(|| tracing::warn!("Failed to return completed job"));
        }
        drop(return_span);
    }

    drop(log_on_drop);
    drop(starter);
}

fn make_span(id: Uuid, queue: &str, operation: &str) -> Span {
    tracing::info_span!(
        parent: None,
        "Worker",
        worker.id = tracing::field::display(id),
        worker.queue = tracing::field::display(queue),
        worker.operation.id = tracing::field::display(&Uuid::new_v4()),
        worker.operation.name = tracing::field::display(operation),
        exception.message = tracing::field::Empty,
        exception.details = tracing::field::Empty,
    )
}
