use crate::storage::Storage;
use background_jobs_core::ProcessorMap;
use std::{
    future::{poll_fn, Future},
    pin::Pin,
};
use tracing::{Instrument, Span};
use uuid::Uuid;

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

async fn heartbeat_job<F: Future>(
    storage: &Storage,
    future: F,
    job_id: Uuid,
    runner_id: Uuid,
    heartbeat_interval: u64,
) -> F::Output {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(heartbeat_interval));

    let mut future = std::pin::pin!(future);

    let mut hb_future = Some(storage.heartbeat(job_id, runner_id));

    loop {
        tokio::select! {
            output = &mut future => {
                break output;
            },
            Some(hb_output) = option(hb_future.as_mut()), if hb_future.is_some() => {
                hb_future.take();

                if let Err(e) = hb_output {
                    tracing::warn!("Failed to heartbeat: {e}");
                }
            }
            _ = interval.tick() => {
                if hb_future.is_none() {
                    hb_future = Some(storage.heartbeat(job_id, runner_id));
                }
            }
        }
    }
}

async fn time_job<F: Future>(future: F, job_id: Uuid) -> F::Output {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    interval.tick().await;
    let mut count = 0;

    let mut future = std::pin::pin!(future);

    loop {
        tokio::select! {
            output = &mut future => {
                break output;
            },
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

async fn option<F>(opt: Option<&mut F>) -> Option<F::Output>
where
    F: Future + Unpin,
{
    match opt {
        Some(f) => Some(poll_fn(|cx| Pin::new(&mut *f).poll(cx)).await),
        None => None,
    }
}

pub(crate) async fn local_worker<State>(
    queue: String,
    processors: ProcessorMap<State>,
    server: Storage,
) where
    State: Send + Clone + 'static,
{
    metrics::counter!("background-jobs.tokio.worker.started", "queue" => queue.clone())
        .increment(1);

    let id = Uuid::now_v7();

    let log_on_drop = RunOnDrop(|| {
        make_span(id, &queue, "closing").in_scope(|| tracing::debug!("Worker closing"));
    });

    loop {
        let request_span = make_span(id, &queue, "request");

        let job = match request_span
            .in_scope(|| server.pop(&queue, id))
            .instrument(request_span.clone())
            .await
        {
            Ok(job) => job,
            Err(e) => {
                metrics::counter!("background-jobs.tokio.worker.failed-request").increment(1);

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
        let job_id = job.id;
        let heartbeat_interval = job.heartbeat_interval;
        let return_job = process_span
            .in_scope(|| {
                heartbeat_job(
                    &server,
                    time_job(processors.process(job), job_id),
                    job_id,
                    id,
                    heartbeat_interval,
                )
            })
            .instrument(process_span)
            .await;

        let return_span = make_span(id, &queue, "return");
        if let Err(e) = return_span
            .in_scope(|| server.complete(return_job))
            .instrument(return_span.clone())
            .await
        {
            metrics::counter!("background-jobs.tokio.worker.failed-return").increment(1);

            let display_val = format!("{}", e);
            let debug = format!("{:?}", e);
            return_span.record("exception.message", &tracing::field::display(&display_val));
            return_span.record("exception.details", &tracing::field::display(&debug));
            return_span.in_scope(|| tracing::warn!("Failed to return completed job"));
        }
        drop(return_span);
    }

    drop(log_on_drop);
}

fn make_span(id: Uuid, queue: &str, operation: &str) -> Span {
    tracing::info_span!(
        parent: None,
        "Worker",
        worker.id = tracing::field::display(id),
        worker.queue = tracing::field::display(queue),
        worker.operation.id = tracing::field::display(&Uuid::now_v7()),
        worker.operation.name = tracing::field::display(operation),
        exception.message = tracing::field::Empty,
        exception.details = tracing::field::Empty,
    )
}
