use crate::{Job, QueueHandle};
use actix_rt::time::{interval_at, Instant};
use std::time::Duration;

/// A type used to schedule recurring jobs.
///
/// ```rust,ignore
/// let server = create_server(storage);
/// server.every(Duration::from_secs(60 * 30), MyJob::new());
/// ```
pub(crate) async fn every<J>(spawner: QueueHandle, duration: Duration, job: J)
where
    J: Job + Clone + Send,
{
    let mut interval = interval_at(Instant::now(), duration);

    loop {
        interval.tick().await;

        let job = job.clone();
        if spawner.queue::<J>(job).await.is_err() {
            tracing::error!("Failed to queue job: {}", J::NAME);
        }
    }
}
