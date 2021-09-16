use crate::{Job, QueueHandle};
use actix_rt::time::{interval_at, Instant};
use std::time::Duration;
use tracing::error;

/// A type used to schedule recurring jobs.
///
/// ```rust,ignore
/// let server = create_server(storage);
/// server.every(Duration::from_secs(60 * 30), MyJob::new());
/// ```
pub(crate) fn every<J>(spawner: &QueueHandle, duration: Duration, job: J)
where
    J: Job + Clone + Send,
{
    let spawner_clone = spawner.clone();
    spawner.arbiter.spawn(async move {
        let mut interval = interval_at(Instant::now(), duration);

        loop {
            interval.tick().await;

            if spawner_clone.queue(job.clone()).is_err() {
                error!("Failed to queue job");
            }
        }
    });
}
