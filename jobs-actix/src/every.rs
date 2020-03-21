use crate::{Job, QueueHandle};
use actix::{
    clock::{interval_at, Duration, Instant},
    Arbiter,
};
use log::error;

/// A type used to schedule recurring jobs.
///
/// ```rust,ignore
/// let server = ServerConfig::new(storage).start();
/// every(server, Duration::from_secs(60 * 30), MyJob::new());
/// ```
pub fn every<J>(spawner: QueueHandle, duration: Duration, job: J)
where
    J: Job + Clone,
{
    Arbiter::spawn(async move {
        let mut interval = interval_at(Instant::now(), duration);

        loop {
            interval.tick().await;

            match spawner.queue(job.clone()) {
                Err(_) => error!("Failed to queue job"),
                _ => (),
            };
        }
    });
}
