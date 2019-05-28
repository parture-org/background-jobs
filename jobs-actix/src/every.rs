use std::time::Duration;

use super::{Job, QueueHandle};
use actix::{Actor, AsyncContext, Context};
use log::error;

/// A type used to schedule recurring jobs.
///
/// ```rust,ignore
/// let server = ServerConfig::new(storage).start();
/// Every::new(server, Duration::from_secs(60 * 30), MyJob::new()).start();
/// ```
pub struct Every<J>
where
    J: Job + Clone + 'static,
{
    spawner: QueueHandle,
    duration: Duration,
    job: J,
}

impl<J> Every<J>
where
    J: Job + Clone + 'static,
{
    pub fn new(spawner: QueueHandle, duration: Duration, job: J) -> Self {
        Every {
            spawner,
            duration,
            job,
        }
    }
}

impl<J> Actor for Every<J>
where
    J: Job + Clone + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        match self.spawner.queue(self.job.clone()) {
            Ok(_) => (),
            Err(_) => error!("Failed to queue job"),
        };

        ctx.run_interval(self.duration.clone(), move |actor, _| {
            match actor.spawner.queue(actor.job.clone()) {
                Ok(_) => (),
                Err(_) => error!("Failed to queue job"),
            }
        });
    }
}
