# Background Jobs

This crate provides tooling required to run some processes asynchronously from a usually
synchronous application. The standard example of this is Web Services, where certain things
need to be processed, but processing them while a user is waiting for their browser to respond
might not be the best experience.

- [Read the documentation on docs.rs](https://docs.rs/background-jobs)
- [Find the crate on crates.io](https://crates.io/crates/background-jobs)
- [Hit me up on Mastodon](https://asonix.dog/@asonix)

### Usage
#### Add Background Jobs to your project
```toml
[dependencies]
actix-rt = "2.0.0"
background-jobs = "0.9.0"
anyhow = "1.0"
futures = "0.3"
serde = { version = "1.0", features = ["derive"] }
```

#### To get started with Background Jobs, first you should define a job.
Jobs are a combination of the data required to perform an operation, and the logic of that
operation. They implment the `Job`, `serde::Serialize`, and `serde::DeserializeOwned`.

```rust
use background_jobs::Job;
use anyhow::Error;
use futures::future::{ok, Ready};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct MyJob {
    some_usize: usize,
    other_usize: usize,
}

impl MyJob {
    pub fn new(some_usize: usize, other_usize: usize) -> Self {
        MyJob {
            some_usize,
            other_usize,
        }
    }
}

impl Job for MyJob {
    type State = ();
    type Future = Ready<Result<(), Error>>;

    const NAME: &'static str = "MyJob";

    fn run(self, _: Self::State) -> Self::Future {
        info!("args: {:?}", self);

        ok(())
    }
}
```

The run method for a job takes an additional argument, which is the state the job expects to
use. The state for all jobs defined in an application must be the same. By default, the state
is an empty tuple, but it's likely you'll want to pass in some Actix address, or something
else.

Let's re-define the job to care about some application state.

```rust
#[derive(Clone, Debug)]
pub struct MyState {
    pub app_name: String,
}

impl MyState {
    pub fn new(app_name: &str) -> Self {
        MyState {
            app_name: app_name.to_owned(),
        }
    }
}

impl Job for MyJob {
    type State = MyState;
    type Future = Ready<Result<(), Error>>;

    // The name of the job. It is super important that each job has a unique name,
    // because otherwise one job will overwrite another job when they're being
    // registered.
    const NAME: &'static str = "MyJob";

    // The queue that this processor belongs to
    //
    // Workers have the option to subscribe to specific queues, so this is important to
    // determine which worker will call the processor
    //
    // Jobs can optionally override the queue they're spawned on
    const QUEUE: &'static str = DEFAULT_QUEUE;

    // The number of times background-jobs should try to retry a job before giving up
    //
    // Jobs can optionally override this value
    const MAX_RETRIES: MaxRetries = MaxRetries::Count(1);

    // The logic to determine how often to retry this job if it fails
    //
    // Jobs can optionally override this value
    const BACKOFF: Backoff = Backoff::Exponential(2);

    fn run(self, state: Self::State) -> Self::Future {
        info!("{}: args, {:?}", state.app_name, self);

        ok(())
    }
}
```

#### Running jobs
By default, this crate ships with the `background-jobs-actix` feature enabled. This uses the
`background-jobs-actix` crate to spin up a Server and Workers, and provides a mechanism for
spawning new jobs.

`background-jobs-actix` on it's own doesn't have a mechanism for storing worker state. This
can be implemented manually by implementing the `Storage` trait from `background-jobs-core`,
or the provided in-memory store can be used.

With that out of the way, back to the examples:

##### Main
```rust
use background_jobs::{create_server, WorkerConfig};
use anyhow::Error;

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    // Set up our Storage
    // For this example, we use the default in-memory storage mechanism
    use background_jobs::memory_storage::Storage;
    let storage = Storage::new();

    // Start the application server. This guards access to to the jobs store
    let queue_handle = create_server(storage);

    // Configure and start our workers
    WorkerConfig::new(move || MyState::new("My App"))
        .register::<MyJob>()
        .set_worker_count(DEFAULT_QUEUE, 16)
        .start(queue_handle.clone());

    // Queue our jobs
    queue_handle.queue(MyJob::new(1, 2))?;
    queue_handle.queue(MyJob::new(3, 4))?;
    queue_handle.queue(MyJob::new(5, 6))?;

    // Block on Actix
    actix_rt::signal::ctrl_c().await?;
    Ok(())
}
```

##### Complete Example
For the complete example project, see [the examples folder](https://git.asonix.dog/Aardwolf/background-jobs/src/branch/master/examples/actix-example)

#### Bringing your own server/worker implementation
If you want to create your own jobs processor based on this idea, you can depend on the
`background-jobs-core` crate, which provides the Job trait, as well as some
other useful types for implementing a jobs processor and job store.

### Contributing
Feel free to open issues for anything you find an issue with. Please note that any contributed code will be licensed under the GPLv3.

### License
This work is licensed under the Cooperative Software License. This is not a Free Software
License, but may be considered a "source-available License." For most hobbyists, self-employed
developers, worker-owned companies, and cooperatives, this software can be used in most
projects so long as this software is distributed under the terms of the CSL. For more
information, see the provided LICENSE file. If none exists, the license can be found online
[here](https://lynnesbian.space/csl/). If you are a free software project and wish to use this
software under the terms of the GNU Affero General Public License, please contact me at
[asonix@asonix.dog](mailto:asonix@asonix.dog) and we can sort that out. If you wish to use this
project under any other license, especially in proprietary software, the answer is likely no.
