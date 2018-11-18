# Background Jobs

This crate provides tooling required to run some processes asynchronously from a usually
synchronous application. The standard example of this is Web Services, where certain things
need to be processed, but processing them while a user is waiting for their browser to respond
might not be the best experience.

### Usage
#### Add Background Jobs to your project
```toml
[dependencies]
background-jobs = "0.2"
failure = "0.1"
futures = "0.1"
tokio = "0.1"
```

#### To get started with Background Jobs, first you should define a job.
Jobs are a combination of the data required to perform an operation, and the logic of that
operation. They implment the `Job`, `serde::Serialize`, and `serde::DeserializeOwned`.

```rust
#[derive(Clone, Debug, Deserialize, Serialize)]
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
    fn run(self) -> Box<dyn Future<Item = (), Error = Error> + Send> {
        info!("args: {:?}", self);

        Box::new(Ok(()).into_future())
    }
}
```

#### Next, define a Processor.
Processors are types that define default attributes for jobs, as well as containing some logic
used internally to perform the job. Processors must implement `Proccessor` and `Clone`.

```rust
#[derive(Clone, Debug)]
pub struct MyProcessor;

impl Processor for MyProcessor {
    // The kind of job this processor should execute
    type Job = MyJob;

    // The name of the processor. It is super important that each processor has a unique name,
    // because otherwise one processor will overwrite another processor when they're being
    // registered.
    const NAME: &'static str = "MyProcessor";

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
    const BACKOFF_STRATEGY: Backoff = Backoff::Exponential(2);
}
```

#### Running jobs
By default, this crate ships with the `background-jobs-server` feature enabled. This uses the
`background-jobs-server` crate to spin up a Server and Workers, and provides a mechanism for
spawning new jobs.

`background-jobs-server` uses LMDB to keep track of local state. LMDB is a memory-mapped storage
mechanism, so the jobs information it keeps track of is all stored locally on-disk. In the future,
the storage mechanism may be made generic so implementors can bring their own storage.

`background-jobs-server` also uses ZeroMQ to transfer data between the spawner, server, and
workers. If you plan to run two or more of these pieces from the same process, look at the
documentation for the methods `new_with_context` and `init_with_context`. It is important that
ZeroMQ contexts are shared when possible to avoid spinning up multiple ZeroMQ instances for the
same application.

With that out of the way, back to the examples:

##### Starting the job server
```rust
use background_jobs::ServerConfig;
use failure::Error;
use server_jobs_example::queue_set;

fn main() -> Result<(), Error> {
    // Run our job server
    tokio::run(ServerConfig::init(
        "127.0.0.1",
        5555,
        1,
        queue_set(),
        "example-db",
    ));

    Ok(())
}
```
##### Starting the job worker
```rust
use background_jobs::WorkerConfig;
use failure::Error;
use server_jobs_example::{queue_map, MyProcessor};

fn main() -> Result<(), Error> {
    // Create the worker config
    let mut worker = WorkerConfig::new("localhost".to_owned(), 5555, queue_map());

    // Register our processor
    worker.register_processor(MyProcessor);

    // Spin up the workers
    tokio::run(worker.run());

    Ok(())
}
```
##### Queuing jobs
```rust
use background_jobs::SpawnerConfig;
use futures::{future::lazy, Future};
use server_jobs_example::{MyJob, MyProcessor};

fn main() {
    // Create 50 new jobs, each with two consecutive values of the fibonacci sequence
    let (_, _, jobs) = (1..50).fold((0, 1, Vec::new()), |(x, y, mut acc), _| {
        acc.push(MyJob::new(x, y));

        (y, x + y, acc)
    });

    // Create the spawner
    let spawner = SpawnerConfig::new("localhost", 5555);

    // Queue each job
    tokio::run(lazy(move || {
        for job in jobs {
            tokio::spawn(spawner.queue::<MyProcessor>(job).map_err(|_| ()));
        }

        Ok(())
    }));
}
```
##### Queuing jobs from a synchronous application
```rust
use background_jobs::SpawnerConfig;
use failure::Error;
use server_jobs_example::{MyJob, MyProcessor};

fn main() -> Result<(), Error> {
    // Create 50 new jobs, each with two consecutive values of the fibonacci sequence
    let (_, _, jobs) = (1..50).fold((0, 1, Vec::new()), |(x, y, mut acc), _| {
        acc.push(MyJob::new(x, y));

        (y, x + y, acc)
    });

    // Create the spawner
    let spawner = SpawnerConfig::new("localhost", 5555);

    // Queue each job
    for job in jobs {
        spawner.queue_sync::<MyProcessor>(job)?
    }
}
```

##### Complete Example
For the complete example project, see [the examples folder](https://git.asonix.dog/asonix/background-jobs/src/branch/master/examples/server-jobs-example)

#### Using on Windows
`background-jobs-server` depends by default on [`tokio-zmq`](https://crates.io/crates/tokio-zmq), which
only works on unix (and unix-like) systems. This might mean it works on the Windows Subsystem for Linux,
but it's untested and hard to say. You can override this behavior by specifying the following in your
Cargo.toml
```toml
[Dependencies.background-jobs]
version = "0.1"
default-features = false
features = ["background-jobs-server", "background-jobs-server/futures-zmq"]
```

[`futures-zmq`](https://crates.io/crates/futures-zmq) Is designed to be a drop-in replacement for
tokio-zmq that works on non-unix and non-tokio platforms. The reason why it isn't enabled by default is
that it's slower than tokio-zmq, and in all likelihood, the production environment for projects
depending on this one will be linux.

#### Not using a ZeroMQ+LMDB based client/server model
If you want to create your own jobs processor based on this idea, you can depend on the
`background-jobs-core` crate, which provides the LMDB storage, Processor and Job traits, as well as some
other useful types for implementing a jobs processor.

### Contributing
Feel free to open issues for anything you find an issue with. Please note that any contributed code will be licensed under the GPLv3.

### License

Copyright © 2018 Riley Trautman

Background Jobs is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

Background Jobs is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details. This file is part of Background Jobs.

You should have received a copy of the GNU General Public License along with Background Jobs. If not, see [http://www.gnu.org/licenses/](http://www.gnu.org/licenses/).
