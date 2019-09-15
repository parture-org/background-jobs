use background_jobs_core::{JobInfo, Stats, Storage};
use chrono::offset::Utc;
use sled_extensions::{structured::{cbor, bincode::Tree}, Db, DbExt};

pub use sled_extensions::error::{Result, Error};

#[derive(Clone)]
pub struct SledStorage {
    jobinfo: cbor::Tree<JobInfo>,
    running: Tree<u64>,
    running_inverse: Tree<u64>,
    queue: Tree<String>,
    stats: Tree<Stats>,
    lock: Tree<u64>,
    db: Db,
}

impl Storage for SledStorage {
    type Error = Error;

    fn generate_id(&mut self) -> Result<u64> {
        Ok(self.db.generate_id()?)
    }

    fn save_job(&mut self, job: JobInfo) -> Result<()> {
        self.jobinfo
            .insert(job_key(job.id()).as_bytes(), job)
            .map(|_| ())
    }

    fn fetch_job(&mut self, id: u64) -> Result<Option<JobInfo>> {
        self.jobinfo.get(job_key(id))
    }

    fn fetch_job_from_queue(&mut self, queue: &str) -> Result<Option<JobInfo>> {
        let queue_tree = self.queue.clone();
        let job_tree = self.jobinfo.clone();

        self.lock_queue(queue, move || {
            let now = Utc::now();

            let job = queue_tree
                .iter()
                .filter_map(|res| res.ok())
                .filter_map(|(id, in_queue)| if queue == in_queue { Some(id) } else { None })
                .filter_map(|id| job_tree.get(id).ok())
                .filter_map(|opt| opt)
                .filter(|job| job.is_ready(now))
                .next();

            if let Some(ref job) = job {
                queue_tree.remove(&job_key(job.id()))?;
            }

            Ok(job)
        })
    }

    fn queue_job(&mut self, queue: &str, id: u64) -> Result<()> {
        if let Some(runner_id) = self.running_inverse.remove(&job_key(id))? {
            self.running.remove(&runner_key(runner_id))?;
        }

        self.queue
            .insert(job_key(id).as_bytes(), queue.to_owned())
            .map(|_| ())
    }

    fn run_job(&mut self, id: u64, runner_id: u64) -> Result<()> {
        self.queue.remove(job_key(id))?;
        self.running.insert(runner_key(runner_id).as_bytes(), id)?;
        self.running_inverse
            .insert(job_key(id).as_bytes(), runner_id)?;

        Ok(())
    }

    fn delete_job(&mut self, id: u64) -> Result<()> {
        self.jobinfo.remove(&job_key(id))?;
        self.queue.remove(&job_key(id))?;

        if let Some(runner_id) = self.running_inverse.remove(&job_key(id))? {
            self.running.remove(&runner_key(runner_id))?;
        }

        Ok(())
    }

    fn get_stats(&self) -> Result<Stats> {
        Ok(self.stats.get("stats")?.unwrap_or(Stats::default()))
    }

    fn update_stats<F>(&mut self, f: F) -> Result<()>
    where
        F: Fn(Stats) -> Stats,
    {
        self.stats.fetch_and_update("stats", |opt| {
            let stats = match opt {
                Some(stats) => stats,
                None => Stats::default(),
            };

            Some((f)(stats))
        })?;

        Ok(())
    }
}

impl SledStorage {
    pub fn new(db: Db) -> Result<Self> {
        Ok(SledStorage {
            jobinfo: db.open_cbor_tree("background-jobs-jobinfo")?,
            running: db.open_bincode_tree("background-jobs-running")?,
            running_inverse: db.open_bincode_tree("background-jobs-running-inverse")?,
            queue: db.open_bincode_tree("background-jobs-queue")?,
            stats: db.open_bincode_tree("background-jobs-stats")?,
            lock: db.open_bincode_tree("background-jobs-lock")?,
            db,
        })
    }

    fn lock_queue<T, F>(&self, queue: &str, f: F) -> Result<T>
    where
        F: Fn() -> Result<T>,
    {
        let id = self.db.generate_id()?;

        let mut prev;
        while {
            prev = self.lock.fetch_and_update(queue, move |opt| match opt {
                Some(_) => opt,
                None => Some(id),
            })?;

            prev.is_some()
        } {}

        let res = (f)();

        self.lock.fetch_and_update(queue, |_| None)?;

        res
    }
}

fn job_key(id: u64) -> String {
    format!("job-{}", id)
}

fn runner_key(runner_id: u64) -> String {
    format!("runner-{}", runner_id)
}
