use background_jobs_core::{JobInfo, Stats, Storage};
use chrono::offset::Utc;

mod error;
mod sled_wrappers;

pub use error::Error;

use self::{error::Result, sled_wrappers::Tree};

#[derive(Clone)]
pub struct SledStorage {
    jobinfo: Tree<JobInfo>,
    running: Tree<u64>,
    running_inverse: Tree<u64>,
    queue: Tree<String>,
    stats: Tree<Stats>,
    lock: Tree<u64>,
    db: sled::Db,
}

impl Storage for SledStorage {
    type Error = Error;

    fn generate_id(&mut self) -> Result<u64> {
        self.db.generate_id().map_err(Error::from)
    }

    fn save_job(&mut self, job: JobInfo) -> Result<()> {
        self.jobinfo.set(&job_key(job.id()), job).map(|_| ())
    }

    fn fetch_job(&mut self, id: u64) -> Result<Option<JobInfo>> {
        self.jobinfo.get(&job_key(id))
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
                queue_tree.del(&job_key(job.id()))?;
            }

            Ok(job)
        })
    }

    fn queue_job(&mut self, queue: &str, id: u64) -> Result<()> {
        if let Some(runner_id) = self.running_inverse.del(&job_key(id))? {
            self.running.del(&runner_key(runner_id))?;
        }

        self.queue.set(&job_key(id), queue.to_owned()).map(|_| ())
    }

    fn run_job(&mut self, id: u64, runner_id: u64) -> Result<()> {
        self.queue.del(&job_key(id))?;
        self.running.set(&runner_key(runner_id), id)?;
        self.running_inverse.set(&job_key(id), runner_id)?;

        Ok(())
    }

    fn delete_job(&mut self, id: u64) -> Result<()> {
        self.jobinfo.del(&job_key(id))?;
        self.queue.del(&job_key(id))?;

        if let Some(runner_id) = self.running_inverse.del(&job_key(id))? {
            self.running.del(&runner_key(runner_id))?;
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
    pub fn new(db: sled::Db) -> Result<Self> {
        Ok(SledStorage {
            jobinfo: open_tree(&db, "background-jobs-jobinfo")?,
            running: open_tree(&db, "background-jobs-running")?,
            running_inverse: open_tree(&db, "background-jobs-running-inverse")?,
            queue: open_tree(&db, "background-jobs-queue")?,
            stats: open_tree(&db, "background-jobs-stats")?,
            lock: open_tree(&db, "background-jobs-lock")?,
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

fn open_tree<T>(db: &sled::Db, name: &str) -> sled::Result<Tree<T>>
where
    T: serde::de::DeserializeOwned + serde::ser::Serialize,
{
    db.open_tree(name).map(Tree::new)
}
