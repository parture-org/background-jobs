use background_jobs_core::{JobInfo, Storage, Stats};
use failure::Fail;
use std::{marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct SledStorage {
    jobinfo: Tree<JobInfo>,
    running: Tree<u64>,
    running_inverse: Tree<u64>,
    queue: Tree<String>,
    stats: Tree<Stats>,
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
        let job = self
            .queue
            .iter()
            .filter_map(|res| res.ok())
            .filter_map(|(id, in_queue)| if queue == in_queue { Some(id) } else { None })
            .filter_map(|id| self.jobinfo.get(id).ok())
            .filter_map(|opt| opt)
            .next();

        Ok(job)
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

fn job_key(id: u64) -> String {
    format!("job-{}", id)
}

fn runner_key(runner_id: u64) -> String {
    format!("runner-{}", runner_id)
}

impl SledStorage {
    pub fn new(db: sled::Db) -> Result<Self> {
        Ok(SledStorage {
            jobinfo: open_tree(&db, "background-jobs-jobinfo")?,
            running: open_tree(&db, "background-jobs-running")?,
            running_inverse: open_tree(&db, "background-jobs-running-inverse")?,
            queue: open_tree(&db, "background-jobs-queue")?,
            stats: open_tree(&db, "background-jobs-stats")?,
            db,
        })
    }
}

fn open_tree<T>(db: &sled::Db, name: &str) -> sled::Result<Tree<T>>
where
    T: serde::de::DeserializeOwned + serde::ser::Serialize,
{
    db.open_tree(name).map(Tree::new)
}


#[derive(Clone)]
struct Tree<T>(Arc<sled::Tree>, PhantomData<T>);

impl<T> Tree<T>
where
    T: serde::de::DeserializeOwned + serde::ser::Serialize,
{
    fn new(t: Arc<sled::Tree>) -> Self {
        Tree(t, PhantomData)
    }

    fn iter(&self) -> Iter<T> {
        Iter::new(self.0.iter())
    }

    fn get<K>(&self, key: K) -> Result<Option<T>>
    where
        K: AsRef<[u8]>
    {
        match self.0.get(key)? {
            Some(vec) => {
                serde_json::from_slice(&vec)
                    .map_err(|_| Error::Deserialize)
                    .map(Some)
            },
            None => Ok(None),
        }
    }

    fn set(&self, key: &str, value: T) -> Result<Option<T>> {
        let vec = serde_json::to_vec(&value).map_err(|_| Error::Serialize)?;

        Ok(self.0.set(key, vec)?.map(move |_| value))
    }

    fn del(&self, key: &str) -> Result<Option<T>> {
        match self.0.del(key)? {
            Some(vec) => {
                serde_json::from_slice(&vec)
                    .map_err(|_| Error::Deserialize)
                    .map(Some)
            },
            None => Ok(None),
        }
    }

    fn fetch_and_update<F>(&self, key: &str, f: F) -> Result<Option<T>>
    where
        F: Fn(Option<T>) -> Option<T>,
    {
        let final_opt = self.0.fetch_and_update(key, |opt| {
            let new_opt = match opt {
                Some(vec) => {
                    let t = serde_json::from_slice(&vec)
                        .map(Some)
                        .unwrap_or(None);

                    (f)(t)
                },
                None => (f)(None),
            };

            match new_opt {
                Some(t) => serde_json::to_vec(&t)
                    .map(Some)
                    .unwrap_or(None),
                None => None,
            }
        })?;

        match final_opt {
            Some(vec) => {
                serde_json::from_slice(&vec)
                    .map_err(|_| Error::Deserialize)
                    .map(Some)
            },
            None => Ok(None),
        }
    }
}

struct Iter<'a, T>(sled::Iter<'a>, PhantomData<T>);

impl<'a, T> Iter<'a, T> {
    fn new(i: sled::Iter<'a>) -> Self {
        Iter(i, PhantomData)
    }
}

#[derive(Clone, Debug, Fail)]
pub enum Error {
    #[fail(display = "Error in database: {}", _0)]
    Sled(#[cause] sled::Error),

    #[fail(display = "Failed to deserialize data")]
    Deserialize,

    #[fail(display = "Failed to serialize data")]
    Serialize,
}

type Result<T> = std::result::Result<T, Error>;

impl<'a, T> Iterator for Iter<'a, T>
where
    T: serde::de::DeserializeOwned
{
    type Item = Result<(Vec<u8>, T)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|res| {
            res.map_err(Error::from).and_then(|(k, v)| {
                serde_json::from_slice(&v)
                    .map(|item| (k, item))
                    .map_err(|_| Error::Deserialize)
            })
        })
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T>
where
    T: serde::de::DeserializeOwned
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|res| {
            res.map_err(Error::from).and_then(|(k, v)| {
                serde_json::from_slice(&v)
                    .map(|item| (k, item))
                    .map_err(|_| Error::Deserialize)
            })
        })
    }
}

impl From<sled::Error> for Error {
    fn from(e: sled::Error) -> Self {
        Error::Sled(e)
    }
}
