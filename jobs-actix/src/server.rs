use std::{ops::Deref, sync::Arc};

use background_jobs_core::Storage;

use crate::storage::{ActixStorage, StorageWrapper};

/// The server Actor
///
/// This server guards access to Thee storage, and keeps a list of workers that are waiting for
/// jobs to process
#[derive(Clone)]
pub(crate) struct Server {
    storage: Arc<dyn ActixStorage + Send + Sync>,
}

impl Server {
    /// Create a new Server from a compatible storage implementation
    pub(crate) fn new<S>(storage: S) -> Self
    where
        S: Storage + Sync + 'static,
    {
        Server {
            storage: Arc::new(StorageWrapper(storage)),
        }
    }
}

impl Deref for Server {
    type Target = dyn ActixStorage;

    fn deref(&self) -> &Self::Target {
        self.storage.as_ref()
    }
}
