use std::{marker::PhantomData, sync::Arc};

use crate::{Error, Result};

#[derive(Clone)]
pub struct Tree<T>(Arc<sled::Tree>, PhantomData<T>);

impl<T> Tree<T>
where
    T: serde::de::DeserializeOwned + serde::ser::Serialize,
{
    pub(crate) fn new(t: Arc<sled::Tree>) -> Self {
        Tree(t, PhantomData)
    }

    pub(crate) fn iter(&self) -> Iter<T> {
        Iter::new(self.0.iter())
    }

    pub(crate) fn get<K>(&self, key: K) -> Result<Option<T>>
    where
        K: AsRef<[u8]>,
    {
        match self.0.get(key)? {
            Some(vec) => serde_json::from_slice(&vec)
                .map_err(|_| Error::Deserialize)
                .map(Some),
            None => Ok(None),
        }
    }

    pub(crate) fn set(&self, key: &str, value: T) -> Result<Option<T>> {
        let vec = serde_json::to_vec(&value).map_err(|_| Error::Serialize)?;

        Ok(self.0.set(key, vec)?.map(move |_| value))
    }

    pub(crate) fn del(&self, key: &str) -> Result<Option<T>> {
        match self.0.del(key)? {
            Some(vec) => serde_json::from_slice(&vec)
                .map_err(|_| Error::Deserialize)
                .map(Some),
            None => Ok(None),
        }
    }

    pub(crate) fn fetch_and_update<F>(&self, key: &str, f: F) -> Result<Option<T>>
    where
        F: Fn(Option<T>) -> Option<T>,
    {
        let final_opt = self.0.fetch_and_update(key, |opt| {
            let new_opt = match opt {
                Some(vec) => {
                    let t = serde_json::from_slice(&vec).map(Some).unwrap_or(None);

                    (f)(t)
                }
                None => (f)(None),
            };

            match new_opt {
                Some(t) => serde_json::to_vec(&t).map(Some).unwrap_or(None),
                None => None,
            }
        })?;

        match final_opt {
            Some(vec) => serde_json::from_slice(&vec)
                .map_err(|_| Error::Deserialize)
                .map(Some),
            None => Ok(None),
        }
    }
}

pub(crate) struct Iter<'a, T>(sled::Iter<'a>, PhantomData<T>);

impl<'a, T> Iter<'a, T> {
    fn new(i: sled::Iter<'a>) -> Self {
        Iter(i, PhantomData)
    }
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: serde::de::DeserializeOwned,
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
    T: serde::de::DeserializeOwned,
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
