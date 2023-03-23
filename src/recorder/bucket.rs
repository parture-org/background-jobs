use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

pub struct Bucket {
    timestamp: Instant,
    count: u64,
}

pub(crate) struct Buckets {
    oldest: Duration,
    span: Duration,
    max: usize,
    buckets: VecDeque<Bucket>,
}

impl Buckets {
    pub(super) fn new(oldest: Duration, span: Duration, max: usize) -> Self {
        Self {
            oldest,
            span,
            max,
            buckets: VecDeque::new(),
        }
    }

    pub(super) fn sum(&self) -> u64 {
        self.buckets.iter().fold(0, |acc, item| acc + item.count)
    }

    pub(super) fn count(&mut self, value: u64, timestamp: Instant) {
        while let Some(bucket) = self.buckets.front() {
            if bucket.timestamp + self.oldest < timestamp {
                self.buckets.pop_front();
                continue;
            }

            break;
        }

        if let Some(bucket) = self.bucket_mut(timestamp) {
            bucket.count += value;
            return;
        }

        self.insert(value, timestamp);
    }

    fn bucket_mut(&mut self, timestamp: Instant) -> Option<&mut Bucket> {
        self.buckets.iter_mut().find(|bucket| {
            if let Some(upper) = bucket.timestamp.checked_add(self.span) {
                bucket.timestamp < timestamp && timestamp <= upper
            } else {
                false
            }
        })
    }

    fn insert(&mut self, value: u64, timestamp: Instant) {
        if self.buckets.len() == self.max {
            self.buckets.pop_front();
        }

        let found = self
            .buckets
            .iter()
            .enumerate()
            .find(|(_, bucket)| timestamp < bucket.timestamp);

        if let Some((index, bucket)) = found {
            let mut timestamp_index = bucket.timestamp;

            while let Some(lower) = timestamp_index.checked_sub(self.span) {
                if lower < timestamp {
                    self.buckets.insert(
                        index,
                        Bucket {
                            timestamp: lower,
                            count: value,
                        },
                    );

                    return;
                }

                timestamp_index = lower;
            }
        } else {
            self.buckets.push_back(Bucket {
                timestamp,
                count: value,
            });
        }
    }
}
