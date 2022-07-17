use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};
use crate::storage::domain;
use crate::storage::guarded_file::FileHandle;

pub type UnixTime = i64;

pub struct Record {
    pub timestamp: DateTime<Utc>,
    pub data: String
}

pub struct BucketIssuer {
    bucket_width: i64
}

impl BucketIssuer {
    pub fn new(batch_size: i32, stride: chrono::Duration) -> BucketIssuer {
        let width = batch_size as i64 * stride.num_milliseconds() as i64;

        return BucketIssuer {
            bucket_width: width
        }
    }

    pub fn get_bucket_for(&self, instant_unix: UnixTime) -> Bucket {
        let bucket_for_time = instant_unix - (instant_unix % self.bucket_width);

        return Bucket{
            value: bucket_for_time as i64
        };
    }

    pub fn next(&self, bucket: &Bucket) -> Bucket {
        return Bucket {
            value: bucket.value + self.bucket_width};
    }
}

#[derive(PartialEq, PartialOrd, Copy, Clone, Hash, Eq, Ord)]
pub struct Bucket {
    pub value: i64
}

impl Bucket {
    pub fn new(val: i64) -> Bucket {
        return Bucket {value: val};
    }
}


pub struct DataPage {
    pub bucket: Bucket,
    pub file: Arc<RwLock<FileHandle>>,
    pub data: Option<Vec<String>>
}

impl DataPage {
    pub fn open_page(bucket: Bucket, file: Arc<RwLock<FileHandle>>) -> DataPage {
        return DataPage {
            bucket,
            file,
            data: None
        };
    }

    pub fn update(self, bucket: Bucket, file: Arc<RwLock<FileHandle>>) -> DataPage {
        let mut opt: Option<Vec<String>> = None;

        // Move the vec out of self, so that the vec can be reused.
        {
            let this = self;

            if let Some(mut val) = this.data {
                val.clear();
                opt = Some(val);
            }

            // Self is dropped here
        }

        return DataPage {
            bucket,
            file,
            data: opt
        }
    }
}
