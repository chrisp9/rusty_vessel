use std::cmp::Ordering;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};
use crate::storage::guarded_file::FileHandle;

pub struct Record {
    pub timestamp: DateTime<Utc>,
    pub data: String
}

pub struct BucketIssuer {
    bucket_width: i64
}

impl BucketIssuer {
    pub fn new(batch_size: i32, stride: chrono::Duration) -> BucketIssuer {
        let width = batch_size as i64 * stride.num_milliseconds();

        return BucketIssuer {
            bucket_width: width
        }
    }

    pub fn get_bucket_for(&self, instant: chrono::DateTime<Utc>) -> Bucket {
        let instant_unix = instant.timestamp_millis();
        let bucket_for_time = instant_unix - (instant_unix % self.bucket_width);

        return Bucket {
            bucket_for_time
        };
    }

    pub fn next(&self, bucket: &Bucket) -> Bucket {
        return bucket + self.bucket_width;
    }
}

pub type Bucket = i64;

impl FileHandle {
    pub fn new(path: PathBuf, bucket: Bucket) -> FileHandle {
        let handle = FileHandle::new(path, bucket);
        return handle;

    }
}

pub struct DataPage {
    pub bucket: Bucket,
    pub file_handle: Arc<RwLock<FileHandle>>,
    pub data: Vec<String>
}

impl DataPage {
    pub fn open_page(bucket: Bucket, file: Arc<RwLock<FileHandle>>) -> DataPage {
        let data = Vec!{};

        return DataPage {
            bucket,
            file_handle: file,
            data
        };
    }
}
