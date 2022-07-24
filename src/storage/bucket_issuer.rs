use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::fs::{File, OpenOptions, read};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};
use bincode::{config, Decode, Encode};
use bincode::config::{Config, Configuration};
use bincode::de::read::Reader;

use crate::storage::domain;
use crate::storage::domain::bucket::Bucket;
pub type UnixTime = i64;

#[derive(Clone, Copy)]
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

    pub fn next(&self, bucket: Bucket) -> Bucket {
        return Bucket {
            value: bucket.value + self.bucket_width};
    }
}
