use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use chrono::{DateTime, Utc};
use crate::storage::domain;
use crate::storage::guarded_file::FileHandle;

pub type UnixTime = i64;

pub struct Record {
    pub timestamp: i64,
    pub data: String
}

#[derive(Clone)]
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
    pub data: Vec<Record>
}

impl DataPage {
    pub fn open_page(bucket: Bucket, file: Arc<RwLock<FileHandle>>) -> DataPage {
        return DataPage {
            bucket,
            file,
            data: Vec::new()
        };
    }

    pub fn write(&mut self, record: Record) {
        let file_guard = self.file.write().unwrap();

        if self.data.len() == 0 {
            let handle = file_guard;

            let file = self.open_read(handle.path.clone());
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line.unwrap();
                let vec = line.split("|").collect::<Vec<&str>>();

                let timestamp = vec[0].parse::<i64>().unwrap();
                let data = vec[1];

                let record = Record {
                    timestamp,
                    data: data.to_string()
                };

                self.data.push(record);
            }
        }

        self.data.push(record);
    }

    pub fn update(mut self, bucket: Bucket, file: Arc<RwLock<FileHandle>>) -> DataPage {
        // Move the vec out of self, so that the vec can be reused.
        let mut opt: Vec<Record>;
        {
            self.flush();
            opt = self.data;

            // Self is dropped here
        }

        return DataPage {
            bucket,
            file,
            data: opt
        }
    }

    fn flush(&mut self) {
        let mut guard = self.file.write().unwrap();
        let dref = guard.deref();
        let file = self.open_write(&dref.path);

        let mut writer = BufWriter::new(&file);

        for record in &self.data {
            let ts = &record.timestamp.to_string();
            let data = &record.data;
            writeln!(writer, "{ts}|{data}");
        }
    }

    pub fn open_read(&self, path_buf: PathBuf) -> File {
        return OpenOptions::new()
            .read(true)
            .open(path_buf.clone())
            .unwrap();
    }

    pub fn open_write(&self, path_buf: &PathBuf) -> File {
        return OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(path_buf.clone())
            .unwrap();
    }
}
