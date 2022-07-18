use std::collections::HashMap;
use std::fmt::Error;
use std::fs;
use std::fs::{DirEntry, File};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::current;
use chrono::{DateTime, Utc};
use crate::storage::domain;
use crate::storage::domain::{Bucket, BucketIssuer, DataPage, UnixTime};
use crate::storage::guarded_file::FileHandle;

pub struct FileSystem {
    path: PathBuf,
    files: HashMap<Bucket, Arc<RwLock<FileHandle>>>,
}

impl FileSystem {
    pub fn new(path: PathBuf, bucket_issuer: BucketIssuer) -> FileSystem {
        fs::create_dir_all(path.clone());
        // Read the file system to build up an in-memory index of the
        // current file system. It doesn't matter that this is slow,
        // because this only happens on initialization.
        let paths = fs::read_dir(&path)
            .unwrap()
            .collect::<Vec<Result<DirEntry, std::io::Error>>>()
            .into_iter()
            .map(|v| v.unwrap())
            .map(|v| (v.file_name().into_string().unwrap().parse::<UnixTime>().unwrap(), v))
            .collect::<Vec<(UnixTime, DirEntry)>>();

        let mut files = HashMap::new();

        for (date, entry) in paths {
            let bucket = bucket_issuer.get_bucket_for(date);
            let node = FileHandle::new(entry.path(), bucket);
            let arc = Arc::new(RwLock::new(node));

            files.insert(bucket, arc.clone());
        }

        return FileSystem {
            path,
            files
        };
    }

    pub fn create_page(&mut self, bucket: Bucket) -> DataPage {
        let file = self.files.get(&bucket);

        if let Some(v) = file {
            return DataPage::open_page(bucket, v.clone());
        }

        let file_handle = FileHandle::new(self.path.clone().join(bucket.value.to_string()), bucket);
        File::create(&file_handle.path).unwrap();

        let arc = Arc::new(RwLock::new(file_handle));
        self.files.insert(bucket, arc.clone());

        return DataPage::open_page(bucket, arc)
    }

    pub fn update_page(&mut self, mut page: DataPage, bucket: Bucket) -> DataPage {
        let file = self.files.get(&bucket);

        if let Some(v) = file {
            return page.update(bucket, v.clone());
        }

        let file_handle = FileHandle::new(self.path.clone().join(bucket.value.to_string()), bucket);
        File::create(&file_handle.path).unwrap();

        let arc = Arc::new(RwLock::new(file_handle));
        self.files.insert(bucket, arc.clone());

        return page.update(bucket, arc)
    }
}
