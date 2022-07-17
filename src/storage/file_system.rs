use std::collections::HashMap;
use std::fmt::Error;
use std::fs;
use std::fs::DirEntry;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use chrono::{DateTime, Utc};
use crate::storage::domain::{Bucket, BucketIssuer, DataPage, FileNode};
use crate::storage::guarded_file::FileHandle;

pub struct FileSystem {
    bucket_issuer: BucketIssuer,
    path: PathBuf,
    files: HashMap<Bucket, Arc<RwLock<FileHandle>>>,
    current_page: Arc<RwLock<DataPage>>
}

impl FileSystem {
    pub fn new(path: PathBuf, bucket_issuer: BucketIssuer) -> FileSystem {
        fs::create_dir_all(&path).unwrap();

        // Read the file system to build up an in-memory index of the
        // current file system. It doesn't matter that this is slow,
        // because this only happens on initialization.
        let paths = fs::read_dir(&path)
            .unwrap()
            .collect::<Vec<Result<DirEntry, std::io::Error>>>()
            .iter()
            .map(|v| v.unwrap())
            .map(|v| (v.file_name().parse::<DateTime<Utc>>().unwrap(), v))
            .collect::<Vec<(DateTime<Utc>, DirEntry)>>();

        let mut files = HashMap::new();

        for (date, entry) in paths {
            let bucket = bucket_issuer.get_bucket_for(date);
            let node = FileHandle::new(entry.path());
            files.insert(bucket, Arc::new(RwLock::new(node)));
        }

        return FileSystem {
            bucket_issuer,
            path,
            files
        };
    }
}
