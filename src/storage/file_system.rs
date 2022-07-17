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
    bucket_issuer: BucketIssuer,
    path: PathBuf,
    files: HashMap<Bucket, Arc<RwLock<FileHandle>>>,
    current_page: Arc<RwLock<DataPage>>
}

impl FileSystem {
    fn ensure_genesis_page(path: PathBuf) -> Arc<RwLock<DataPage>> {
        fs::create_dir_all(&path).unwrap();
        let path = Path::new(&path).join("0");
        let bucket = Bucket::new(0);

        let _ = File::create(path.clone()).unwrap();
        let handle = FileHandle::new(path.clone(), bucket);

        let guard = Arc::new(RwLock::new(handle));

        let page = DataPage::open_page(bucket, guard);
        return Arc::new(RwLock::new(page));
    }

    pub fn new(path: PathBuf, bucket_issuer: BucketIssuer) -> FileSystem {
        let mut current_page = Self::ensure_genesis_page(path.clone());

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
            {
                let mut w = current_page.write().unwrap();
                if bucket > w.bucket {
                    *w = DataPage::open_page(bucket, arc.clone());
                }
            }
        }

        return FileSystem {
            bucket_issuer,
            path,
            files,
            current_page
        };
    }
}
