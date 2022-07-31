use std::collections::{BTreeMap, HashMap};
use std::fmt::Error;
use std::fs;
use std::fs::{DirEntry, File};
use std::marker::PhantomData;
use std::ops::Index;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::current;
use chrono::{DateTime, Utc};
use crate::Blob;
use crate::domain::UnixTime;
use crate::storage::domain::bucket::Bucket;
use crate::storage::domain::data_page::DataPage;
use crate::storage::file_handle::FileHandle;

pub struct FileSystem {
    path: PathBuf,
    files: BTreeMap<Bucket, Arc<RwLock<FileHandle>>>,
}

impl FileSystem {
    pub fn new(path: PathBuf, page_length: i64) -> (FileSystem, Option<DataPage>) {
        fs::create_dir_all(path.clone());
        // Read the file system to build up an in-memory index of the
        // current file system. It doesn't matter that this is slow,
        // because this only happens on initialization.
        let mut paths = fs::read_dir(&path)
            .unwrap()
            .collect::<Vec<Result<DirEntry, std::io::Error>>>()
            .into_iter()
            .map(|v| v.unwrap())
            .map(|v| (v.file_name().into_string().unwrap().parse::<UnixTime>().unwrap(), v))
            .collect::<Vec<(UnixTime, DirEntry)>>();

        paths.sort_by(|(a,_),(b,_)| a.cmp(b));

        let mut files = BTreeMap::new();
        let mut last: Option<Arc<RwLock<FileHandle>>> = None;

        for (date, entry) in paths {
            let bucket = Bucket::new(date, page_length);
            let node = FileHandle::new(entry.path(), bucket);
            let arc = Arc::new(RwLock::new(node));

            files.insert(bucket, arc.clone());
            last = Some(arc.clone());
        }

        let mut file_system = FileSystem {
            path,
            files
        };

        let page = last.map(|v| {
            let guard = v.write().unwrap();
            return file_system.create_page(guard.bucket);
        });

        return (file_system, page);
    }

    pub fn read(&self, bucket: Bucket) -> Vec<Blob> {
        let file_handle = self.files.get(&bucket);

        if let Some(v) = file_handle {
            let page = DataPage::open_page(bucket.clone(),  v.clone());
            return page.read();
        }

        return vec![];
    }

    pub fn get_last_time(&self) -> UnixTime {
        let last = self.files.last_key_value();

        return match last {
            Some((bucket, file)) => {
                let page = DataPage::open_page(bucket.clone(), file.clone());

                let records = page.read();
                if records.len() == 0 {
                    return 0;
                }

                let last = &records[records.len()-1];

                return last.timestamp;
            }
            None => 0
        }
    }

    pub fn create_page(&mut self, bucket: Bucket) -> DataPage {
        let file = self.files.get(&bucket);

        if let Some(v) = file {
            return DataPage::open_page(bucket, v.clone());
        }

        let file = self.create(bucket);
        return DataPage::open_page(bucket, file)
    }

    pub fn update_page(&mut self, mut page: DataPage, bucket: Bucket) -> DataPage {
        let file = self.files.get(&bucket);

        if let Some(v) = file {
            return page.update(bucket, v.clone());
        }

        let file = self.create(bucket);
        return page.update(bucket, file)
    }

    fn create(&mut self, bucket: Bucket) -> Arc<RwLock<FileHandle>> {
        let file_handle = FileHandle::new(
            self.path.clone().join(bucket.val.to_string()), bucket);

        let arc = Arc::new(RwLock::new(file_handle));
        self.files.insert(bucket, arc.clone());

        return arc;
    }
}
