use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use crate::{domain, threading};
use crate::storage::domain::{Bucket, BucketIssuer, DataPage, Record, UnixTime};
use crate::storage::file_system::FileSystem;

pub struct Vessel2 {
    pub path: PathBuf,
    file_system: Arc<RwLock<FileSystem>>,
    bucket_issuer: BucketIssuer,
    current_page: Arc<RwLock<Option<DataPage>>>
}

const BUFFER_SIZE: i32 = 1000;

impl Vessel2 {
    pub fn new(db_root: &str, key: &str, stride: chrono::Duration) -> Vessel2 {
        let path = Path::new(db_root).join(key);
        let bucket_issuer = BucketIssuer::new(BUFFER_SIZE, stride);

        let file_system = FileSystem::new(path.clone(), bucket_issuer.clone());
        let data_page = Arc::new(RwLock::new(None));

        return Vessel2 {
            path,
            file_system: Arc::new(RwLock::new(file_system)),
            bucket_issuer,
            current_page: data_page
        };
    }

    pub fn write(&mut self, record: Record) {
        println!("Writing");

        let this_bucket = self.bucket_issuer.get_bucket_for(record.timestamp);
        let mut this_page = threading::write_lock(
            &self.current_page);

        if this_page.is_none() {
            let mut file_system = threading::write_lock(
                &self.file_system);

            let that_page = file_system.create_page(this_bucket);
            this_page.replace(that_page);
        }
        else {
            let mut chunk = this_page.take().unwrap();
            println!("{} {}", this_bucket.value.to_string(), chunk.bucket.value.to_string());

            if this_bucket.value == chunk.bucket.value {
                chunk.write(record);
                this_page.replace(chunk);
            } else {
                let that_page = threading::write_lock(&self.file_system)
                    .update_page(chunk, this_bucket);

                let _ = this_page.replace(that_page);
            }
        }
    }


    pub fn read(&mut self, key: String) {
        // let container = Self::get_cursor(
        //    self.files.entry(key),
        //    self.path.clone());

        //container.write(0, "");
    }
}