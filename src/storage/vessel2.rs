use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use crate::{ArcRead, threading};
use crate::storage::bucket_issuer::{BucketIssuer, UnixTime};
use crate::storage::domain::blob::Blob;
use crate::storage::domain::data_page::DataPage;
use crate::storage::file_system::FileSystem;
use crate::threading::ArcRw;

pub struct Vessel {
    pub path: PathBuf,
    file_system: ArcRw<FileSystem>,
    bucket_issuer: BucketIssuer,
    current_page: ArcRw<Option<DataPage>>
}

const BUFFER_SIZE: i32 = 1000;

impl Vessel {
    pub fn new(
        db_root: &str,
        key: &str,
        stride: chrono::Duration)
        -> ArcRw<Vessel>
    {
        let path = Path::new(db_root).join(key);
        let bucket_issuer = BucketIssuer::new(BUFFER_SIZE, stride);

        let (mut file_system, page) = FileSystem::new(
            path.clone(),
            bucket_issuer.clone());

        let last_time = file_system.get_last_time();
        let data_page = ArcRw::new(page);

        let vessel =  Vessel {
            path,
            file_system: ArcRw::new(file_system),
            bucket_issuer,
            current_page: data_page
        };

        let vessel = ArcRw::new(vessel);
        let flush_vessel = vessel.clone();

        tokio::task::spawn(async move {
            loop {
                {
                    let vessel = flush_vessel.write();
                    let mut page = vessel.current_page.write();

                    if page.is_some() {
                        page.as_mut().unwrap().flush();
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        return vessel;
    }

    pub fn write(&mut self, record: Blob) {
        let this_bucket = self
            .bucket_issuer
            .get_bucket_for(record.timestamp);

        let mut this_page= self.current_page.write();

        match this_page.as_mut() {

            None => {
                let mut fs = self.file_system.write();

                let mut that_page = fs.create_page(this_bucket);
                that_page.write(record);

                this_page.replace(that_page);

            }

            Some(v) => {
                let mut chunk = this_page.take().unwrap();

                if this_bucket.value == chunk.bucket.value {
                    chunk.write(record.clone());
                    this_page.replace(chunk);
                } else {
                    println!("{} {}", this_bucket.value.to_string(), chunk.bucket.value.to_string());

                    let mut fs = self.file_system.write();

                    let that_page = fs.update_page(chunk, this_bucket);
                    this_page.replace(that_page);
                }
            }
        }
    }

    pub fn get_last_time(&self) -> UnixTime{
        let mut lock = self.file_system.read();
        return lock.get_last_time().clone();
    }

    pub fn read(&mut self, key: String) {
        // let container = Self::get_cursor(
        //    self.files.entry(key),
        //    self.path.clone());

        //container.write(0, "");
    }
}