use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::{fs, thread};
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use crate::{ArcRead, threading};
use crate::domain::UnixTime;
use crate::storage::domain::blob::Blob;
use crate::storage::domain::bucket::Bucket;
use crate::storage::domain::data_page::DataPage;
use crate::storage::file_system::FileSystem;
use crate::threading::ArcRw;

pub struct Vessel {
    pub path: PathBuf,
    file_system: ArcRw<FileSystem>,
    current_page: ArcRw<Option<DataPage>>,
    page_length: i64
}

const BUFFER_SIZE: i32 = 1000;

impl Vessel {
    pub fn new(
        db_root: &str,
        key: &str,
        page_length: i64)
        -> ArcRw<Vessel>
    {
        let path = Path::new(db_root).join(key);

        let (mut file_system, page) = FileSystem::new(
            path.clone(),
        page_length);

        let data_page = ArcRw::new(page);

        let vessel =  Vessel {
            path,
            file_system: ArcRw::new(file_system),
            current_page: data_page,
            page_length: page_length as i64
        };

        let vessel = ArcRw::new(vessel);
        let flush_vessel = vessel.clone();

        thread::spawn(move|| {
            loop {
                {
                    let vessel = flush_vessel.write_lock();
                    let mut page =
                        vessel.current_page.write_lock();

                    if page.is_some() {
                        page.as_mut().unwrap().flush();
                    }
                }

                thread::sleep(Duration::from_secs(1));
            }
        });

        return vessel;
    }


    pub fn write(&self, records: &Vec<Blob>) {
        let mut this_page= self.current_page.write_lock();

        for record in records {
            let record_bucket = Bucket::for_time(
                record.timestamp,
                self.page_length);

            let page = this_page.as_mut();
            match page {
                Some(v) if v.bucket == record_bucket => {
                    v.write(record.clone());
                },
                Some(_) => {
                    let mut fs = self.file_system.write_lock();
                    let old_page = this_page.take().unwrap();
                    let mut next_page = fs.turn_page(old_page, record_bucket);

                    next_page.write(record.clone());
                    this_page.replace(next_page);
                }
                None => {
                    let mut fs = self.file_system.write_lock();
                    let mut next_page = fs.create_page(record_bucket);
                    next_page.write(record.clone());
                    this_page.replace(next_page);
                }
            }
        }
    }


    pub fn get_last_time(&self) -> UnixTime{
        let mut lock = self.file_system.read_lock();
        return lock.get_last_time().clone();
    }

    pub fn read_from(&self, from: UnixTime) -> VesselIterator {
        return VesselIterator::new(
            self.file_system.clone(),
            Bucket::for_time(from, self.page_length),
        Some(from));
    }
}

pub struct VesselIterator {
    pub fs: ArcRw<FileSystem>,
    pub bucket: Bucket,
    pub start: Option<UnixTime>
}

impl VesselIterator {
    pub fn new(
        fs: ArcRw<FileSystem>,
        bucket: Bucket,
        start: Option<UnixTime>) -> VesselIterator
    {
        return VesselIterator {
            fs,
            bucket,
            start
        };
    }
}

impl Iterator for VesselIterator{
    type Item = Vec::<Blob>;

    fn next(&mut self) -> Option<Self::Item> {
        let lock = self.fs.read_lock();
        let mut data = lock.read(self.bucket);

        if data.len() == 0 {
            return None;
        }

        // As we read an entire page per call, for the first call
        // we may need to truncate values prior to the start time.
        if let Some(v) = self.start {
            data = data
                .into_iter()
                .filter(|blob| blob.timestamp > v)
                .into_iter()
                .collect::<Vec::<Blob>>();

            println!("{}", data.len());

            self.start = None;
        }

        let bucket = self.bucket.next();
        self.bucket = bucket;

        return Some(data);
    }
}