use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::{fs, thread};
use std::cell::RefCell;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use crate::{ArcRead, threading, UnixTime};
use crate::storage::domain::blob::Blob;
use crate::storage::domain::bucket::Bucket;
use crate::storage::domain::data_page::DataPage;
use crate::storage::file_system::FileSystem;
use crate::threading::ArcRw;

pub struct Vessel {
    pub path: PathBuf,
    file_system: Rc<RefCell<FileSystem>>,
    current_page: Option<DataPage>,
    page_length: i64,
}

const BUFFER_SIZE: i32 = 1000;

impl Vessel {
    pub fn new(
        path_buf: PathBuf,
        page_length: i64)
        -> Vessel
    {
        let path = path_buf;

        let (mut file_system, page) = FileSystem::new(
            path.clone(),
        page_length);

        let data_page = page;

        let vessel =  Vessel {
            path,
            file_system: Rc::new(RefCell::new(file_system)),
            current_page: data_page,
            page_length: page_length as i64
        };

        return vessel;
    }

    pub fn flush(&mut self) {
        let page = &mut self.current_page;

        if page.is_some() {
            page.as_mut().unwrap().flush();
        }
    }

    pub fn write(&mut self, records: Rc<Vec<Blob>>) {
        let mut this_page =  &mut self.current_page;

        for record in &*records {
            let record_bucket = Bucket::for_time(
                record.timestamp,
                self.page_length);

            let c = self.file_system.as_ref();
            let mut fs = c.borrow_mut();

            let page = this_page.as_mut();
            match page {
                Some(v) if v.bucket == record_bucket => {
                    v.write(record.clone());
                },
                Some(_) => {
                    let old_page = this_page.take().unwrap();
                    let mut next_page = fs.turn_page(old_page, record_bucket);

                    next_page.write(record.clone());
                    this_page.replace(next_page);
                }
                None => {
                    let mut next_page = fs.create_page(record_bucket);
                    next_page.write(record.clone());
                    this_page.replace(next_page);
                }
            }
        }
    }

    pub fn get_last_time(&self) -> UnixTime {
        let fs: &RefCell<FileSystem> = self.file_system.borrow();
        return fs.borrow().get_last_time().clone();
    }

    pub fn read_from(&self, from: UnixTime) -> VesselIterator {
        return VesselIterator::new(
            self.file_system.clone(),
            Bucket::for_time(from, self.page_length),
        Some(from.clone()));
    }
}

pub struct VesselIterator {
    pub fs: Rc<RefCell<FileSystem>>,
    pub bucket: Bucket,
    pub start: Option<UnixTime>
}

impl VesselIterator {
    pub fn new(
        fs: Rc<RefCell<FileSystem>>,
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
        let b = self.fs.as_ref().borrow();

        let mut data = b.read(self.bucket);

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

            self.start = None;
        }

        let bucket = self.bucket.next();
        self.bucket = bucket;

        return Some(data);
    }
}