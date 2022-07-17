use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use crate::domain;
use crate::storage::domain::{BucketIssuer, Record};
use crate::storage::file_system::FileSystem;

pub struct Vessel2 {
    pub path: PathBuf,
    file_system: Arc<RwLock<FileSystem>>,
}

const BUFFER_SIZE: i32 = 1000;

impl Vessel2 {
    pub fn new(db_root: &str, key: &str, stride: chrono::Duration) -> Vessel2 {
        let path = Path::new(db_root).join(key);
        let bucket_issuer = BucketIssuer::new(BUFFER_SIZE, stride);

        let file_system = FileSystem::new(path.clone(), bucket_issuer);

        return Vessel2 {
            path,
            file_system: Arc::new(RwLock::new(file_system))
        };
    }

    pub fn write(&mut self, record: Record) {


    }

    pub fn read(&mut self, key: String) {
       // let container = Self::get_cursor(
        //    self.files.entry(key),
        //    self.path.clone());

        //container.write(0, "");
    }
}