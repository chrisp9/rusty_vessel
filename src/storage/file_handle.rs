use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use crate::storage::domain::bucket::Bucket;
use crate::storage::file_system::FileSystem;

pub struct FileHandle {
    pub path: PathBuf,
    pub bucket: Bucket
}

impl FileHandle {
    pub fn new(path: PathBuf, bucket: Bucket) -> FileHandle {
        return FileHandle {
            path,
            bucket
        }
    }
}