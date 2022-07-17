use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use crate::domain;
use crate::storage::domain::Bucket;
use crate::storage::file_system::FileSystem;

pub struct FileHandle {
    path: PathBuf,
    bucket: Bucket
}

impl FileHandle {
    pub fn new(path: PathBuf, bucket: Bucket) -> FileHandle {
        return FileHandle {
            path,
            bucket
        }
    }
}

// impl FileHandle {
//     pub fn new(path: PathBuf) -> FileHandle {
//         return FileHandle {
//             path,
//         };
//     }
//
//     fn open_file(&self) -> File {
//         return OpenOptions::new()
//             .read(true)
//             .write(true)
//             .append(true)
//             .open(self.path.clone())
//             .unwrap();
//     }
//
//     fn read(&self) -> Vec<(u64, String)> {
//         let file =  self.open_file();
//         let file_name = file.file_name().unwrap();
//
//         let timestamp = file_name
//             .parse::<i64>()
//             .unwrap();
//
//         let reader = BufReader::new(&file);
//         let mut results = Vec::new();
//
//         for line in reader.lines() {
//             let vec = line.unwrap().split("|").collect::<Vec<&str>>();
//             timestamp = vec[0].parse::<u64>().unwrap();
//
//             results.push((timestamp, vec[1]));
//         }
//
//         return results;
//     }
//
//     fn write(&self) {
//
//     }
