use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use crate::ArcRead;
use crate::storage::domain::blob::Blob;
use crate::storage::domain::bucket::Bucket;
use crate::storage::file_handle::FileHandle;

pub struct DataPage {
    pub bucket: Bucket,
    pub file: Arc<RwLock<FileHandle>>,
    data: Vec<Blob>,
}
static NEWLINE: u8 = b'\n';

impl DataPage {
    pub fn open_page(bucket: Bucket, file: Arc<RwLock<FileHandle>>) -> DataPage {
        return DataPage {
            bucket,
            file,
            data: Vec::new()
        };
    }

    pub fn read(&self) -> Vec<Blob> {
        let handle_lock = self.file.read().unwrap();

        let file = self.open_read(handle_lock.path.clone());
        let mut reader = BufReader::new(file);

        let mut bytes: [u8; 8] = [0,0,0,0,0,0,0,0];
        let mut blobs = Vec::<Blob>::new();

        loop {
            let read_result = reader.read_exact(&mut bytes);
            if let Err(e) = read_result {
                break;
            }

            let timestamp = i64::from_ne_bytes(bytes);

            reader.read_exact(&mut bytes).unwrap();
            let value = f64::from_ne_bytes(bytes);

            let blob = Blob::new(timestamp, value);
            blobs.push(blob);
        }

        return blobs;
    }

    pub fn write(&mut self, record: Blob) {
        // let file_guard = self.file.write().unwrap();

        // if self.data.len() == 0 {
        //     let handle = file_guard;
        //
        //     let file = self.open_read(handle.path.clone());
        //     let mut reader = BufReader::new(file);
        //
        //     loop {
        //         let mut prefix_buf: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        //         let read_result = reader.read_exact(&mut prefix_buf);
        //
        //         if let Err(e) = read_result {
        //             break;
        //         }
        //
        //         let len = i64::from_ne_bytes(prefix_buf);
        //         let mut buf = vec![0u8; len as usize];
        //         reader.read_exact(&mut buf).unwrap();
        //
        //         let (decoded, _): (Blob, usize) = bincode::decode_from_slice(
        //             &buf[..], config::standard()).unwrap();
        //
        //         self.data.push(decoded);
        //     }
        // }

        self.data.push(record);
    }

    pub fn update(
        mut self,
        bucket: Bucket,
        file: Arc<RwLock<FileHandle>>) -> DataPage {

        // Move the vec out of self, so that the vec can be reused.
        let mut vec: Vec<Blob>;
        {
            self.flush();
            vec = self.data;
            vec.clear();

            // Self is dropped here
        }

        return DataPage {
            bucket,
            file,
            data: vec
        }
    }

    pub fn flush(&mut self) {
        let mut guard = self.file.write().unwrap();
        let handle = guard.deref();
        let file = self.open_append(&handle.path);

        let mut writer = BufWriter::new(&file);

        for &blob in self.data.iter() {

            let timestamp = i64::to_ne_bytes(blob.timestamp);
            let data = f64::to_ne_bytes(blob.data);

            writer.write(&timestamp).unwrap();
            writer.write(&data).unwrap();
        }

        self.data.clear();
    }

    pub fn open_read(&self, path_buf: PathBuf) -> File {
        return OpenOptions::new()
            .read(true)
            .open(path_buf.clone())
            .unwrap();
    }

    pub fn open_append(&self, path_buf: &PathBuf) -> File {
        return OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(path_buf.clone())
            .unwrap();
    }
}