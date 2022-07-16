use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions, read_dir};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use crate::domain::Record;
use crate::vessel::Vessel;

mod container;
mod vessel;
mod domain;

static CHUNK_SIZE:i32 = 1000;

pub struct OpenChunk {
    dir: File,
    size: i32
}

impl OpenChunk {
    pub fn is_full(&self, capacity: i32) -> bool {
        let r = self.size >= capacity;
        println!("{r}");

        return r;
    }

    pub fn open_latest(path: PathBuf) -> Option<OpenChunk> {
        let paths = fs::read_dir(path).unwrap();

        let mut recent_chunk: Option<(i64, DirEntry)> = None;

        for path in paths {
            let file = path.unwrap();
            let file_name = file.file_name().into_string().unwrap();

           let timestamp = file_name
               .parse::<i64>()
               .unwrap();

            recent_chunk = match &recent_chunk {

                Some ((time, f)) =>
                    if timestamp > *time { Some((timestamp, file)) }
                    else { recent_chunk }
                None => Some((timestamp, file))
            };
        }

        return recent_chunk.map(|(time, file)| {
            let file = OpenOptions::new().read(true).write(true).append(true).open(
                file.path().clone()).unwrap();

            let reader = BufReader::new(&file);

            let mut size = 0;

            for _ in reader.lines() {
                size += 1;
            }

            return OpenChunk {
                size,
                dir: file
            };
        });
    }

    pub fn write(&mut self, timestamp: u64, data: &str) {
        self.size = self.size + 1;
        let mut f = BufWriter::new(&self.dir);
        let _ = writeln!(f, "{data}");
    }

    pub fn create_new(root: PathBuf, timestamp: u64) -> OpenChunk {
        let path =  root.join(timestamp.to_string());
        File::create(path.clone()).unwrap();

        let file = OpenOptions::new().read(true).write(true).append(true).open(
            path.clone()).unwrap();

        return OpenChunk {
            dir: file,
            size: 0
        }
    }
}

fn main() {
    let root = "/home/chris/rusty_vessel";
    let mut vessel = Vessel::new(root, "Temperature");

    for i in 0..10000 {
        let record = Record {
            key: "London".to_string(),
            index: i,
            data: "35".to_string()};

        vessel.write(record);
    }
}
