use std::fs;
use std::fs::{DirEntry, File, OpenOptions};
use std::path::PathBuf;
use std::io::{BufRead, BufReader, BufWriter, Write};

pub struct OpenChunk {
    dir: File,
    data: Vec<String>
}

impl OpenChunk {
    pub fn is_full(&self, capacity: i32) -> bool {
        let r = self.data.len() >= capacity as usize;
        println!("{r}");

        return r;
    }

    pub fn open_latest(path: PathBuf) -> Option<OpenChunk> {
        let paths = fs::read_dir(&path).unwrap();

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
            let file = Self::open_file(file.path().clone());
            let reader = BufReader::new(&file);
            let mut data : Vec<String> = Vec::new();

            for line in reader.lines() {
                data.push(line.unwrap());
            }

            return OpenChunk {
                dir: file,
                data
            };
        });
    }

    pub fn append(&mut self, timestamp: u64, data: &str) {
        self.data.push(format!("{data}"));
    }

    pub fn write(&mut self) {
        let mut f = BufWriter::new(&self.dir);
        for item in &self.data {
            writeln!(f, "{item}");
        }
    }

    pub fn create_new(root: PathBuf, timestamp: u64) -> OpenChunk {
        let path =  root.join(timestamp.to_string());
        let _ = File::create(path.clone()).unwrap();
        let file = Self::open_file(path.clone());

        return OpenChunk {
            dir: file,
            data: Vec::new()
        }
    }

    fn open_file(path: PathBuf) -> File {
        return OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(path.clone())
            .unwrap();
    }
}