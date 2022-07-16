use std::fs::OpenOptions;
use std::path::PathBuf;
use crate::OpenChunk;

pub struct Container {
    pub root: PathBuf,
    current_chunk: Option<OpenChunk>,
    chunk_capacity: i32
}

impl Container {
    pub fn new(path: PathBuf, capacity: i32) -> Container {
        return Container {root: path, current_chunk: None, chunk_capacity: capacity};
    }

    pub fn write(&mut self, index: u64, data: String) {
        if self.current_chunk.is_none() {
            let chunk = OpenChunk::open_latest(self.root.clone());

            self.current_chunk = match chunk {
                Some(c) => Some(c),
                None => Some(OpenChunk::create_new(self.root.clone(),index))
            };
        }

        let mut a = self.current_chunk.as_mut().unwrap().write(index, &data);

    }

    pub fn is_full(&self) -> bool {
        return match &self.current_chunk {
            Some(chunk) => chunk.is_full(self.chunk_capacity),
            None => false
        }
    }
}
