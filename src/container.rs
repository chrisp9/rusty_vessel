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

    pub fn get_current_chunk(&mut self, chunk: &mut OpenChunk, index: u64) -> &mut OpenChunk {
        match chunk.is_full(self.chunk_capacity) {
            true => chunk,
            false => self.current_chunk.insert(
                OpenChunk::create_new(self.root.clone, index))
        }
    }

    pub fn write(&mut self, index: u64, data: String) {
        let this_chunk = self.current_chunk.get_or_insert_with(|| {

            let latest_chunk = OpenChunk::open_latest(self.root.clone());

            return match latest_chunk {
                Some(c) => c,

                // If there is no latest chunk, create a new genesis chunk
                None => OpenChunk::create_new(self.root.clone(), index)
            };
        });

        // If the current chunk is not full, write to it,
        // otherwise create a new chunk and write to the
        // new chunk.
        self.get_current_chunk(this_chunk, index)
            .write(index, &data);
    }

    pub fn is_full(&self) -> bool {
        return match &self.current_chunk {
            Some(chunk) => chunk.is_full(self.chunk_capacity),
            None => false
        }
    }
}