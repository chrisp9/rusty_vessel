use std::cell::{Cell, RefCell};
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::task;
use crate::open_chunk::OpenChunk;
use crate::threading;

pub struct Cursor {
    pub path: PathBuf,
    current_chunk: Arc<Mutex<Cell<OpenChunk>>>,
    chunk_capacity: i32
}

impl Cursor {
    pub fn new(path: PathBuf, capacity: i32) -> Cursor {
        let chunk = Arc::new(
            Mutex::new(
                    Cell::new(
                OpenChunk::open_latest(path.clone()))));

        let cursor = Cursor {
            path,
            current_chunk: chunk.clone(),
            chunk_capacity: capacity};

        fn flush(mut guard: MutexGuard<Cell<OpenChunk>>) {
             guard.get_mut().write();
        }

        // Periodically flush the buffer to disk if necessary.
        tokio::spawn(async move {
            loop {
                flush(chunk.lock().unwrap());
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        return cursor;
    }

    pub fn read(&mut self, index: u64) {

    }

    pub fn write(&mut self, index: u64, data: String) {
        let mut guarded_chunk = &mut *self.current_chunk.lock().unwrap();

        // If the current chunk is not full, write to it, otherwise create a new chunk
        // and write to the new chunk.
        let chunk = match Self::is_full(guarded_chunk.get_mut(), self.chunk_capacity) {
            false => guarded_chunk,
            true => {
                guarded_chunk.get_mut().write();

                guarded_chunk.replace(
                    OpenChunk::create_new(self.path.clone(), index));

                guarded_chunk
            }
        };

        chunk.get_mut().append(index, &data);
    }

    pub fn is_full(chunk: &mut OpenChunk, capacity: i32) -> bool {
        return chunk.is_full(capacity);
    }
}