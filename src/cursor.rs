use std::fs::OpenOptions;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::task;
use crate::open_chunk::OpenChunk;
use crate::threading;

pub struct Cursor {
    pub root: PathBuf,
    current_chunk: Arc<Mutex<Option<OpenChunk>>>,
    chunk_capacity: i32
}

impl Cursor {
    pub fn new(path: PathBuf, capacity: i32) -> Cursor {
        let chunk = Arc::new(Mutex::new(None));

        let cursor = Cursor {
            root: path,
            current_chunk: chunk.clone(),
            chunk_capacity: capacity};

        fn flush(mut guard: MutexGuard<Option<OpenChunk>>) {
            match &mut *guard {
                Some(c) => c.write(),
                None => ()
            };
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

    pub fn write(&mut self, index: u64, data: String) {
        let mut guarded_chunk = &mut *self.current_chunk.lock().unwrap();

        let this_chunk = guarded_chunk.get_or_insert_with(|| {

            let latest_chunk = OpenChunk::open_latest(self.root.clone());

            return match latest_chunk {
                Some(c) => c,

                // If there is no latest chunk, create a new genesis chunk
                None => OpenChunk::create_new(self.root.clone(), index)
            };
        });

        // If the current chunk is not full, write to it, otherwise create a new chunk
        // and write to the new chunk.
        let chunk = match Self::is_full(Some(this_chunk), self.chunk_capacity) {
            false => this_chunk,
            true => {
                this_chunk.write();

                guarded_chunk.insert(
                    OpenChunk::create_new(self.root.clone(), index))
            }
        };

        chunk.append(index, &data);
    }

    pub fn is_full(chunk: Option<&mut OpenChunk>, capacity: i32) -> bool {
        return match chunk {
            Some(chunk) => chunk.is_full(capacity),
            None => false
        }
    }
}