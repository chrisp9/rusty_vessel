use std::fs::OpenOptions;
use std::path::PathBuf;
use crate::OpenChunk;

pub struct Container {
    pub root: PathBuf,
    current_chunk: Option<OpenChunk>
}

impl Container {
    pub fn new(path: PathBuf) -> Container {
        return Container {root: path, current_chunk: None};
    }

    pub fn write(&mut self, index: u64, data: String) {
        if self.current_chunk.is_none() {
            let chunk = OpenChunk::open_latest(self.root.clone());

            self.current_chunk = match chunk {
                Some(c) => Some(c),
                None => Some(OpenChunk::create_new(self.root.clone(),index))
            };
        }

        self.current_chunk.as_ref().unwrap().write(index, &data);
    }
}
