use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use crate::container::Container;
use std::fs;
use std::rc::Rc;
use std::sync::Arc;
use crate::domain;

pub struct Vessel {
    pub path: PathBuf,
    files: HashMap<String, Container>
}

impl Vessel {
    pub fn new(root: &str, name: &str) -> Vessel {
        let path = Path::new(root)
            .join(name);

        fs::create_dir_all(&path).unwrap();

        return Vessel {
            path,
            files: HashMap::new()
        }
    }

    pub fn write(
        &mut self,
        record: domain::Record)
    {
        let key = record.key;

        let container = match self.files.entry(key) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(
                Container::new(self.path.clone(), 1000))
        };

        if !(container.is_full()) {
            container.write(record.index, record.data);
        }
    }
}
