use std::borrow::BorrowMut;
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

        let container = match self.files.get_mut(key.as_str()) {
            Some(v) => v,
            None => {
                let container = Container::new(self.path.clone());
                self.files.insert(key.clone(), container);
                self.files.get_mut(key.as_str()).unwrap()
            }
        };

        container.write(record.index, record.data);
    }
}
