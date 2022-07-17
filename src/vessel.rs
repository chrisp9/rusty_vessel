// use std::collections::hash_map::Entry;
// use std::collections::HashMap;
// use std::path::{Path, PathBuf};
// use crate::cursor::Cursor;
// use std::fs;
// use std::marker::PhantomData;
// use std::rc::Rc;
// use std::sync::Arc;
// use crate::{domain, storage};
//
// pub struct Vessel {
//     pub path: PathBuf,
//     files: HashMap<String, Cursor>
// }
//
// impl Vessel {
//     pub fn new(root: &str, name: &str) -> Vessel {
//         let path = Path::new(root).join(name);
//
//         fs::create_dir_all(&path).unwrap();
//
//         return Vessel {
//             path,
//             files: HashMap::new()
//         };
//     }
//
//     pub fn read(&mut self, key: String) {
//         let container = Self::get_cursor(
//             self.files.entry(key),
//             self.path.clone());
//
//         //container.write(0, "");
//     }
//
//     pub fn write(&mut self, record: storage::domain::Record) {
//         let container = Self::get_cursor(
//             self.files.entry(record.key),
//             self.path.clone());
//
//         container.write(record.index, record.data);
//     }
//
//     fn get_cursor(cursor: Entry<String, Cursor>, path: PathBuf)  -> &mut Cursor {
//         return match cursor {
//             Entry::Occupied(o) => o.into_mut(),
//             Entry::Vacant(v) => v.insert(
//                 Cursor::new(path, 1000))
//         };
//     }
// }
