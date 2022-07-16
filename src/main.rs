use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions, read_dir};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use crate::domain::Record;
use crate::vessel::Vessel;

mod cursor;
mod vessel;
mod domain;
mod open_chunk;

static CHUNK_SIZE:i32 = 1000;

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
