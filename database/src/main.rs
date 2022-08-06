#![feature(map_first_last)]
#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

extern crate core;
extern crate core;

use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions, read, read_dir};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use crate::storage::vessel2::Vessel;
use bincode::{Encode,Decode};
use chrono::{NaiveDateTime, Utc};
use crate::data_structures::domain::{StreamDefinition, StreamKind};
use crate::data_structures::executor::{Executor};
use crate::storage::domain::blob::Blob;
use crate::streaming::domain::{Stream};
use crate::threading::ArcRead;

mod threading;
mod storage;
mod streaming;
mod domain;
mod data_structures;

static CHUNK_SIZE:i32 = 1000;

pub struct TemperatureRecord {
    pub temp: f64,
    pub altitude: f64
}

fn main() {
    let root = "/home/chris/rusty_vessel";
    let page_size = 1000*60000;

    let mut temp = StreamDefinition::new("Temperature".to_string(), page_size, StreamKind::Source());
    let mut executor = Executor::new(temp.clone(), root.to_string(), 10000);

    let v = vec![0;100];

    let record = TemperatureRecord {
        temp: 1f64,
        altitude: 339.4
    };

    let last = executor.get_last_time();

    for (n, _) in v.iter().enumerate() {
      //  let stream_def = StreamDefinition::new(
      //      format!("alt_{}", n), page_size, StreamKind::Aggregate(Box::new(temp.clone())));

      //  executor.add(stream_def.clone());
    }

    for i in (last..last+(60000*100000000)).step_by(60000) {

        let blob = Blob::new(i, record.temp.clone());
        {
            executor.send_data(vec![blob]);
        }

        //time::sleep(Duration::from_millis(100)).await;
    }

    time::sleep(Duration::from_secs(100000));
}