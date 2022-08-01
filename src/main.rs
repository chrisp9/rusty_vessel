#![feature(map_first_last)]
#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

extern crate core;

use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions, read, read_dir};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use crate::storage::vessel2::Vessel;
use bincode::{Encode,Decode};
use chrono::{NaiveDateTime, Utc};
use crate::data_structures::executor::{Executor, StreamDefinition};
use crate::storage::domain::blob::Blob;
use crate::streaming::domain::StreamMsg;
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

    let root_def = StreamDefinition::new(-1, "Root".to_string(), 0);
    let mut executor = Executor::new(root_def.clone(), root.to_string(), 10000);
    let mut i = 0;

    let mut temp = StreamDefinition::new(i, "Temperature".to_string(), 100000 * 60000);
    executor.subscribe(root_def.clone(), temp.clone());

    let v = vec![0;10];

    for (n, _) in v.iter().enumerate() {
        i += 1;
        let stream_def = StreamDefinition::new(i, format!("alt_{}", n), 100000 * 60000);
        executor.subscribe(temp.clone(), stream_def.clone());
    }

    let record = TemperatureRecord {
        temp: 1f64,
        altitude: 339.4
    };

    let last = 0;

    for i in (last..last+(60000*100000000)).step_by(60000) {

        let blob = Blob::new(i, record.temp.clone());
        {
            let data = StreamMsg::Tick(blob);
            executor.send_data(temp.clone(), vec![blob]);
        }

        //time::sleep(Duration::from_millis(100)).await;
    }

    time::sleep(Duration::from_secs(100000));
}

