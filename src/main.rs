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
use crate::storage::domain::blob::Blob;
use crate::streaming::domain::StreamMsg;
use crate::streaming::persistent_stream::{PersistentStream};
use crate::streaming::domain::{Stream};
use crate::threading::ArcRead;

mod threading;
mod storage;
mod streaming;
mod domain;

static CHUNK_SIZE:i32 = 1000;

pub struct TemperatureRecord {
    pub temp: f64,
    pub altitude: f64
}

fn new_stream(root: &str, name: &str, page_size: i64) -> PersistentStream {
    let mut vessel = Vessel::new(
        root, name, page_size);

    let stream = PersistentStream::new(vessel);
    return stream;
}

#[tokio::main]
async fn main() {
    let root = "/home/chris/rusty_vessel";

    let mut temp = new_stream(
        root, "Temperature", 10000 * 60000);

    let mut data = Vec::<Arc<Box<(dyn Stream + Send + Sync)>>>::new();

    let v = vec![0;10];

    for (n, _) in v.iter().enumerate() {
        let mut alt = new_stream(
            root,
            format!("alt_{}", n).as_str(),
            10000 * 60000);

        let alt : Arc<Box<(dyn Stream + Send + Sync)>> =
            Arc::new(Box::new(alt));

        data.push(alt.clone());
        temp.subscribe(alt.clone()).await;
    }

    let record = TemperatureRecord {
        temp: 1f64,
        altitude: 339.4
    };

    let last = temp.get_last_tick_time();

    for i in (last..last+(60000*100000000)).step_by(60000) {

        let blob = Blob::new(i, record.temp.clone());
        {
            let data = StreamMsg::Tick(blob);
            temp.on_next(data).await;
        }

        tokio::task::yield_now().await;

        //time::sleep(Duration::from_millis(100)).await;
    }

    time::sleep(Duration::from_secs(100000)).await;
}

