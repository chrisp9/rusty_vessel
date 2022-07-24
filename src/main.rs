#![feature(map_first_last)]
#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

use std::collections::HashMap;
use std::fs;
use std::fs::{DirEntry, File, OpenOptions, read, read_dir};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time;
use crate::storage::vessel2::Vessel;
use bincode::{Encode,Decode};
use chrono::{NaiveDateTime, Utc};
use crate::storage::bucket_issuer::UnixTime;
use crate::storage::domain::blob::Blob;
use crate::streaming::domain::StreamMsg;
use crate::streaming::persistent_stream::{PersistentStream};
use crate::streaming::domain::{Stream};
use crate::threading::ArcRead;

mod threading;
mod storage;
mod streaming;

static CHUNK_SIZE:i32 = 1000;

pub struct TemperatureRecord {
    pub temp: f64,
    pub altitude: f64
}

fn new_stream(root: &str, name: &str, stride: chrono::Duration) -> PersistentStream {
    let mut vessel = Vessel::new(
        root, name, stride);

    let stream = PersistentStream::new(vessel);
    return stream;
}

#[tokio::main]
async fn main() {
    let root = "/home/chris/rusty_vessel";

    let mut temp = new_stream(
        root, "Temperature", chrono::Duration::minutes(1));

    let mut alt = new_stream(
        root, "alt", chrono::Duration::minutes(1));

    let alt : ArcRead<Box<(dyn Stream + Send + Sync)>> = ArcRead::new(Box::new(alt));
    temp.subscribe(alt.clone());

    let record = TemperatureRecord {
        temp: 1f64,
        altitude: 339.4
    };

    let last = temp.get_last_tick_time();

    for i in (last..last+(60000*10000000)).step_by(60000) {

        let blob = Blob::new(i, record.temp.clone());
        {
            let data = StreamMsg::Delta(blob);
            temp.on_next(data);
        }

        time::sleep(Duration::from_millis(100)).await;
    }

    time::sleep(Duration::from_secs(100000)).await;
}
