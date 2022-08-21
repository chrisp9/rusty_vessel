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
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::time::Duration;
use tokio::time;
use crate::storage::vessel2::Vessel;
use bincode::{Encode,Decode};
use chrono::{NaiveDateTime, Utc};
use crate::data_structures::domain::{MergedStreamRef, StreamDefinition, StreamKind, StreamRef};
use crate::data_structures::executor::{Executor};
use crate::domain::UnixTime;
use crate::storage::domain::blob::Blob;
use crate::streaming::domain::{Calc};
use crate::threading::ArcRead;

mod threading;
mod storage;
mod streaming;
mod domain;
mod data_structures;
mod user_model;

static CHUNK_SIZE:i32 = 1000;

pub struct Ohlc {
    pub timestamp: UnixTime,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64
}

fn main() {
    let root = "/home/chris/rusty_vessel";
    let page_size = 1000*60000;

    let (o, h, l, c) = create_ohlc_topic(
        "BTC".to_string(),
        "1min".to_string(),
        page_size);

    let roots = vec![o.clone(), h.clone(), l.clone(), c.clone()];

    let mut executor = Executor::new(
        root_def(),
        roots.clone(),
        root.to_string(),
        10000);


    let last = executor.get_last_time();

    for timestamp in (last..last+(60000*100000000)).step_by(60000) {

        let candle = create_ohlc(timestamp);

        executor.send_data(o, vec![Blob::new(timestamp, candle.open)]);
        executor.send_data(h, vec![Blob::new(timestamp, candle.high)]);
        executor.send_data(l, vec![Blob::new(timestamp, candle.low)]);
        executor.send_data(c, vec![Blob::new(timestamp, candle.close)]);
    }

    let _ = time::sleep(Duration::from_secs(100000));
}

pub fn leak<T>(item: T) -> &'static T {
    let leak = Box::leak(Box::new(item));
    return leak;
}

pub fn create_ohlc(timestamp: UnixTime) -> Ohlc {
    return Ohlc {
        timestamp,
        open: (timestamp % 17) as f64,
        high: (timestamp % 23) as f64,
        low: (timestamp % 25) as f64,
        close: (timestamp % 12) as f64
    };
}