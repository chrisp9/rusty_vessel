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
use crate::data_structures::domain::{StreamDefinition, StreamKind};
use crate::data_structures::executor::{Executor};
use crate::domain::UnixTime;
use crate::storage::domain::blob::Blob;
use crate::streaming::domain::{Calc, Stream};
use crate::threading::ArcRead;

mod threading;
mod storage;
mod streaming;
mod domain;
mod data_structures;

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

    let (open, high, low, close) = create_ohlc_topic(
        "BTC".to_string(),
        "1min".to_string(),
        page_size);

    let roots = vec![open.clone(), high.clone(), low.clone(), close.clone()];

    let mut executor = Executor::new(
        roots.clone(),
        root.to_string(),
        10000);

    let last = executor.get_last_time();

    for timestamp in (last..last+(60000*100000000)).step_by(60000) {

        let candle = create_ohlc(timestamp);

        executor.send_data(0, vec![Blob::new(timestamp, candle.open)]);
        executor.send_data(1, vec![Blob::new(timestamp, candle.high)]);
        executor.send_data(2, vec![Blob::new(timestamp, candle.low)]);
        executor.send_data(3, vec![Blob::new(timestamp, candle.close)]);
    }

    time::sleep(Duration::from_secs(100000));
}

fn create_ohlc_topic(symbol: String, name: String, page_size: usize) -> (
    StreamDefinition, StreamDefinition, StreamDefinition, StreamDefinition) {
    let open_stream_btc = StreamDefinition::new(
        symbol.clone(),
        format!("{name}_open").to_string(),
        page_size,
        StreamKind::Source()
    );

    let high_stream_btc = StreamDefinition::new(
        symbol.clone(),
        format!("{name}_high").to_string(),
        page_size,
        StreamKind::Source()
    );

    let low_stream_btc = StreamDefinition::new(
        symbol.clone(),
        format!("{name}_low").to_string(),
        page_size,
        StreamKind::Source()
    );

    let close_stream_btc = StreamDefinition::new(
        symbol,
        format!("{name}_close").to_string(),
        page_size,
        StreamKind::Source()
    );

    return (open_stream_btc, high_stream_btc, low_stream_btc, close_stream_btc);
}

pub fn create_ohlc(timestamp: UnixTime) -> Ohlc {
    return Ohlc {
        timestamp,
        open: (timestamp % 100) as f64,
        high: (timestamp % 50) as f64,
        low: (timestamp % 25) as f64,
        close: (timestamp % 12) as f64
    };
}