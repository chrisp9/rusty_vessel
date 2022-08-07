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
use crate::streaming::domain::{Calc};
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

    let mins_5_candles
        = create_agg_ohlc_topic(roots, "5min".to_string(), 10000);

   // executor.add(mins_5_candles[0]);
  //  executor.add(mins_5_candles[1]);

    for def in mins_5_candles {
        executor.add(def);
        //break;
    }

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

fn create_agg_ohlc_topic(candles: Vec<&'static StreamDefinition>, name: String, page_size: usize) ->
    Vec<&'static StreamDefinition> {

    let open = &candles[0];
    let high = &candles[1];
    let low = &candles[2];
    let close = &candles[3];

    let agg_open = StreamDefinition::new(
        open.topic.clone(),
        open.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            open, Calc::First, 5, 60000));

    let agg_high = StreamDefinition::new(
        high.topic.clone(),
        high.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            high, Calc::Max, 5, 60000));

    let agg_low = StreamDefinition::new(
        low.topic.clone(),
        low.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            low, Calc::Min, 5, 60000));

    let agg_close = StreamDefinition::new(
        close.topic.clone(),
        close.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            close, Calc::Last, 5, 60000));

    return vec!(leak(agg_open), leak(agg_high), leak(agg_low), leak(agg_close));
}

fn create_ohlc_topic(symbol: String, name: String, page_size: usize) -> (
    &'static StreamDefinition, &'static StreamDefinition, &'static StreamDefinition, &'static StreamDefinition) {

    let open_stream = StreamDefinition::new(
        symbol.clone(),
        "open".to_string(),
        name.clone(),
        page_size,
        StreamKind::Source()
    );

    let high_stream = StreamDefinition::new(
        symbol.clone(),
        "high".to_string(),
        name.clone(),
        page_size,
        StreamKind::Source()
    );

    let low_stream = StreamDefinition::new(
        symbol.clone(),
        "low".to_string(),
        name.clone(),
        page_size,
        StreamKind::Source()
    );

    let close_stream = StreamDefinition::new(
        symbol,
        "close".to_string(),
        name.clone(),
        page_size,
        StreamKind::Source()
    );

    return (leak(open_stream), leak(high_stream), leak(low_stream), leak(close_stream));
}

pub fn leak<T>(item: T) -> &'static T {
    let leak = Box::leak(Box::new(item));
    return leak;
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