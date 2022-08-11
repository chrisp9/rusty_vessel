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
use crate::data_structures::domain::{MergeKind, StreamDefinition, StreamKind, StreamRef};
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

fn root_def() -> StreamRef {
    return StreamRef::new(leak(StreamDefinition::new(
        "root".to_string(),
        "root".to_string(),
        "root".to_string(),
        0,
        StreamKind::Source()
    )));
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

    let hlc = create_hlc3(
        "BTC".to_string(),
        "1min".to_string(),
        h.clone(),
        l.clone(),
        c.clone(),
        10000);

    let (o5, h5, l5, c5)
        = create_agg_ohlc_topic(o,h,l,c, "5min".to_string(), 10000);

    executor.add(vec![o], o5);
    executor.add(vec![h], h5);
    executor.add(vec![l], l5);
    executor.add(vec![c], c5);

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

fn create_agg_ohlc_topic( open: StreamRef, high: StreamRef, low: StreamRef, close: StreamRef, name: String, page_size: usize) ->
    (StreamRef, StreamRef, StreamRef, StreamRef) {

    let agg_open = StreamDefinition::new(
        open.topic.clone(),
        open.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            Calc::First, 5, 60000));

    let agg_high = StreamDefinition::new(
        high.topic.clone(),
        high.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            Calc::Max, 5, 60000));

    let agg_low = StreamDefinition::new(
        low.topic.clone(),
        low.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            Calc::Min, 5, 60000));

    let agg_close = StreamDefinition::new(
        close.topic.clone(),
        close.sub_topic.clone(),
        name.clone(),
        page_size,
        StreamKind::Aggregate(
            Calc::Last, 5, 60000));

    return (
        StreamRef::new(leak(agg_open)),
        StreamRef::new(leak(agg_high)),
        StreamRef::new(leak(agg_low)),
        StreamRef::new(leak(agg_close)));
}

fn create_hlc3(symbol: String,
               name: String,
               high: StreamRef,
               low: StreamRef,
               close: StreamRef, page_size: usize) -> StreamRef {

    let open_stream = StreamDefinition::new(
        symbol.clone(),
        "hlc3".to_string(),
        name.clone(),
        page_size,
        StreamKind::Merge(MergeKind::Hlc3 {
            high,
            low,
            close
        })
    );

    return StreamRef::new(leak(open_stream));
}

fn create_ohlc_topic(symbol: String, name: String, page_size: usize) -> (
    StreamRef, StreamRef, StreamRef, StreamRef) {

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

    return (
        StreamRef::new(leak(open_stream)),
        StreamRef::new(leak(high_stream)),
        StreamRef::new(leak(low_stream)),
        StreamRef::new(leak(close_stream)));
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