use std::rc::Rc;
use tokio::time::interval;
use crate::{Blob};
use crate::domain::UnixTime;
use crate::storage::domain::bucket::Bucket;

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum Calc {
    First,
    Max,
    Min,
    Last
}


pub struct Entry {
    item: Option<f64>,
    timestamp: UnixTime
}

pub struct Aggregator {
    calc: Calc,
    size: i64,
    max: i64,
    entry: Option<Entry>
}

impl Aggregator {
    pub fn new(calc: Calc, size: usize, interval: usize) -> Aggregator {
        let size = size as i64 * interval as i64;
        let max = size as i64 - interval as i64;

        return Aggregator { calc, size: (size as i64) * (interval as i64), max, entry: None};
    }

    pub fn add(&mut self, blob: Blob) -> Option<Blob> {
        let idx = blob.timestamp % self.size;

        if let None = self.entry {
            if idx != 0 { return None }
            self.entry = Some(Entry{item: None, timestamp: blob.timestamp})
        }

        let mut entry = self.entry.take().unwrap();

        match self.calc {
            Calc::First if idx == 0 => {
                entry.item = Some(blob.data)
            }
            Calc::Max => {
                match entry.item {
                    Some(existing) if blob.data > existing => entry.item = Some(blob.data),
                    None => entry.item = Some(blob.data),
                    _ => ()
                }
            }
            Calc::Min => {
                match entry.item {
                    Some(existing) if blob.data < existing => entry.item = Some(blob.data),
                    None => entry.item = Some(blob.data),
                    _ => ()
                }
            }
            Calc::Last if idx >= self.max => {
                entry.item = Some(blob.data);
            }
            _ => ()
        }

        if idx >= self.max {
            let record = Blob { timestamp: entry.timestamp, data: entry.item.unwrap() };
            return Some(record)
        }

        self.entry = Some(entry);
        return None;
    }
}