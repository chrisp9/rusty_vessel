use core::panicking::panic;
use std::rc::Rc;
use crate::{Blob};
use crate::domain::UnixTime;
use crate::storage::domain::bucket::Bucket;

pub trait Stream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>>;
    fn flush(&mut self);
    fn on_next(&mut self, batch: Rc<Vec<Blob>>) -> Rc<Vec<Blob>>;
}

pub enum Calc {
    First,
    Max,
    Min,
    Last
}

pub struct Aggregator {
    item: Option<Blob>,
    calc: Calc,
    idx: usize,
    count: usize
}

impl Aggregator {
    pub fn new(calc: Calc, bucket_size: usize) -> Aggregator {
        return Aggregator { item: None, calc, idx: 0, count: bucket_size};
    }

    pub fn add(&mut self, blob: Blob) -> Option<Blob> {
        match self.calc {
            Calc::First if self.idx == 0 => {
                self.item = Some(blob)
            }
            Calc::Max => {
                match self.item {
                    Some(v) if blob.data > v.data => self.item = Some(blob),
                    None => self.item = Some(blob),
                    _ => ()
                }
            }
            Calc::Min => {
                match self.item {
                    Some(v) if blob.data < v.data => self.item = Some(blob),
                    None => self.item = Some(blob),
                    _ => ()
                }
            }
            Calc::Last if self.idx == self.count - 2 => {
                self.item = Some(blob);
            }
            _ => panic!("Unsupported Calculation")
        }

        return self.full_or_none();
    }

    pub fn full_or_none(&mut self) -> Option<Blob> {
        self.idx += 1;
        if self.idx <= self.count - 1 {
            return None;
        }

        self.idx = 0;
        let buf = self.item;
        self.item = None;
        return buf;
    }
}