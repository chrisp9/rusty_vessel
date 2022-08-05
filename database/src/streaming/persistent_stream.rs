use std::{io, sync, thread};
use std::borrow::Borrow;
use std::borrow::{BorrowMut};
use std::cell::RefCell;
use std::rc::Rc;
use crate::{Blob, Vessel};
use crate::data_structures::domain::StreamDefinition;
use crate::domain::UnixTime;

use crate::storage::domain::bucket::Bucket;
use crate::streaming::domain::{BufferBucket, Stream};

pub trait Processor {
    fn process(input: Blob) -> Option<Blob>;
}

pub struct StaticWindowProcessor {
    buffer: BufferBucket,
}

impl StaticWindowProcessor {
    pub fn new(window_size: chrono::Duration) -> StaticWindowProcessor {
        let bucket = Bucket::epoch(window_size.num_milliseconds());
        let buffer = BufferBucket::new(bucket);

        return StaticWindowProcessor {
            buffer
        };
    }
}

// impl Processor for StaticWindowProcessor {
//     fn process(&mut self, input: Blob) -> Option<Blob> {
//
//
//     }
// }

pub struct PersistentStream {
    pub stream_def: StreamDefinition,
    vessel: Vessel
}

impl PersistentStream {
    pub fn new(stream_def: StreamDefinition, vessel: Vessel) -> PersistentStream {
        return PersistentStream {
            stream_def,
            vessel
        }
    }
}

impl Stream for PersistentStream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>> {
        return Box::new(self.vessel.read_from(since));
    }

    fn flush(&mut self) {
        self.vessel.flush();
    }

    fn on_next(&mut self, record: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        self.vessel.write(record.clone());
        return record.clone();
    }
}

