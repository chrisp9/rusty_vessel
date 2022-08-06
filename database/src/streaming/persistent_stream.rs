use std::{io, sync, thread};
use std::borrow::Borrow;
use std::borrow::{BorrowMut};
use std::cell::RefCell;
use std::rc::Rc;
use crate::{Blob, StreamKind, Vessel};
use crate::data_structures::domain::StreamDefinition;
use crate::domain::UnixTime;

use crate::storage::domain::bucket::Bucket;
use crate::streaming::domain::{Aggregator, Stream};
use crate::streaming::streams::basic_stream::BasicStream;

pub fn create_stream(stream_def: StreamDefinition, vessel: Vessel) -> Box<dyn Stream> {
    return match stream_def.stream_kind {
        StreamKind::Source() => Box::new(BasicStream::new(stream_def, vessel)),
        StreamKind::Aggregate(_, _) => {}
        StreamKind::Merge(_) => {}
    }
}



pub struct StaticWindowProcessor {
    buffer: Aggregator,
}

impl StaticWindowProcessor {
    pub fn new(window_size: chrono::Duration) -> StaticWindowProcessor {
        let bucket = Bucket::epoch(window_size.num_milliseconds());
        let buffer = Aggregator::new(bucket);

        return StaticWindowProcessor {
            buffer
        };
    }
}
