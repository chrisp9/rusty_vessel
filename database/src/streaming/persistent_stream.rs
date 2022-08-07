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
use crate::streaming::streams::aggregate_stream::AggregateStream;
use crate::streaming::streams::basic_stream::BasicStream;

pub fn create_stream(stream_def: StreamDefinition, vessel: Vessel) -> Box<dyn Stream> {
    return match stream_def.stream_kind {
        StreamKind::Source() => Box::new(BasicStream::new(stream_def, vessel)),
        StreamKind::Aggregate(_, ref calc, size, interval) => Box::new(
            AggregateStream::new(stream_def.clone(), calc.clone(), vessel, size, interval)),
        StreamKind::Merge(_) => Box::new(BasicStream::new(stream_def, vessel))
    }
}
