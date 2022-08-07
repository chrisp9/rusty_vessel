use std::rc::Rc;
use crate::{Blob, StreamDefinition, StreamKind, UnixTime, Vessel};
use crate::streaming::streams::aggregate_stream::AggregateStream;
use crate::streaming::streams::basic_stream::BasicStream;

pub trait Stream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>>;
    fn flush(&mut self);
    fn on_next(&mut self, batch: Rc<Vec<Blob>>) -> Rc<Vec<Blob>>;
}

pub fn create_stream(stream_def: &'static StreamDefinition, vessel: Vessel) -> Box<dyn Stream> {
    return match stream_def.stream_kind {
        StreamKind::Source() => Box::new(BasicStream::new(stream_def, vessel)),
        StreamKind::Aggregate(_, ref calc, size, interval) => Box::new(
            AggregateStream::new(stream_def, calc.clone(), vessel, size, interval)),
        StreamKind::Merge(_) => Box::new(BasicStream::new(stream_def, vessel))
    }
}

