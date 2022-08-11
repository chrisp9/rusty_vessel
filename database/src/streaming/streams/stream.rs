use std::rc::Rc;
use crate::{Blob, StreamDefinition, StreamKind, StreamRef, UnixTime, Vessel};
use crate::streaming::streams::aggregate_stream::AggregateStream;
use crate::streaming::streams::basic_stream::BasicStream;
use crate::streaming::streams::merged_stream::MergedStream;

pub trait Stream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>>;
    fn flush(&mut self);
    fn on_next(&mut self, source: StreamRef, batch: Rc<Vec<Blob>>) -> Rc<Vec<Blob>>;
}

pub fn create_stream(stream_def: StreamRef, vessel: Vessel) -> Box<dyn Stream> {
    let kind = &stream_def.stream_kind;

    return match kind {
        StreamKind::Source() => Box::new(BasicStream::new(stream_def, vessel)),

        StreamKind::Aggregate(calc, size, interval) => Box::new(
            AggregateStream::new(stream_def, calc.clone(), vessel, *size, *interval)),

        StreamKind::Merge(kind) => {
            return Box::new(MergedStream::new(stream_def, kind.clone(),  vessel))
        }
    };
}

