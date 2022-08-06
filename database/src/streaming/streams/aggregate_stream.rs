use std::rc::Rc;
use futures::sink::Buffer;
use crate::{Blob, Stream, StreamDefinition, Vessel};
use crate::domain::UnixTime;

use crate::streaming::domain::Aggregator;

pub struct AggregateStream {
    pub stream_def: StreamDefinition,
    vessel: Vessel,
    buf: Aggregator
}

impl AggregateStream {
    pub fn new(stream_def: StreamDefinition, vessel: Vessel, count: usize) -> AggregateStream {
        return AggregateStream {
            stream_def,
            vessel,
            buf: Aggregator::new(count as i64)
        }
    }
}

impl Stream for AggregateStream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>> {
        return Box::new(self.vessel.read_from(since));
    }

    fn flush(&mut self) {
        self.vessel.flush();
    }

    fn on_next(&mut self, record: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        self.buf.,
        self.vessel.write(record.clone());
        return record.clone();
    }
}
