use std::rc::Rc;
use crate::{Blob, StreamDefinition, StreamRef, Vessel};
use crate::domain::UnixTime;
use crate::streaming::streams::stream::Stream;

pub struct BasicStream {
    pub stream_def: StreamRef,
    vessel: Vessel
}

impl BasicStream {
    pub fn new(stream_def: StreamRef, vessel: Vessel) -> BasicStream {
        return BasicStream {
            stream_def,
            vessel
        }
    }
}

impl Stream for BasicStream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>> {
        return Box::new(self.vessel.read_from(since));
    }

    fn flush(&mut self) {
        self.vessel.flush();
    }

    fn on_next(&mut self, source: StreamRef, record: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        self.vessel.write(record.clone());
        return record;
    }
}
