use std::rc::Rc;
use crate::{Blob, Stream, StreamDefinition, Vessel};
use crate::domain::UnixTime;

pub struct BasicStream {
    pub stream_def: StreamDefinition,
    vessel: Vessel
}

impl BasicStream {
    pub fn new(stream_def: StreamDefinition, vessel: Vessel) -> BasicStream {
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

    fn on_next(&mut self, record: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        self.vessel.write(record.clone());
        return record.clone();
    }
}
