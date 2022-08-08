use std::collections::HashMap;
use std::rc::Rc;
use crate::{Blob, StreamDefinition, UnixTime, Vessel};
use crate::streaming::streams::stream::Stream;

pub struct MergedStream<F> where F:FnMut(HashMap<StreamDefinition, Blob>) -> Blob {
    pub stream_def: &'static StreamDefinition,
    pub sources: Vec<&'static StreamDefinition>,
    pub merge_func: F,
    vessel: Vessel
}

impl<F> MergedStream<F> where F:FnMut(HashMap<StreamDefinition, Blob>) -> Blob {
    pub fn new(
        stream_def: &'static StreamDefinition,
        sources: Vec<&'static StreamDefinition>,
        merge_func: F,
        vessel: Vessel) -> MergedStream<F> {

        return MergedStream {
            stream_def,
            sources,
            merge_func,
            vessel
        }
    }
}

impl<F> Stream for MergedStream<F> where F:FnMut(HashMap<StreamDefinition, Blob>) -> Blob {
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
