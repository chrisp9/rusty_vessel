use std::collections::HashMap;
use std::rc::Rc;
use crate::{Blob, StreamDefinition, StreamRef, UnixTime, Vessel};
use crate::streaming::streams::stream::Stream;

pub struct StreamBuffer {
    pub items: HashMap<UnixTime, HashMap<StreamRef, Blob>>,
    pub count: usize
}

impl StreamBuffer {
    pub fn new(count: usize) -> StreamBuffer {
        return StreamBuffer {
            items: HashMap::new(),
            count
        };
    }

    pub fn add(&mut self, stream: StreamRef, data: Blob) -> Option<HashMap<StreamRef, Blob>>{
        let mut items = &mut self.items;

        let mut streams_for_tick = items
            .entry(data.timestamp)
            .or_insert(HashMap::new());

        streams_for_tick
            .insert(stream, data).unwrap();

        if streams_for_tick.len() >= self.count {
            let result = streams_for_tick.clone();
            items.remove(&data.timestamp);
            return Some(result);
        }

        return None;
    }
}


pub struct MergedStream<F> where F:FnMut(HashMap<StreamRef, Blob>) -> Blob {
    pub stream_def: StreamRef,
    pub sources: StreamBuffer,
    pub merge_func: F,
    vessel: Vessel
}

impl<F> MergedStream<F> where F:FnMut(HashMap<StreamRef, Blob>) -> Blob {
    pub fn new(
        stream_def: StreamRef,
        sources: Vec<StreamRef>,
        merge_func: F,
        vessel: Vessel) -> MergedStream<F> {

        return MergedStream {
            stream_def,
            sources: StreamBuffer::new(sources.len()),
            merge_func,
            vessel
        }
    }
}

impl<F> Stream for MergedStream<F> where F:FnMut(HashMap<StreamRef, Blob>) -> Blob {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>> {
        return Box::new(self.vessel.read_from(since));
    }

    fn flush(&mut self) {
        self.vessel.flush();
    }

    fn on_next(&mut self, source: StreamRef, record: Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {

        for item in &*record {
            let merge_result = self.sources.add(source, item.clone());

            if let Some(result) = merge_result {
                (self.merge_func)(result);
            }
        }

        self.vessel.write(record.clone());
        return record.clone();
    }
}
