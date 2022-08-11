use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::iter;
use std::ops::Deref;
use uuid::Uuid;
use crate::{Blob};
use crate::streaming::domain::Calc;
use crate::streaming::streams::stream::Stream;

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum MergeKind {
    Hlc3 { high: StreamRef, low: StreamRef, close: StreamRef }
}

impl MergeKind {
    pub fn get_func(&mut self, streams: HashMap<StreamRef, Blob>) -> Blob {
        return match self {
            MergeKind::Hlc3 { ref high, ref low, ref close } =>
                self.calc_hlc3(*high, *low, *close, streams)
        };
    }

    pub fn len(&self) -> usize {
        return match self {
            MergeKind::Hlc3 { .. } => 3
        }
    }

    pub fn calc_hlc3(&mut self, high: StreamRef, low: StreamRef, close: StreamRef, streams: HashMap<StreamRef, Blob>) -> Blob{
        let high_blob = streams.get(&high).unwrap();
        let low_blob = streams.get(&low).unwrap();
        let close_blob = streams.get(&close).unwrap();

        let timestamp = close_blob.timestamp;
        let hlc3 = (high_blob.data + low_blob.data + close_blob.data) / 3.0;

        return Blob::new(timestamp, hlc3);
    }
}


#[derive(Clone, Eq, Hash, PartialEq)]
pub enum StreamKind {
    Source(),
    Aggregate(Calc, usize, usize),
    Merge(MergeKind)
}

#[derive(Copy, Clone, Hash, Eq, PartialEq)]
pub struct StreamRef {
    refr:  &'static StreamDefinition
}

impl StreamRef {
    pub fn new(defn: &'static StreamDefinition) -> StreamRef {
        return StreamRef {
            refr: defn
        };
    }
}

impl Deref for StreamRef {
    type Target = StreamDefinition;

    fn deref(&self) -> &Self::Target {
        return &self.refr;
    }
}


#[derive(Eq)]
pub struct StreamDefinition {
    pub id: Uuid,
    pub topic: String,
    pub sub_topic: String,
    pub name: String,
    pub page_size: usize,
    pub stream_kind: StreamKind
}

impl Hash for StreamDefinition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.name.as_bytes())
    }
}

impl PartialEq for StreamDefinition {
    fn eq(&self, other: &Self) -> bool {
        return self.topic == other.topic && self.name == other.name;
    }
}

impl StreamDefinition {
    pub fn new(topic: String, sub_topic: String, name: String, page_size: usize, stream_kind: StreamKind) -> StreamDefinition {
        return StreamDefinition {
            id: Uuid::new_v4(),
            topic,
            sub_topic: sub_topic.clone(),
            name: format!("{sub_topic}_{name}"),
            page_size,
            stream_kind
        };
    }
}

pub struct Node {
    pub defn: StreamRef,
    pub stream: Box<dyn Stream>,
    pub children: Vec<StreamRef>
}

impl Node {
    pub fn new(defn: StreamRef, stream: Box<dyn Stream>) -> Node {
        return Node {
            defn,
            stream,
            children: Vec::new()
        };
    }
}

pub enum Envelope {
    Add(Vec<StreamRef>, StreamRef),
    Flush(),
    Data(StreamRef, Vec<Blob>)
}
