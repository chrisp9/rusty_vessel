use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::iter;
use std::option::Iter;
use std::rc::Rc;
use crate::{Blob, Stream};
use crate::domain::UnixTime;
use crate::streaming::domain::Calc;

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum StreamKind {
    Source(),
    Aggregate(Box<StreamDefinition>, Calc, usize, usize),
    Merge(Vec<StreamDefinition>)
}

impl StreamKind {
    pub fn iter(&self) -> Box<dyn Iterator<Item=&StreamDefinition> + '_> {
        return match self {
            StreamKind::Source() => Box::new(iter::empty::<&StreamDefinition>()),
            StreamKind::Aggregate(v, _, _, _) => Box::new(iter::once::<&StreamDefinition>(v.as_ref())),
            StreamKind::Merge(v) => Box::new(v.iter())
        }
    }
}

#[derive(Clone, Eq)]
pub struct StreamDefinition {
    pub topic: String,
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
    pub fn new(topic: String, name: String, page_size: usize, stream_kind: StreamKind) -> StreamDefinition {
        return StreamDefinition {
            topic,
            name,
            page_size,
            stream_kind
        };
    }
}

pub struct Node {
    pub defn: StreamDefinition,
    pub stream: Box<dyn Stream>,
    pub children: Vec<usize>
}

impl Node {
    pub fn new(defn: StreamDefinition, stream: Box<dyn Stream>) -> Node {
        return Node {
            defn,
            stream,
            children: Vec::new()
        };
    }
}

pub enum Envelope {
    Add(StreamDefinition),
    Flush(),
    Data(usize, Vec<Blob>)
}
