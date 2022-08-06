use std::collections::HashMap;
use std::iter;
use std::option::Iter;
use std::rc::Rc;
use crate::{Blob, Stream};
use crate::domain::UnixTime;

#[derive(Clone, Eq, Hash, PartialEq)]
pub enum StreamKind {
    Source(),
    Aggregate(Box<StreamDefinition>, UnixTime),
    Merge(Vec<StreamDefinition>)
}

impl StreamKind {
    pub fn iter(&self) -> Box<dyn Iterator<Item=&StreamDefinition> + '_> {
        return match self {
            StreamKind::Source() => Box::new(iter::empty::<&StreamDefinition>()),
            StreamKind::Aggregate(v, _) => Box::new(iter::once::<&StreamDefinition>(v.as_ref())),
            StreamKind::Merge(v) => Box::new(v.iter())
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct StreamDefinition {
    pub name: String,
    pub page_size: usize,
    pub stream_kind: StreamKind
}

impl StreamDefinition {
    pub fn new(name: String, page_size: usize, stream_kind: StreamKind) -> StreamDefinition {
        return StreamDefinition {
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
    Data(Vec<Blob>)
}
