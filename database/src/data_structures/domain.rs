use std::collections::HashMap;
use std::rc::Rc;
use crate::{Blob, Stream};

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct StreamDefinition {
    pub name: String,
    pub page_size: usize,
}

impl StreamDefinition {
    pub fn new(name: String, page_size: usize) -> StreamDefinition {
        return StreamDefinition {
            name,
            page_size
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

pub struct Graph {
    nodes: Vec<Node>,
    indexes: HashMap<StreamDefinition, usize>
}

impl Graph {
    pub fn new() -> Graph{
        return Graph {
            nodes: vec![],
            indexes: HashMap::new()
        }
    }

    pub fn add(&mut self, stream_definition: StreamDefinition, stream: Box<dyn Stream>) {
        let node = Node::new(stream_definition.clone(), stream);
        self.indexes.insert(stream_definition, self.nodes.len());
        self.nodes.push(node)
    }

    pub fn subscribe(&mut self, source: StreamDefinition, target: StreamDefinition) {
        let source_idx = self.indexes.get(&source).unwrap();
        let mut source_node = &mut self.nodes[source_idx.clone()];

        let target_idx = self.indexes.get_mut(&target).unwrap();

        source_node.children.push(target_idx.clone());
    }

    pub fn get_stream(&mut self, key: StreamDefinition) -> &mut Box<dyn Stream>{
        let idx = self.indexes.get(&key).unwrap();
        let node = &mut self.nodes;

        return &mut node[*idx].stream;
    }

    pub fn visit<F>(&mut self, source: StreamDefinition, data: Rc<Vec<Blob>>, visitor: F)
            where F : FnMut(&mut Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        let idx = self.indexes.get(&source).unwrap();
        self.visit_from(*idx, data, visitor);
    }

    pub fn visit_all<F>(&mut self, data: Rc<Vec<Blob>>, visitor: F)
            where F : FnMut(&mut Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        self.visit_from(0, data, visitor);
    }

    pub fn visit_from<F>(&mut self, idx: usize, data: Rc<Vec<Blob>>, mut visitor: F)
        where F : FnMut(&mut Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {

        let mut results = Vec::new();
        results.push((idx, data));

        while let Some((idx, input)) = results.pop() {
            let node = &mut self.nodes[idx];
            let output = visitor(&mut node.stream, input);

            for child in &node.children {
                results.push((*child, output.clone()))
            }
        }
    }
}

pub enum Envelope {
    Subscribe(StreamDefinition, StreamDefinition),
    Flush(),
    Data(Vec<Blob>)
}
