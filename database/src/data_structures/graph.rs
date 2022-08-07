use std::collections::HashMap;
use std::rc::Rc;
use crate::data_structures::domain::Node;
use crate::{Blob, StreamDefinition};
use crate::streaming::streams::stream::Stream;

pub struct FastBuf {

}

pub struct Graph {
    nodes: Vec<Node>,
    indexes: HashMap<&'static StreamDefinition, usize>,
    buf: Vec<(usize, Rc<Vec<Blob>>)>
}

impl Graph {
    pub fn new() -> Graph{
        return Graph {
            nodes: vec![],
            indexes: HashMap::new(),
            buf: Vec::new()
        }
    }

    pub fn add(&mut self, stream_definition: &'static StreamDefinition, stream: Box<dyn Stream>) {
        let node = Node::new(stream_definition, stream);
        self.indexes.insert(stream_definition, self.nodes.len());
        self.nodes.push(node)
    }

    pub fn subscribe(&mut self, source: &'static StreamDefinition, target: &'static StreamDefinition) {
        let source_idx = self.indexes.get(&source).unwrap();
        let mut source_node = &mut self.nodes[source_idx.clone()];

        let target_idx = self.indexes.get_mut(&target).unwrap();

        source_node.children.push(target_idx.clone());
    }

    pub fn get_stream(&mut self, key: &'static StreamDefinition) -> &mut Box<dyn Stream>{
        let idx = self.indexes.get(key).unwrap();
        let node = &mut self.nodes;

        return &mut node[*idx].stream;
    }

    pub fn visit<F>(&mut self, source: &'static StreamDefinition, data: Rc<Vec<Blob>>, mut visitor: F)
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

        let buf = &mut self.buf;
        buf.push((idx, data));

        while let Some((idx, input)) = buf.pop() {
            let node = &mut self.nodes[idx];
            let stream = &mut node.stream;
            let output = visitor(stream, input);

            for child in &node.children {
                buf.push((*child, output.clone()))
            }
        }

        buf.clear();
    }
}
