use std::collections::HashMap;
use std::rc::Rc;
use crate::data_structures::domain::Node;
use crate::{Blob, StreamDefinition, StreamRef};
use crate::streaming::streams::stream::Stream;

pub struct Graph {
    nodes: HashMap<StreamRef, Node>,
    buf: Vec<(StreamRef, StreamRef, Rc<Vec<Blob>>)>,
    root: StreamRef
}

impl Graph {
    pub fn new(root: StreamRef) -> Graph{
        return Graph {
            nodes: HashMap::new(),
            buf: Vec::new(),
            root,
        }
    }

    pub fn add(&mut self, stream_definition: StreamRef, stream: Box<dyn Stream>) {
        let node = Node::new(stream_definition, stream);
        self.nodes.insert(stream_definition, node);
    }

    pub fn subscribe(&mut self, source: StreamRef, target: StreamRef) {
        let mut source_node = self.nodes.get_mut(&source).unwrap();
        source_node.children.push(target);
    }

    pub fn get_stream(&mut self, key: StreamRef) -> &mut Box<dyn Stream>{
        let mut node = self.nodes.get_mut(&key).unwrap();
        return &mut node.stream;
    }

    pub fn visit<F>(&mut self, source: StreamRef, data: Rc<Vec<Blob>>, mut visitor: F)
        where F : FnMut(StreamRef, &mut Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        let idx = self.nodes.get(&source).unwrap();

        self.visit_from(
            idx.defn,
            data,
            visitor);
    }

    pub fn visit_from<F>(&mut self, def: StreamRef, data: Rc<Vec<Blob>>, mut visitor: F)
        where F : FnMut(StreamRef, &mut Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {

        let buf = &mut self.buf;
        buf.push((self.root, def, data));

        while let Some((source_stream, target_stream, input_data)) = buf.pop() {
            let mut node = self.nodes.get_mut(&target_stream).unwrap();
            let output_stream = &mut node.stream;
            let output = visitor(source_stream, output_stream, input_data);

            for child in &node.children {
                buf.push((target_stream, *child, output.clone()))
            }
        }

        buf.clear();
    }
}
