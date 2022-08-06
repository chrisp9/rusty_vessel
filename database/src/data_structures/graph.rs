use std::collections::HashMap;
use std::rc::Rc;
use crate::data_structures::domain::Node;
use crate::{Blob, Stream, StreamDefinition};

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

    pub fn get_stream(&mut self, key: &StreamDefinition) -> &mut Box<dyn Stream>{
        let idx = self.indexes.get(key).unwrap();
        let node = &mut self.nodes;

        return &mut node[*idx].stream;
    }

    pub fn visit<F>(&mut self, source: StreamDefinition, data: Rc<Vec<Blob>>, visitor: F)
        where F : FnMut(&Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        let idx = self.indexes.get(&source).unwrap();
        self.visit_from(*idx, data, visitor);
    }

    pub fn visit_all<F>(&mut self, data: Rc<Vec<Blob>>, visitor: F)
        where F : FnMut(&Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {
        self.visit_from(0, data, visitor);
    }

    pub fn visit_from<F>(&mut self, idx: usize, data: Rc<Vec<Blob>>, visitor: F)
        where F : FnMut(&Box<dyn Stream>, Rc<Vec<Blob>>) -> Rc<Vec<Blob>> {

        let mut results = Vec::new();
        results.push((idx, data));

        while let Some((idx, input)) = results.pop() {
            let node = &self.nodes[idx];
            let output = visitor(&node.stream, input);

            for child in &node.children {
                results.push((*child, output.clone()))
            }
        }
    }
}
