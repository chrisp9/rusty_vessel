use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::thread;
use std::time::Duration;
use crossbeam::channel::Sender;
use crate::{Blob, Stream, StreamMsg, Vessel};
use crate::streaming::persistent_stream::PersistentStream;

#[derive(Clone)]
pub struct StreamDefinition {
    pub id: usize,
    pub name: String,
    pub page_size: usize
}

impl StreamDefinition {
    pub fn new(id: usize, name: String, page_size: usize) -> StreamDefinition {
        return StreamDefinition {
            id,
            name,
            page_size
        };
    }
}

pub struct Node {
    defn: StreamDefinition,
    stream: Box<dyn Stream>,
    subscribers: Rc<Vec<StreamDefinition>
}

impl Node {
    pub fn new(defn: StreamDefinition, stream: Box<dyn Stream>) -> Node {
        return Node {
            defn,
            stream,
            subscribers: Rc<Vec::<StreamDefinition>::new()
        };
    }
}

pub enum Envelope {
    Subscribe(StreamDefinition, StreamDefinition),
    Flush(),
    Data(Rc<Vec<Blob>)
}

pub struct Executor {
    root: String,
    thread: std::thread::JoinHandle<()>,
    stream: Sender<Envelope>,
}

impl Executor {
    pub fn new(root: StreamDefinition, dir_path: String, buf_size: usize) -> Executor {
        let (sender, receiver) =
            crossbeam::channel::bounded::<Envelope>(buf_size);

        let mut nodes = vec![];

        let thread = std::thread::spawn(move || {
            let local_dir_path = dir_path.clone();

            let root_stream = Self::create_stream(
                local_dir_path.as_str(), root.clone());

            let node = Node::new(root, root_stream);
            nodes.push(node);

            loop {
                let msg = receiver.recv().unwrap();

                match msg {
                    Envelope::Subscribe(source, target) => {
                        let stream = Self::create_stream(
                            local_dir_path.as_str(), target.clone());

                        let mut node = Node::new(target.clone(), stream);
                        let source_node = &mut nodes[source.id];
                        let mut source = &source_node.subscribers;

                        source.push(target);
                        nodes.push(node);

                        let mut it = &mut *source_node.stream.replay();

                        for (_, batch) in it.enumerate() {
                            Self::dispatch()
                        }
                    },
                    Envelope::Flush() => {
                        for node in nodes {
                            node.stream.flush()
                        }
                    }
                    Envelope::Data(data) => {
                        Self::dispatch(&nodes, root.clone(), data);
                    }
                }
            }
        });

        let executor = Executor {
            root: dir_path.clone(),
            thread,
            stream: sender.clone()
        };

        thread::spawn(move|| {
            sender.send(Envelope::Flush()).unwrap();
            thread::sleep(Duration::from_secs(5));
        });

        return executor;
    }

    pub fn dispatch(nodes: &Vec<Node>, root: StreamDefinition, data: Rc<Vec<Blob>>) {
        let mut results = vec![];
        results.push((root, data));

        while let Some((stream_def, input)) = results.pop() {
            let node = &nodes[stream_def.id];
            let output = node.stream.on_next(input);
            results.push((node.defn.clone(), output))
        }
    }

    pub fn new_node(&self, root: &str, defn: StreamDefinition) -> Node {
        return Node::new(defn.clone(), Self::create_stream(root, defn));
    }

    pub fn send_data(&self, data: Rc<Vec<Blob>>) {
        self.stream.send(Envelope::Data(data)).unwrap();
    }

    pub fn subscribe(&self, source: StreamDefinition, target: StreamDefinition) {
        self.stream.send(Envelope::Subscribe(source, target)).unwrap();
    }

    fn create_stream(root: &str, def: StreamDefinition) -> Box<dyn Stream> {
        let vessel = Vessel::new(
            root,
            def.name.as_str(),
            def.page_size as i64);

        let stream = PersistentStream::new(def.clone(), vessel);

        return Box::new(stream);
    }
}

