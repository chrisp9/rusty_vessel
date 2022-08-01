use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::thread;
use std::time::Duration;
use crossbeam::channel::Sender;
use crate::{Blob, Stream, StreamMsg, Vessel};
use crate::storage::vessel2::VesselIterator;
use crate::streaming::persistent_stream::{PersistentStream, RootStream};

#[derive(Clone)]
pub struct StreamDefinition {
    pub id: i16,
    pub name: String,
    pub page_size: usize
}

impl Hash for StreamDefinition {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq<Self> for StreamDefinition {
    fn eq(&self, other: &Self) -> bool {
        return self.id == other.id;
    }
}

impl Eq for StreamDefinition {

}

impl StreamDefinition {
    pub fn new(id: i16, name: String, page_size: usize) -> StreamDefinition {
        return StreamDefinition {
            id,
            name,
            page_size
        };
    }
}

pub struct SubscriberList {
    vessel: Vessel,
    subscribers: Vec<StreamDefinition>
}

impl SubscriberList {
    pub fn new(vessel: Vessel) -> SubscriberList {
        return SubscriberList {
            vessel,
            subscribers: vec![]
        };
    }
}

pub enum Envelope {
    Subscribe(StreamDefinition, StreamDefinition),
    Flush(),
    Data(StreamDefinition, Vec<Blob>)
}

pub struct Executor {
    root: String,
    thread: std::thread::JoinHandle<()>,
    stream: Sender<Envelope>,
}

impl Executor {
    pub fn new(root_stream_def: StreamDefinition, dir_path: String, buf_size: usize) -> Executor {
        let (sender, receiver) =
            crossbeam::channel::bounded::<Envelope>(buf_size);

        let path = dir_path.clone();

        let thread = std::thread::spawn(move || {
            let local_dir_path = dir_path.clone();
            let mut root = RootStream::new(root_stream_def, );

            loop {
                let msg = receiver.recv().unwrap();

                match msg {
                    Envelope::Subscribe(source, target) => {
                        let vessel = Self::create_stream(
                            local_dir_path.as_str(), target.clone());

                        root.subscribe(source, vessel);
                    },
                    Envelope::Flush() => {
                        root.on_next(Rc::new(StreamMsg::Flush()));
                    }
                    Envelope::Data(target, data) => {
                        let batch = Rc::new(StreamMsg::Batch(data));
                        root.on_next(batch);
                    }
                }
            }
        });

        let executor = Executor {
            root: path,
            thread,
            stream: sender.clone()
        };

        thread::spawn(move|| {
            sender.send(Envelope::Flush()).unwrap();
            thread::sleep(Duration::from_secs(5));
        });

        return executor;
    }

    pub fn send_data(&self, target: StreamDefinition, data: Vec<Blob>) {
        self.stream.send(Envelope::Data(target, data)).unwrap();
    }

    pub fn subscribe(&self, source: StreamDefinition, target: StreamDefinition) {
        self.stream.send(Envelope::Subscribe(source, target)).unwrap();
    }

    fn create_stream(root: &str, def: StreamDefinition) -> Rc<dyn Stream> {
        let vessel = Vessel::new(
            root,
            def.name.as_str(),
            def.page_size as i64);

        let stream = PersistentStream::new(def.clone(), vessel);

        return Rc::new(stream);
    }
}

