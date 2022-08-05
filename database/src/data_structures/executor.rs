use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use crossbeam::channel::Sender;
use crate::{Blob, Stream, Vessel};
use crate::data_structures::domain::{Envelope, Graph, Node, StreamDefinition};
use crate::domain::UnixTime;
use crate::streaming::persistent_stream::PersistentStream;

pub struct Executor {
    root: String,
    thread: thread::JoinHandle<()>,
    stream: Sender<Envelope>,
    last_time: Arc<Mutex<Option<UnixTime>>>
}

impl Executor {
    pub fn new(root: StreamDefinition, dir_path: String, buf_size: usize) -> Executor {
        let (sender, receiver) =
            crossbeam::channel::bounded::<Envelope>(buf_size);

        let local_dir_path = dir_path.clone();
        let last_time = Arc::new(Mutex::new(None));
        let last_time_ref = last_time.clone();

        let thread = std::thread::spawn(move || {
            let mut graph = Graph::new();

            let (root_stream, last) = Self::create_stream(
                local_dir_path.clone().as_str(), root.clone());

            graph.add(root, root_stream);
            {
                let locked = last_time_ref.lock();
                locked.unwrap().replace(last);
            }

            loop {
                let msg = receiver.recv().unwrap();

                match msg {
                    Envelope::Subscribe(source, target) => {
                        let (stream, last) = Self::create_stream(
                            local_dir_path.clone().as_str(), target.clone());

                        graph.add(target.clone(), stream);
                        graph.subscribe(source.clone(), target.clone());

                        let source =&mut graph.get_stream(source);
                        let it = source.replay(last);

                        for (_, batch) in it.enumerate() {
                            graph.visit(
                                target.clone(),
                                Rc::new(batch),
                                |stream, input| stream.on_next(input));
                        }
                    },
                    Envelope::Flush() => {
                        graph.visit_all(Rc::new(vec![]), |stream, v| {
                            stream.flush();
                            return v;
                        })
                    },
                    Envelope::Data(data) => {
                        let rc_data = Rc::new(data);

                        graph.visit_all(rc_data, |stream, input| {
                            return stream.on_next(input);
                        })
                    }
                }
            }
        });

        let executor = Executor {
            root: dir_path.clone(),
            thread,
            stream: sender.clone(),
            last_time: last_time.clone(),
        };

        thread::spawn(move|| {
            sender.send(Envelope::Flush()).unwrap();
            thread::sleep(Duration::from_secs(5));
        });

        return executor;
    }

    pub fn get_last_time(&self) -> UnixTime {
        loop {
            let locked = self.last_time.lock();
            let x = &*locked.unwrap();

            if x.is_some() {
                return x.unwrap();
            }
        }
    }

    pub fn send_data(&self, data: Vec<Blob>) {
        self.stream.send(Envelope::Data(data)).unwrap();
    }

    pub fn subscribe(&self, source: StreamDefinition, target: StreamDefinition) {
        self.stream.send(Envelope::Subscribe(source, target)).unwrap();
    }

    fn create_stream(root: &str, def: StreamDefinition) -> (Box<dyn Stream>, UnixTime) {
        let vessel = Vessel::new(
            root,
            def.name.as_str(),
            def.page_size as i64);

        let last_time = &vessel.get_last_time();

        let stream = PersistentStream::new(def.clone(), vessel);

        return (Box::new(stream), last_time.clone());
    }
}

