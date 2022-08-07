use std::hash::{Hash, Hasher};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use crossbeam::channel::Sender;
use crate::{Blob, Stream, Vessel};
use crate::data_structures::domain::{Envelope, Node, StreamDefinition};
use crate::data_structures::graph::Graph;
use crate::domain::UnixTime;
use crate::streaming::persistent_stream::{create_stream};

pub struct Executor {
    root: String,
    thread: thread::JoinHandle<()>,
    stream: Sender<Envelope>,
    last_time: Arc<Mutex<Option<UnixTime>>>
}

impl Executor {
    pub fn new(roots: Vec<StreamDefinition>, dir_path: String, buf_size: usize) -> Executor {
        let (sender, receiver) =
            crossbeam::channel::bounded::<Envelope>(buf_size);

        let local_dir_path = dir_path.clone();
        let last_time = Arc::new(Mutex::new(None));
        let last_time_ref = last_time.clone();

        let thread = std::thread::spawn(move || {
            let mut graph = Graph::new();

            for root in roots {
                let (root_stream, last) = Self::create_stream(
                    local_dir_path.clone().as_str(), root.clone());

                graph.add(root, root_stream);
                {
                    let locked = last_time_ref.lock();
                    locked.unwrap().replace(last);
                }
            }

            loop {
                let msg = receiver.recv().unwrap();

                match msg {
                    Envelope::Add(target) => {
                        let (target_stream, last) = Self::create_stream(
                            local_dir_path.clone().as_str(), target.clone());

                        graph.add(target.clone(), target_stream);

                        for source in target.stream_kind.iter() {
                            graph.subscribe(source.clone(), target.clone());

                            let source = &mut graph.get_stream(source);
                            let it = source.replay(last);

                            for (_, batch) in it.enumerate() {
                                graph.visit(
                                    target.clone(),
                                    Rc::new(batch),
                                    |stream, input| stream.on_next(input));
                            }
                        }
                    }
                    Envelope::Flush() => {
                        graph.visit_all(Rc::new(vec![]), |stream, v| {
                            stream.flush();
                            return v;
                        })
                    },
                    Envelope::Data(stream, data) => {
                        let rc_data = Rc::new(data);

                        graph.visit_from(stream, rc_data, |stream, input| {
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

    pub fn send_data(&self, source: usize, data: Vec<Blob>) {
        self.stream
            .send(Envelope::Data(source, data))
            .unwrap();
    }

    pub fn add(&self, source: StreamDefinition) {
        self.stream
            .send(Envelope::Add(source))
            .unwrap();
    }

    fn create_stream(root: &str, def: StreamDefinition) -> (Box<dyn Stream>, UnixTime) {
        let path = Path::new(root).join(&def.topic).join(&def.name);

        let vessel = Vessel::new(
            path,
            def.page_size as i64);

        let last_time = &vessel.get_last_time();
        let stream = create_stream(def, vessel);

        return (stream, last_time.clone());
    }
}

