use std::hash::{Hash, Hasher};
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use crossbeam::channel::Sender;
use crate::{Blob, StreamRef, Vessel};
use crate::data_structures::domain::{Envelope, Node, StreamDefinition};
use crate::data_structures::graph::Graph;
use crate::domain::UnixTime;
use crate::streaming::streams::stream::{create_stream, Stream};

pub struct Executor {
    root: String,
    thread: thread::JoinHandle<()>,
    stream: Sender<Envelope>,
    last_time: Arc<Mutex<Option<UnixTime>>>
}

impl Executor {
    pub fn new(root: StreamRef, roots: Vec<StreamRef>, dir_path: String, buf_size: usize) -> Executor {
        let (sender, receiver) =
            crossbeam::channel::bounded::<Envelope>(buf_size);

        let local_dir_path = dir_path.clone();
        let last_time = Arc::new(Mutex::new(None));
        let last_clone = last_time.clone();

        let thread = std::thread::spawn(move || {
            let mut graph = Graph::new(root);
            let mut last_var = None;

            for root in roots.clone() {
                let (root_stream, last) = Self::create_stream(
                    local_dir_path.clone().as_str(), root);

                graph.add(root, root_stream);
                {
                    if last_var.is_none() {
                        last_var = Some(last);
                    }
                    else {
                        if last < last_var.unwrap() {
                            last_var = Some(last);
                        }
                    }
                }
            }

            {
                let mut locked = last_time.lock().unwrap();
                locked.replace(last_var.unwrap());
            }


            loop {
                let msg = receiver.recv().unwrap();

                match msg {
                    Envelope::Add(sources, target) => {
                        let (target_stream, last) = Self::create_stream(
                            local_dir_path.clone().as_str(), target.clone());

                        graph.add(target, target_stream);

                        for source in sources {
                            graph.subscribe(source, target);

                            let source_stream = &mut graph.get_stream(source);
                            let it = source_stream.replay(last);

                            for (_, batch) in it.enumerate() {
                                graph.visit_from(
                                    target,
                                    Rc::new(batch),
                                    |source, target, input| target.on_next(source, input));
                            }
                        }
                    }
                    Envelope::Flush() => {
                        for root in &roots {
                            graph.visit_from(*root, Rc::new(vec![]), |source, target, input| {
                                target.flush();
                                return input;
                            });
                        }
                    },
                    Envelope::Data(stream, data) => {
                        let rc_data = Rc::new(data);

                        graph.visit_from(stream, rc_data, |source, target,  input| {
                            return target.on_next(source,input);
                        })
                    }
                }
            }
        });

        let executor = Executor {
            root: dir_path.clone(),
            thread,
            stream: sender.clone(),
            last_time: last_clone,
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

    pub fn send_data(&self, source: StreamRef, data: Vec<Blob>) {
        self.stream
            .send(Envelope::Data(source, data))
            .unwrap();
    }

    pub fn add(&self, source: Vec<StreamRef>, target: StreamRef) {
        self.stream
            .send(Envelope::Add(source, target))
            .unwrap();
    }

    fn create_stream(root: &str, def: StreamRef) -> (Box<dyn Stream>, UnixTime) {
        let path = Path::new(root).join(&def.topic).join(&def.name);

        let vessel = Vessel::new(
            path,
            def.page_size as i64);

        let last_time = &vessel.get_last_time();
        let stream = create_stream(def, vessel);

        return (stream, last_time.clone());
    }
}

