use std::{io, sync, thread};
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use futures::{FutureExt, StreamExt};
use tokio::runtime::Handle;
use crate::{Blob, StreamDefinition, Vessel};
use crate::threading::{ArcRead, ArcRw};
use crate::domain::UnixTime;
use crate::storage::domain::bucket::Bucket;
use crate::streaming::domain::{Stream, StreamMsg};
use async_trait::async_trait;
use crossbeam::channel::{Receiver, Sender};
use crate::storage::vessel2::VesselIterator;

pub trait Processor {
    fn process(input: Blob) -> Option<Blob>;
}

pub struct BufferBucket {
    buf: Vec<Blob>,
    bucket: Bucket,
}

impl BufferBucket {
    pub fn new(bucket: Bucket) -> BufferBucket {
        return BufferBucket { buf: Vec::new(), bucket };
    }
}

pub struct StaticWindowProcessor {
    buffer: BufferBucket,
}

impl StaticWindowProcessor {
    pub fn new(window_size: chrono::Duration) -> StaticWindowProcessor {
        let bucket = Bucket::epoch(window_size.num_milliseconds());
        let buffer = BufferBucket::new(bucket);

        return StaticWindowProcessor {
            buffer
        };
    }
}

// impl Processor for StaticWindowProcessor {
//     fn process(&mut self, input: Blob) -> Option<Blob> {
//
//
//     }
// }

pub struct PersistentStream {
    pub stream_def: StreamDefinition,
    vessel: Vessel,
    subscribers: RefCell<Vec<Rc<dyn Stream>>>,
}

impl PersistentStream {
    pub fn new(stream_def: StreamDefinition, vessel: Vessel) -> PersistentStream {
        return PersistentStream {
            stream_def,
            vessel,
            subscribers: RefCell::new(vec![])
        }
    }
}

impl Stream for PersistentStream {
    fn subscribe(&self, parent: StreamDefinition, stream: Rc<dyn Stream>) {
        if self.stream_def == parent {
            let mut subs = self.subscribers.borrow_mut();
            subs.push(stream.clone());

            let it = self.vessel.read_from(self.vessel.get_last_time());

            for batch in it {
                let batch = StreamMsg::Batch(batch);
                stream.on_next(Rc::new(batch));
            }
        }
    }

    fn on_next(&self, record: Rc<StreamMsg>) {
        let subs = self.subscribers.borrow_mut();

        for subscriber in subs.iter() {
            subscriber.on_next(record.clone());
        }

        match record.borrow() {
            StreamMsg::Batch(ref b) => {
                self.vessel.write(b);
            },
            StreamMsg::Tick(b) => {
                let vector = vec![b.clone()];
                self.vessel.write(&vector);
            },
            StreamMsg::Flush() => {
                self.vessel.flush();
            }
        }

    }

    fn flush(&self) {
        self.vessel.flush();
    }
}


pub struct RootStream {
    pub stream_def: StreamDefinition,
    subscribers: RefCell<Vec<Rc<dyn Stream>>>
}

impl RootStream {
    pub fn new(
        stream_def: StreamDefinition) -> RootStream{

        return RootStream {
            stream_def,
            subscribers: RefCell::new(vec![])
        };

    }
}

impl Stream for RootStream {
    fn subscribe(&self, parent: StreamDefinition, stream: Rc<dyn Stream>) {
        if self.stream_def == parent {
            let mut mut_borrow = self.subscribers.borrow_mut();
            mut_borrow.push(stream.clone());
        }
        else {

            for mut subscriber in self.subscribers.borrow_mut().iter() {
                subscriber.subscribe(parent.clone(), stream.clone());
            }
        }
    }

    fn on_next(&self, msg: Rc<StreamMsg>) {
        for subscriber in self.subscribers.borrow_mut().iter() {
            subscriber.on_next(msg.clone());
        }
    }

    fn flush(&self) {
        for subscriber in self.subscribers.borrow_mut().iter() {
            subscriber.flush();
        }
    }
}

