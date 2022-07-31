use std::{io, sync, thread};
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use futures::{FutureExt, StreamExt};
use tokio::runtime::Handle;
use crate::{Blob, Vessel};
use crate::threading::{ArcRead, ArcRw};
use crate::domain::UnixTime;
use crate::storage::domain::bucket::Bucket;
use crate::streaming::domain::{Stream, StreamMsg};
use tokio::sync::mpsc::{Receiver, Sender};
use async_trait::async_trait;
use crate::storage::vessel2::VesselIterator;

pub trait Processor {
    fn process(input: Blob) -> Option<Blob>;
}

pub struct BufferBucket {
    buf: Vec<Blob>,
    bucket: Bucket
}

impl BufferBucket {
    pub fn new(bucket: Bucket) -> BufferBucket {
        return BufferBucket{buf: Vec::new(), bucket};
    }
}

pub struct StaticWindowProcessor {
    buffer: BufferBucket
}

impl StaticWindowProcessor {
    pub fn new(window_size: chrono::Duration) -> StaticWindowProcessor {
        let bucket = Bucket::epoch(window_size.num_milliseconds());
        let buffer = BufferBucket::new(bucket);

        return StaticWindowProcessor {
            buffer
        }
    }
}

// impl Processor for StaticWindowProcessor {
//     fn process(&mut self, input: Blob) -> Option<Blob> {
//
//
//     }
// }

pub struct PersistentStream {
    stream_chan: Arc<Sender<StreamMsg>>,
    last_tick_time: ArcRw<UnixTime>,
    chan_buf_size: i32
}

impl PersistentStream {
    pub fn run_db_loop(&self, mut vessel: ArcRw<Vessel>, mut recv: Receiver<Blob>) {
        let chan_size = 50;

        tokio::spawn(async move {
            let mut buf = Vec::<Blob>::with_capacity(
                chan_size);

            loop {
                let next = recv.recv().await.unwrap();
                buf.push(next);

                while buf.len() < buf.capacity()
                {
                    if let Ok(next) = recv.try_recv() {
                        buf.push(next);
                    }
                    else {
                        break;
                    }
                }

                {
                    let lock = vessel.write_lock();
                    lock.write(&mut buf);
                    buf.clear();
                }

                tokio::task::yield_now().await;

            }
        });
    }

    pub fn run_stream_loop(
        mut vessel: ArcRw<Vessel>,
        send: Sender<Blob>,
        recv: Receiver<StreamMsg>)
    {
        tokio::spawn(async move {
            let mut recv = recv;
            let mut db = send;

            let mut subscribers: Vec<Arc<Box<dyn Stream + Send + Sync>>> = Vec::new();
            let subscribers_borrow = &mut subscribers;

            while let Some(next) = recv.recv().await {
                match next {
                    StreamMsg::Batch(data) => {
                        for subscriber in &mut *subscribers_borrow {
                            subscriber.on_next(StreamMsg::Batch(data.clone())).await;
                        }

                        for blob in data.into_iter() {
                            db.send(blob).await.unwrap();
                        }
                    }
                    StreamMsg::Tick(data) => {
                        for subscriber in &mut *subscribers_borrow {
                            subscriber.on_next(StreamMsg::Tick(data)).await;
                        }

                        db.send(data).await.unwrap();
                    }
                    StreamMsg::Subscribe(subscriber) => {
                        let subscriber_last_tick = subscriber.get_last_tick_time();

                        let iterator: VesselIterator;
                        let self_last_tick: UnixTime;
                        {
                            let self_lock = vessel.read_lock();
                            self_last_tick = self_lock.get_last_time();

                            iterator = self_lock.read_from(subscriber_last_tick);
                        }

                        if subscriber_last_tick < self_last_tick {
                            for item in iterator {
                                subscriber.on_next(StreamMsg::Batch(item)).await;
                            }
                        }

                        (&mut *subscribers_borrow).push(subscriber);
                    }
                }
            }
        });
    }

    pub fn new(mut vessel: ArcRw<Vessel>) -> PersistentStream {
        let buf_size = 10000;

        let (db_send, db_recv) =
            tokio::sync::mpsc::channel(buf_size);

        let (stream_send, stream_recv) =
            tokio::sync::mpsc::channel(buf_size);

        let lock = vessel.read_lock();
        let last_time = lock.get_last_time();

        let stream = PersistentStream {
            stream_chan: Arc::new(stream_send),
            last_tick_time: ArcRw::new(last_time),
            chan_buf_size: buf_size as i32
        };

        stream.run_db_loop(vessel.clone(), db_recv);
        Self::run_stream_loop(vessel.clone(), db_send.clone(), stream_recv);

        return stream;
    }
}

#[async_trait]
impl Stream for PersistentStream {
    async fn subscribe(&mut self, stream: Arc<Box<dyn Stream + Send + Sync>>) {
       self.stream_chan.send(StreamMsg::Subscribe(stream)).await;
    }

    async fn on_next(&self, msg: StreamMsg) {
        self.stream_chan.send(msg).await;
    }

    fn get_last_tick_time(&self) -> UnixTime {
        let lock = self.last_tick_time.read_lock();
        return *lock;
    }
}
