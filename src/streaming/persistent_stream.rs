use std::borrow::Borrow;
use std::future::Future;
use std::{io, sync, thread};
use std::ops::Deref;
use std::os::unix::raw::mode_t;
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;
use crate::{Blob, Vessel};
use crate::storage::bucket_issuer::UnixTime;
use crate::threading::{ArcRead, ArcRw};
use std::sync::mpsc::{Sender, Receiver, sync_channel, SyncSender};
use std::sync::mpsc;


pub trait Stream {
    // fn subscribe(&mut self, stream: dyn Stream);
    fn on_next(&self, record: StreamMsg);
}

pub enum StreamMsg {
    Snapshot(Vec<Blob>),
    Delta(Blob),
    Subscribe(ArcRead<dyn Stream + Send + Sync>),
}

pub struct PersistentStream {
    stream_chan: SyncSender<StreamMsg>,
}

impl PersistentStream {
    pub fn run_db_loop(mut vessel: ArcRw<Vessel>, recv: Receiver<Blob>) {
        thread::spawn(move || {
            loop {
                let next = recv.recv().unwrap();
                let mut lock = vessel.write();

                lock.write(next);
            }
        });
    }

    pub fn run_stream_loop(mut vessel: ArcRw<Vessel>, send: SyncSender<Blob>, recv: Receiver<StreamMsg>) {
        thread::spawn(move || {
            loop {
                let next = recv.recv().unwrap();
                {
                    match next {
                        StreamMsg::Snapshot(data) => {
                            for blob in data.iter() {
                                let blob = blob.clone();

                                send.send(blob.clone()).unwrap();
                            }
                        }
                        StreamMsg::Delta(data) => {
                            send.send(data).unwrap();
                        }
                        StreamMsg::Subscribe(_) => {}
                    }
                }
            }
        });
    }

    pub fn new(mut vessel: ArcRw<Vessel>, last_time: UnixTime) -> PersistentStream {
        let (db_send, db_recv) =
            sync_channel(10);

        let (stream_send, stream_recv) =
            sync_channel(10);

        Self::run_db_loop(vessel.clone(), db_recv);
        Self::run_stream_loop(vessel, db_send, stream_recv);
        ;

        return PersistentStream {
            stream_chan: stream_send,
        };
    }
}

impl Stream for PersistentStream {
    fn on_next(&self, record: StreamMsg) {
        self.stream_chan.send(record).unwrap();
    }
}
