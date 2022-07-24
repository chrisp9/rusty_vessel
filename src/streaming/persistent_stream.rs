use std::{io, sync, thread};
use std::sync::{Arc, RwLock};
use crate::{Blob, Vessel};
use crate::storage::bucket_issuer::UnixTime;
use crate::threading::{ArcRead, ArcRw};
use std::sync::mpsc::{Receiver, sync_channel, SyncSender};
use crate::streaming::domain::{Stream, StreamMsg};

pub struct PersistentStream {
    stream_chan: SyncSender<StreamMsg>,
    last_tick_time: ArcRw<UnixTime>
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

    pub fn run_stream_loop(
        mut vessel: ArcRw<Vessel>,
        send: SyncSender<Blob>,
        recv: Receiver<StreamMsg>)
    {
        thread::spawn(move || {
            loop {
                let next = recv.recv().unwrap();

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
                    StreamMsg::Subscribe(subscriber) => {

                    }
                }
            }
        });
    }

    pub fn new(mut vessel: ArcRw<Vessel>) -> PersistentStream {
        let (db_send, db_recv) =
            sync_channel(10);

        let (stream_send, stream_recv) =
            sync_channel(10);

        Self::run_db_loop(vessel.clone(), db_recv);
        Self::run_stream_loop(vessel.clone(), db_send, stream_recv);

        let lock = vessel.read();
        let last_time = lock.get_last_time();

        return PersistentStream {
            stream_chan: stream_send,
            last_tick_time: ArcRw::new(last_time)
        };
    }
}

impl Stream for PersistentStream {
    fn subscribe(&mut self, stream: ArcRead<dyn Stream + Send + Sync>) {
        self.stream_chan.send(StreamMsg::Subscribe(stream)).unwrap();
    }

    fn on_next(&self, msg: StreamMsg) {
        self.stream_chan.send(msg).unwrap();
    }

    fn get_last_tick_time(&self) -> UnixTime {
        let lock = self.last_tick_time.read();
        return *lock;
    }
}
