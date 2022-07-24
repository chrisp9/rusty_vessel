use crate::{ArcRead, Blob, UnixTime};

pub trait Stream {
    fn subscribe(&mut self, stream: ArcRead<Box<dyn Stream + Send + Sync>>);
    fn on_next(&self, record: StreamMsg);
    fn get_last_tick_time(&self) -> UnixTime;
}

pub enum StreamMsg {
    Snapshot(Vec<Blob>),
    Delta(Blob),
    Subscribe(ArcRead<Box<dyn Stream + Send + Sync>>),
}
