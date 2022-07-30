use crate::{ArcRead, Blob};
use crate::domain::UnixTime;

pub trait Stream {
    fn subscribe(&mut self, stream: ArcRead<Box<dyn Stream + Send + Sync>>);
    fn on_next(&self, record: StreamMsg);
    fn get_last_tick_time(&self) -> UnixTime;
}

pub enum StreamMsg {
    Batch(Vec<Blob>),
    Tick(Blob),
    Subscribe(ArcRead<Box<dyn Stream + Send + Sync>>),
}
