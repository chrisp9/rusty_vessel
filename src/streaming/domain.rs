use std::rc::Rc;
use std::sync::Arc;
use crate::{ArcRead, Blob};
use crate::domain::UnixTime;
use async_trait::async_trait;

#[async_trait]
pub trait Stream {
    async fn subscribe(&mut self, stream: Arc<Box<dyn Stream + Send + Sync>>);
    async fn on_next(&self, record: StreamMsg);
    fn get_last_tick_time(&self) -> UnixTime;
}

pub enum StreamMsg {
    Batch(Vec<Blob>),
    Tick(Blob),
    Subscribe(Arc<Box<dyn Stream + Send + Sync>>),
}