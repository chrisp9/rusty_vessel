use std::rc::Rc;
use crate::{Blob, StreamDefinition};

pub trait Stream {
    fn subscribe(&self, parent: StreamDefinition, stream: Rc<dyn Stream>);
    fn on_next(&self, msg: Rc<StreamMsg>);
    fn flush(&self);
}

#[derive(Clone)]
pub enum StreamMsg {
    Batch(Vec<Blob>),
    Tick(Blob),
    Flush()
}