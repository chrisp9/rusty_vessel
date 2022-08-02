use std::rc::Rc;
use crate::{Blob, StreamDefinition};
use crate::domain::UnixTime;
use crate::storage::domain::bucket::Bucket;

pub trait Stream {
    fn replay(&mut self) -> Box<dyn Iterator<Item=Vec<Blob>>>;
    fn flush(&self);
    fn on_next(&self, batch: Rc<Vec<Blob>>) -> Rc<Vec<Blob>>;
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
