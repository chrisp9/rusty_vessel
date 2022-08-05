use std::rc::Rc;
use crate::{Blob};
use crate::domain::UnixTime;
use crate::storage::domain::bucket::Bucket;

pub trait Stream {
    fn replay(&mut self, since: UnixTime) -> Box<dyn Iterator<Item=Vec<Blob>>>;
    fn flush(&mut self);
    fn on_next(&mut self, batch: Rc<Vec<Blob>>) -> Rc<Vec<Blob>>;
}

pub struct BufferBucket {
    pub buf: Vec<Blob>,
    bucket: Bucket,
}

impl BufferBucket {
    pub fn new(bucket: Bucket) -> BufferBucket {
        return BufferBucket { buf: Vec::new(), bucket };
    }
}
