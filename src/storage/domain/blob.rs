use std::any::Any;
use std::sync::{Arc, Mutex};
use bincode::{Encode, Decode};

#[derive(Copy, Clone)]
pub struct Blob {
    pub timestamp: i64,
    pub data: f64
}

impl Blob {
    pub fn new(timestamp: i64, data: f64) -> Blob {
        return Blob {
            timestamp,
            data
        };
    }
}