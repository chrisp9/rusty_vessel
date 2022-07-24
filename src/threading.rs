use std::borrow::{Borrow, BorrowMut};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard};
use crate::storage::file_system::FileSystem;

pub struct ArcRead<T: ?Sized> {
    item: Arc<RwLock<T>>
}

impl <T> ArcRead<T> {
    pub fn new(item: T) -> ArcRead<T> {
        return ArcRead {
            item: Arc::new(RwLock::new(item))
        };
    }

    pub fn read(&self) -> RwLockReadGuard<T> {
        let lock = self.item.read().unwrap();
        return lock;
    }
}

impl<T> Clone for ArcRead <T> {
    fn clone(&self) -> Self {
        return ArcRead {
            item: self.item.clone()
        };
    }
}

pub struct ArcRw<T> {
    item: Arc<RwLock<T>>
}

impl<T> ArcRw<T> {
    pub fn new(item: T) -> ArcRw<T> {
        return ArcRw {
            item: Arc::new(RwLock::new(item))
        };
    }

    pub fn read(&self) -> RwLockReadGuard<T> {
        let lock = self.item.read().unwrap();
        return lock;
    }

    pub fn write(&self) -> RwLockWriteGuard<T> {
        let mut lock = self.item.write().unwrap();
        return lock;
    }
}

impl<T> Clone for ArcRw<T> {
    fn clone(&self) -> Self {

        return ArcRw {
            item: self.item.clone()
        };
    }
}
