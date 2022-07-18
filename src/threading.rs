use std::sync::{Arc, LockResult, RwLock, RwLockReadGuard, RwLockWriteGuard};
use crate::storage::file_system::FileSystem;

pub fn read_lock<T>(locked: &Arc<RwLock<T>>) -> RwLockReadGuard<T> {
    return locked.read().unwrap();
}

pub fn write_lock<T>(locked: &Arc<RwLock<T>>) -> RwLockWriteGuard<T> {
    return locked.write().unwrap();
}