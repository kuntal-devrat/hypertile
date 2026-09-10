//! Object pool and memory recycler for Hypertile tasks.
//!
//! Reduces OS heap allocation churn under high-frequency task creation and destruction.

use parking_lot::Mutex;

/// A bounded, thread-safe object recycler.
pub struct ObjectPool<T> {
    pool: Mutex<Vec<T>>,
    capacity: usize,
}

impl<T> ObjectPool<T> {
    pub const fn new(capacity: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::new()),
            capacity,
        }
    }

    /// Pop a recycled object if available, otherwise call `init`.
    pub fn get_or<F: FnOnce() -> T>(&self, init: F) -> T {
        let mut guard = self.pool.lock();
        if let Some(item) = guard.pop() {
            item
        } else {
            drop(guard);
            init()
        }
    }

    /// Recycle an item back into the pool if below capacity.
    pub fn recycle(&self, item: T) {
        let mut guard = self.pool.lock();
        if guard.len() < self.capacity {
            guard.push(item);
        }
    }

    /// Number of recycled items currently available in the pool.
    pub fn available(&self) -> usize {
        self.pool.lock().len()
    }
}
