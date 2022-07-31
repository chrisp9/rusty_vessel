
#[derive(PartialEq, PartialOrd, Copy, Clone, Hash, Eq, Ord)]
pub struct Bucket {
    pub val: i64,
    pub interval: i64
}

impl Bucket {
    pub fn new(val: i64, interval: i64) -> Bucket {
        return Bucket {val, interval };
    }

    pub fn epoch(interval: i64) -> Bucket {
        return Bucket { val: 0, interval };
    }

    pub fn next(&self) -> Bucket {
        return Self::new(self.val + self.interval, self.interval);
    }

    pub fn for_time(val: i64, interval: i64) -> Bucket {
        let start = val - (val % interval);

        return Bucket {
            val: start,
            interval
        };
    }
}
