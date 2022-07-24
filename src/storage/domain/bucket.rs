
#[derive(PartialEq, PartialOrd, Copy, Clone, Hash, Eq, Ord)]
pub struct Bucket {
    pub value: i64
}

impl Bucket {
    pub fn new(val: i64) -> Bucket {
        return Bucket {value: val};
    }
}
