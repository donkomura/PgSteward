use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Share<K> {
    pub key: K,
    pub demand: u32,
    pub current: u32,
    pub may_release: bool,
}

impl<K> Share<K> {
    #[must_use]
    pub fn need(&self) -> u32 {
        if self.may_release {
            self.demand
        } else {
            self.demand.max(self.current)
        }
    }
}

pub fn split<K: Ord + Clone>(_granted: u32, _shares: &[Share<K>]) -> BTreeMap<K, u32> {
    BTreeMap::new()
}
