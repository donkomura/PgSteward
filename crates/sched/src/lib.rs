pub mod fair;
pub mod split;

use std::collections::BTreeMap;
use std::num::NonZeroU32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Claim<K> {
    pub key: K,
    pub min: u32,
    pub max: u32,
    pub weight: NonZeroU32,
    pub demand: u32,
    pub current: u32,
    pub may_release: bool,
}

impl<K> Claim<K> {
    #[must_use]
    pub fn ceiling(&self) -> u32 {
        self.demand.min(self.max)
    }

    #[must_use]
    pub fn guarantee(&self) -> u32 {
        self.min.min(self.ceiling())
    }

    #[must_use]
    pub fn kept(&self, release: u32) -> u32 {
        let release = if self.may_release { release } else { 0 };
        self.current.saturating_sub(release).min(self.max)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grants<K> {
    granted: BTreeMap<K, u32>,
    total: u32,
    unmet_guarantee: u32,
}

impl<K: Ord> Grants<K> {
    #[must_use]
    pub fn get(&self, key: &K) -> u32 {
        self.granted.get(key).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn total(&self) -> u32 {
        self.total
    }

    #[must_use]
    pub fn unmet_guarantee(&self) -> u32 {
        self.unmet_guarantee
    }

    #[must_use]
    pub fn guarantees_met(&self) -> bool {
        self.unmet_guarantee == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, u32)> {
        self.granted.iter().map(|(key, granted)| (key, *granted))
    }
}

impl<'a, K: Ord> IntoIterator for &'a Grants<K> {
    type Item = (&'a K, u32);
    type IntoIter = std::iter::Map<
        std::collections::btree_map::Iter<'a, K, u32>,
        fn((&'a K, &'a u32)) -> (&'a K, u32),
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.granted.iter().map(|(key, granted)| (key, *granted))
    }
}

pub trait Allocator<K: Ord + Clone> {
    fn allocate(&self, budget: u32, claims: &[Claim<K>]) -> Grants<K>;
}
