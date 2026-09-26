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

pub fn split<K: Ord + Clone>(granted: u32, shares: &[Share<K>]) -> BTreeMap<K, u32> {
    let mut order: Vec<&Share<K>> = shares.iter().collect();
    order.sort_by(|a, b| a.key.cmp(&b.key));
    let mut given: Vec<u32> = order
        .iter()
        .map(|share| share.current.min(share.need()))
        .collect();
    let mut total: u64 = given.iter().copied().map(u64::from).sum();
    let granted = u64::from(granted);
    while total > granted {
        let excess = order
            .iter()
            .zip(&given)
            .map(|(share, given)| given.saturating_sub(share.demand));
        let Some(most) = first_max(excess).or_else(|| first_max(given.iter().copied())) else {
            break;
        };
        given[most] -= 1;
        total -= 1;
    }
    while total < granted {
        let shortfalls = order
            .iter()
            .zip(&given)
            .map(|(share, given)| share.need() - given);
        let Some(furthest) = first_max(shortfalls) else {
            break;
        };
        given[furthest] += 1;
        total += 1;
    }
    order
        .into_iter()
        .zip(given)
        .map(|(share, given)| (share.key.clone(), given))
        .collect()
}

fn first_max(values: impl Iterator<Item = u32>) -> Option<usize> {
    let mut best: Option<(usize, u32)> = None;
    for (index, value) in values.enumerate() {
        if value > best.map_or(0, |(_, most)| most) {
            best = Some((index, value));
        }
    }
    best.map(|(index, _)| index)
}
