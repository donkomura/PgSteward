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
        .map(|share| share.current.min(share.demand))
        .collect();
    let mut total = sum(&given);
    let granted = u64::from(granted);
    while total > granted {
        let Some(most) = first_max(given.iter().copied()) else {
            break;
        };
        given[most] -= 1;
        total -= 1;
    }
    give_up_to(granted, &order, &mut given, |share| share.demand);
    give_up_to(granted, &order, &mut given, Share::need);
    order
        .into_iter()
        .zip(given)
        .map(|(share, given)| (share.key.clone(), given))
        .collect()
}

fn give_up_to<K>(
    granted: u64,
    order: &[&Share<K>],
    given: &mut [u32],
    wanted: impl Fn(&Share<K>) -> u32,
) {
    let mut total = sum(given);
    while total < granted {
        let shortfalls = order
            .iter()
            .zip(given.iter())
            .map(|(share, given)| wanted(share).saturating_sub(*given));
        let Some(furthest) = first_max(shortfalls) else {
            break;
        };
        given[furthest] += 1;
        total += 1;
    }
}

fn sum(given: &[u32]) -> u64 {
    given.iter().copied().map(u64::from).sum()
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
