use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap};

use crate::{Allocator, Claim, Grants};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightedMaxMinFair {
    release: u32,
}

impl WeightedMaxMinFair {
    #[must_use]
    pub fn new(release: u32) -> Self {
        Self { release }
    }

    #[must_use]
    pub fn release(&self) -> u32 {
        self.release
    }
}

impl Default for WeightedMaxMinFair {
    fn default() -> Self {
        Self::new(1)
    }
}

#[derive(Debug, Clone, Copy)]
struct Row {
    guarantee: u32,
    ceiling: u32,
    kept: u32,
    held: u32,
    weight: u32,
    granted: u32,
    base: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Guarantee,
    Demand,
    Retention,
}

impl Phase {
    fn limit(self, row: &Row) -> u32 {
        match self {
            Self::Guarantee => row.guarantee,
            Self::Demand => row.ceiling,
            Self::Retention => row.kept,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq)]
struct Next {
    index: usize,
    taken: u32,
    weight: u32,
    moving: bool,
}

impl Next {
    fn after(index: usize, row: &Row) -> Self {
        Self {
            index,
            taken: row.granted - row.base + 1,
            weight: row.weight,
            moving: row.granted >= row.held,
        }
    }
}

impl Ord for Next {
    fn cmp(&self, other: &Self) -> Ordering {
        let mine = u64::from(self.taken) * u64::from(other.weight);
        let theirs = u64::from(other.taken) * u64::from(self.weight);
        mine.cmp(&theirs)
            .then_with(|| self.moving.cmp(&other.moving))
            .then_with(|| self.index.cmp(&other.index))
    }
}

impl PartialOrd for Next {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Next {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

fn fill(rows: &mut [Row], remaining: &mut u32, phase: Phase) {
    if *remaining == 0 {
        return;
    }

    for row in rows.iter_mut() {
        row.base = row.granted;
    }

    let mut queue: BinaryHeap<Reverse<Next>> = rows
        .iter()
        .enumerate()
        .filter(|(_, row)| row.granted < phase.limit(row))
        .map(|(index, row)| Reverse(Next::after(index, row)))
        .collect();

    while *remaining > 0 {
        let Some(Reverse(next)) = queue.pop() else {
            break;
        };
        let row = &mut rows[next.index];
        row.granted += 1;
        *remaining -= 1;
        if row.granted < phase.limit(row) {
            queue.push(Reverse(Next::after(next.index, row)));
        }
    }
}

impl<K: Ord + Clone> Allocator<K> for WeightedMaxMinFair {
    fn allocate(&self, budget: u32, claims: &[Claim<K>]) -> Grants<K> {
        let mut order: Vec<usize> = (0..claims.len()).collect();
        order.sort_by(|&left, &right| claims[left].key.cmp(&claims[right].key));

        let mut rows: Vec<Row> = order
            .iter()
            .map(|&index| Row {
                guarantee: claims[index].guarantee(),
                ceiling: claims[index].ceiling(),
                kept: claims[index].kept(self.release),
                held: claims[index].current,
                weight: claims[index].weight.get(),
                granted: 0,
                base: 0,
            })
            .collect();

        let mut remaining = budget;
        fill(&mut rows, &mut remaining, Phase::Guarantee);
        fill(&mut rows, &mut remaining, Phase::Demand);
        fill(&mut rows, &mut remaining, Phase::Retention);

        let mut granted = BTreeMap::new();
        let mut total = 0;
        let mut unmet: u64 = 0;
        for (row, &index) in rows.iter().zip(order.iter()) {
            unmet += u64::from(row.guarantee.saturating_sub(row.granted));
            if row.granted > 0 {
                granted.insert(claims[index].key.clone(), row.granted);
                total += row.granted;
            }
        }

        Grants {
            granted,
            total,
            unmet_guarantee: u32::try_from(unmet).unwrap_or(u32::MAX),
        }
    }
}
