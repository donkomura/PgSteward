use std::num::NonZeroU32;
use std::time::Duration;

use pgsteward_core::rt::ClockRate;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

const SHORTEST: Duration = Duration::from_millis(1);

/// What a seeded scenario may inject: up to `incidents` faults on `nodes`
/// nodes, each lasting at most `longest` and healed within `span`, and a clock
/// on each node that runs up to `drift` millionths fast or slow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultPlan {
    pub nodes: usize,
    pub span: Duration,
    pub incidents: usize,
    pub longest: Duration,
    pub slowest: Duration,
    pub drift: u32,
}

/// A fault on one node: the node itself crashes, or its link to the database
/// is cut or slowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fault {
    Crash,
    CutOff,
    Slow(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Incident {
    pub from: Duration,
    pub until: Duration,
    pub node: usize,
    pub fault: Fault,
}

impl Incident {
    fn overlaps(&self, other: &Incident) -> bool {
        self.node == other.node && self.from <= other.until && other.from <= self.until
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Change {
    Begin(Fault),
    End(Fault),
}

/// The faults a seed injects. A failing run prints the scenario, and with it
/// the seed that reproduces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scenario {
    pub seed: u64,
    pub clocks: Vec<ClockRate>,
    pub incidents: Vec<Incident>,
}

impl Scenario {
    /// A drawn fault that would overlap one already placed on the same node is
    /// dropped rather than redrawn, so the number of draws, and with it the
    /// scenario, depends on the seed alone.
    #[must_use]
    pub fn from_seed(seed: u64, plan: &FaultPlan) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let clocks = (0..plan.nodes)
            .map(|_| skew(&mut rng, plan.drift))
            .collect();
        let mut incidents: Vec<Incident> = Vec::new();
        for _ in 0..plan.incidents {
            let drawn = draw(&mut rng, plan);
            if incidents.iter().all(|placed| !placed.overlaps(&drawn)) {
                incidents.push(drawn);
            }
        }
        incidents.sort();
        Self {
            seed,
            clocks,
            incidents,
        }
    }

    #[must_use]
    pub fn changes(&self) -> Vec<(Duration, usize, Change)> {
        let mut changes: Vec<_> = self
            .incidents
            .iter()
            .flat_map(|incident| {
                [
                    (incident.from, incident.node, Change::Begin(incident.fault)),
                    (incident.until, incident.node, Change::End(incident.fault)),
                ]
            })
            .collect();
        changes.sort_by_key(|&(at, node, _)| (at, node));
        changes
    }
}

fn skew(rng: &mut StdRng, drift: u32) -> ClockRate {
    let exact = ClockRate::EXACT.get().get();
    let per_million =
        rng.random_range(exact.saturating_sub(drift).max(1)..=exact.saturating_add(drift));
    ClockRate::per_million(NonZeroU32::new(per_million).expect("the rate is at least 1"))
}

fn draw(rng: &mut StdRng, plan: &FaultPlan) -> Incident {
    let length = rng.random_range(SHORTEST..=plan.longest.min(plan.span));
    let from = rng.random_range(Duration::ZERO..=plan.span.saturating_sub(length));
    let fault = match rng.random_range(0..3) {
        0 => Fault::Crash,
        1 => Fault::CutOff,
        _ => Fault::Slow(rng.random_range(SHORTEST..=plan.slowest)),
    };
    Incident {
        from,
        until: from + length,
        node: rng.random_range(0..plan.nodes),
        fault,
    }
}
