use std::collections::BTreeMap;
use std::time::Duration;

use pgsteward_core::rt::ClockRate;
use pgsteward_harness::faults::{Change, Fault, FaultPlan, Incident, Scenario};
use proptest::prelude::*;

const PLAN: FaultPlan = FaultPlan {
    nodes: 3,
    span: Duration::from_secs(30),
    incidents: 8,
    longest: Duration::from_secs(5),
    slowest: Duration::from_millis(500),
    drift: 50_000,
};

fn by_node(scenario: &Scenario) -> BTreeMap<usize, Vec<Incident>> {
    let mut nodes: BTreeMap<usize, Vec<Incident>> = BTreeMap::new();
    for incident in &scenario.incidents {
        nodes.entry(incident.node).or_default().push(*incident);
    }
    for incidents in nodes.values_mut() {
        incidents.sort();
    }
    nodes
}

#[test]
fn different_seeds_inject_different_faults() {
    let scenarios: Vec<_> = (0..20)
        .map(|seed| Scenario::from_seed(seed, &PLAN))
        .collect();
    assert!(
        scenarios
            .windows(2)
            .any(|pair| pair[0].incidents != pair[1].incidents),
        "different seeds must be able to inject different faults"
    );
}

#[test]
fn every_kind_of_fault_is_drawn_across_seeds() {
    let incidents: Vec<_> = (0..20)
        .flat_map(|seed| Scenario::from_seed(seed, &PLAN).incidents)
        .collect();
    assert!(incidents.iter().any(|i| i.fault == Fault::Crash));
    assert!(incidents.iter().any(|i| i.fault == Fault::CutOff));
    assert!(incidents.iter().any(|i| matches!(i.fault, Fault::Slow(_))));
}

#[test]
fn a_plan_without_incidents_injects_nothing() {
    let quiet = FaultPlan {
        incidents: 0,
        ..PLAN
    };
    assert!(
        Scenario::from_seed(7, &quiet).incidents.is_empty(),
        "a plan without incidents must inject no fault"
    );
    assert!(
        Scenario::from_seed(7, &quiet).changes().is_empty(),
        "a plan without incidents must change nothing"
    );
}

#[test]
fn a_plan_without_drift_keeps_every_clock_exact() {
    let steady = FaultPlan { drift: 0, ..PLAN };
    assert_eq!(
        Scenario::from_seed(7, &steady).clocks,
        vec![ClockRate::EXACT; PLAN.nodes]
    );
}

#[test]
fn different_seeds_skew_clocks_differently() {
    let clocks: Vec<_> = (0..20)
        .map(|seed| Scenario::from_seed(seed, &PLAN).clocks)
        .collect();
    assert!(
        clocks.windows(2).any(|pair| pair[0] != pair[1]),
        "different seeds must be able to skew the clocks differently"
    );
    assert!(
        clocks.iter().flatten().any(|rate| *rate > ClockRate::EXACT),
        "some clock must run fast"
    );
    assert!(
        clocks.iter().flatten().any(|rate| *rate < ClockRate::EXACT),
        "some clock must run slow"
    );
}

proptest! {
    #[test]
    fn every_node_runs_a_clock_within_the_drift(seed in any::<u64>()) {
        let clocks = Scenario::from_seed(seed, &PLAN).clocks;
        prop_assert_eq!(clocks.len(), PLAN.nodes);
        for rate in clocks {
            prop_assert!(
                rate.get().get().abs_diff(ClockRate::EXACT.get().get()) <= PLAN.drift,
                "{rate:?}"
            );
        }
    }

    #[test]
    fn the_same_seed_injects_the_same_faults(seed in any::<u64>()) {
        prop_assert_eq!(Scenario::from_seed(seed, &PLAN), Scenario::from_seed(seed, &PLAN));
    }

    #[test]
    fn a_scenario_carries_its_seed(seed in any::<u64>()) {
        prop_assert_eq!(Scenario::from_seed(seed, &PLAN).seed, seed);
    }

    #[test]
    fn the_plan_bounds_every_incident(seed in any::<u64>()) {
        let scenario = Scenario::from_seed(seed, &PLAN);
        prop_assert!(!scenario.incidents.is_empty());
        prop_assert!(scenario.incidents.len() <= PLAN.incidents);
        for incident in &scenario.incidents {
            prop_assert!(incident.node < PLAN.nodes, "{incident:?}");
            prop_assert!(incident.from < incident.until, "{incident:?}");
            prop_assert!(incident.until <= PLAN.span, "every fault must heal within the span: {incident:?}");
            prop_assert!(incident.until <= incident.from + PLAN.longest, "{incident:?}");
            if let Fault::Slow(latency) = incident.fault {
                prop_assert!(!latency.is_zero() && latency <= PLAN.slowest, "{incident:?}");
            }
        }
    }

    #[test]
    fn faults_on_one_node_never_overlap(seed in any::<u64>()) {
        for incidents in by_node(&Scenario::from_seed(seed, &PLAN)).values() {
            for pair in incidents.windows(2) {
                prop_assert!(pair[0].until < pair[1].from, "{pair:?}");
            }
        }
    }

    #[test]
    fn changes_begin_and_end_every_incident_in_time_order(seed in any::<u64>()) {
        let scenario = Scenario::from_seed(seed, &PLAN);
        let changes = scenario.changes();
        prop_assert_eq!(changes.len(), scenario.incidents.len() * 2);
        prop_assert!(changes.windows(2).all(|pair| pair[0].0 <= pair[1].0), "{changes:?}");
        for (node, incidents) in by_node(&scenario) {
            let on_node: Vec<_> = changes
                .iter()
                .filter(|(_, n, _)| *n == node)
                .map(|(at, _, change)| (*at, *change))
                .collect();
            let expected: Vec<_> = incidents
                .iter()
                .flat_map(|i| [(i.from, Change::Begin(i.fault)), (i.until, Change::End(i.fault))])
                .collect();
            prop_assert_eq!(on_node, expected);
        }
    }
}
