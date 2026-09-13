use std::time::Duration;

use pgsteward_core::budget::{
    BudgetChange, BudgetInputs, ForeignPeak, InstanceBudget, ServerLimits, TotalBudget,
};
use pgsteward_core::rt::Instant;
use proptest::prelude::*;

fn inputs(
    max_connections: u32,
    superuser_reserved_connections: u32,
    reserved_connections: u32,
    foreign_peak: u32,
    margin: u32,
) -> BudgetInputs {
    BudgetInputs {
        limits: ServerLimits {
            max_connections,
            superuser_reserved_connections,
            reserved_connections,
        },
        foreign_peak,
        margin,
    }
}

#[test]
fn total_budget_is_max_connections_minus_reservations_foreign_peak_and_margin() {
    let budget = TotalBudget::derive(inputs(200, 3, 0, 32, 15));
    assert_eq!(budget.total(), 150);
    assert!(!budget.is_exhausted());
    assert_eq!(budget.shortfall(), 0);
}

#[test]
fn reserved_connections_of_postgresql_16_are_subtracted_as_well() {
    let budget = TotalBudget::derive(inputs(200, 3, 5, 32, 15));
    assert_eq!(budget.total(), 145);
}

#[test]
fn server_limits_report_the_reserved_total() {
    let limits = ServerLimits {
        max_connections: 200,
        superuser_reserved_connections: 3,
        reserved_connections: 5,
    };
    assert_eq!(limits.reserved(), 8);
}

#[test]
fn without_foreign_connections_or_margin_the_budget_is_what_the_server_leaves_to_clients() {
    let budget = TotalBudget::derive(inputs(100, 3, 0, 0, 0));
    assert_eq!(budget.total(), 97);
}

#[test]
fn budget_saturates_at_zero_and_reports_the_shortfall() {
    let budget = TotalBudget::derive(inputs(100, 3, 0, 90, 15));
    assert_eq!(budget.total(), 0);
    assert!(budget.is_exhausted());
    assert_eq!(budget.shortfall(), 8);
}

#[test]
fn a_budget_of_exactly_zero_is_exhausted_without_shortfall() {
    let budget = TotalBudget::derive(inputs(100, 3, 0, 82, 15));
    assert_eq!(budget.total(), 0);
    assert!(budget.is_exhausted());
    assert_eq!(budget.shortfall(), 0);
}

#[test]
fn budget_keeps_the_inputs_it_was_derived_from() {
    let given = inputs(200, 3, 0, 32, 15);
    let budget = TotalBudget::derive(given);
    assert_eq!(budget.inputs(), given);
}

#[test]
fn display_explains_every_term_of_the_derivation() {
    let text = TotalBudget::derive(inputs(200, 3, 1, 32, 15)).to_string();
    assert_eq!(
        text,
        "149 = max_connections 200 - superuser_reserved_connections 3 - reserved_connections 1 - foreign peak 32 - margin 15"
    );
}

#[test]
fn display_of_an_exhausted_budget_names_the_shortfall() {
    let text = TotalBudget::derive(inputs(100, 3, 0, 90, 15)).to_string();
    assert!(text.starts_with("0 = "), "{text}");
    assert!(text.contains("short by 8"), "{text}");
}

fn arb_inputs() -> impl Strategy<Value = BudgetInputs> {
    (
        0u32..=10_000,
        0u32..=100,
        0u32..=100,
        0u32..=10_000,
        0u32..=1_000,
    )
        .prop_map(|(max, superuser, reserved, peak, margin)| {
            inputs(max, superuser, reserved, peak, margin)
        })
}

proptest! {
    #[test]
    fn total_never_exceeds_what_the_server_leaves_to_clients(given in arb_inputs()) {
        let budget = TotalBudget::derive(given);
        let left_to_clients = given.limits.max_connections.saturating_sub(given.limits.reserved());
        prop_assert!(budget.total() <= left_to_clients);
    }

    #[test]
    fn total_plus_deductions_equals_max_connections_unless_short(given in arb_inputs()) {
        let budget = TotalBudget::derive(given);
        let deductions = u64::from(given.limits.reserved())
            + u64::from(given.foreign_peak)
            + u64::from(given.margin);
        let max = u64::from(given.limits.max_connections);
        if budget.shortfall() == 0 {
            prop_assert_eq!(u64::from(budget.total()) + deductions, max);
        } else {
            prop_assert_eq!(budget.total(), 0);
            prop_assert_eq!(max + u64::from(budget.shortfall()), deductions);
        }
    }

    #[test]
    fn more_foreign_connections_never_grow_the_budget(given in arb_inputs(), extra in 0u32..=1_000) {
        let before = TotalBudget::derive(given).total();
        let after = TotalBudget::derive(BudgetInputs {
            foreign_peak: given.foreign_peak.saturating_add(extra),
            ..given
        })
        .total();
        prop_assert!(after <= before);
    }

    #[test]
    fn a_larger_margin_never_grows_the_budget(given in arb_inputs(), extra in 0u32..=1_000) {
        let before = TotalBudget::derive(given).total();
        let after = TotalBudget::derive(BudgetInputs {
            margin: given.margin.saturating_add(extra),
            ..given
        })
        .total();
        prop_assert!(after <= before);
    }

    #[test]
    fn derivation_is_deterministic(given in arb_inputs()) {
        prop_assert_eq!(TotalBudget::derive(given), TotalBudget::derive(given));
    }
}

#[test]
fn a_peak_without_any_observation_is_zero() {
    let peak = ForeignPeak::new(Duration::from_secs(60));
    assert_eq!(peak.peak(), 0);
}

#[test]
fn a_rise_takes_the_peak_at_once() {
    let base = Instant::now();
    let mut peak = ForeignPeak::new(Duration::from_secs(60));
    peak.observe(base, 5);
    peak.observe(base + Duration::from_secs(1), 40);
    assert_eq!(peak.peak(), 40);
}

#[test]
fn the_peak_is_the_largest_observation_in_the_window_not_the_latest() {
    let base = Instant::now();
    let mut peak = ForeignPeak::new(Duration::from_secs(60));
    peak.observe(base, 32);
    peak.observe(base + Duration::from_secs(10), 20);
    peak.observe(base + Duration::from_secs(20), 11);
    assert_eq!(peak.peak(), 32);
}

#[test]
fn an_observation_older_than_the_window_no_longer_holds_the_peak() {
    let base = Instant::now();
    let mut peak = ForeignPeak::new(Duration::from_secs(60));
    peak.observe(base, 32);
    peak.observe(base + Duration::from_secs(10), 20);
    peak.observe(base + Duration::from_secs(70), 20);
    assert_eq!(peak.peak(), 20);
}

fn limits(max_connections: u32) -> ServerLimits {
    ServerLimits {
        max_connections,
        superuser_reserved_connections: 3,
        reserved_connections: 0,
    }
}

#[test]
fn a_budget_without_any_observation_deducts_no_foreign_connections() {
    let budget = InstanceBudget::new(limits(200), 15, Duration::from_secs(60));
    assert_eq!(budget.current().total(), 182);
    assert_eq!(budget.current().inputs().foreign_peak, 0);
}

#[test]
fn foreign_connections_that_appear_shrink_the_budget() {
    let base = Instant::now();
    let mut budget = InstanceBudget::new(limits(200), 15, Duration::from_secs(60));
    assert_eq!(
        budget.observe(base, 32),
        BudgetChange::Shrank { from: 182, to: 150 }
    );
    assert_eq!(budget.current().total(), 150);
    assert_eq!(budget.current().inputs().foreign_peak, 32);
}

#[test]
fn an_observation_under_the_peak_leaves_the_budget_where_it_is() {
    let base = Instant::now();
    let mut budget = InstanceBudget::new(limits(200), 15, Duration::from_secs(60));
    budget.observe(base, 32);
    assert_eq!(
        budget.observe(base + Duration::from_secs(10), 20),
        BudgetChange::Unchanged
    );
    assert_eq!(budget.current().total(), 150);
}

#[test]
fn the_budget_grows_once_the_peak_leaves_the_window() {
    let base = Instant::now();
    let mut budget = InstanceBudget::new(limits(200), 15, Duration::from_secs(60));
    budget.observe(base, 32);
    budget.observe(base + Duration::from_secs(10), 20);
    assert_eq!(
        budget.observe(base + Duration::from_secs(70), 20),
        BudgetChange::Grew { from: 150, to: 162 }
    );
}

#[test]
fn a_resized_server_moves_the_budget_without_a_new_observation() {
    let base = Instant::now();
    let mut budget = InstanceBudget::new(limits(200), 15, Duration::from_secs(60));
    budget.observe(base, 32);
    assert_eq!(
        budget.update_limits(limits(400)),
        BudgetChange::Grew { from: 150, to: 350 }
    );
    assert_eq!(budget.current().inputs().limits, limits(400));
}

#[test]
fn foreign_connections_beyond_the_server_limits_exhaust_the_budget() {
    let base = Instant::now();
    let mut budget = InstanceBudget::new(limits(100), 15, Duration::from_secs(60));
    budget.observe(base, 90);
    assert!(budget.current().is_exhausted());
    assert_eq!(budget.current().shortfall(), 8);
}

proptest! {
    #[test]
    fn the_peak_is_the_largest_observation_the_window_still_holds(
        observations in prop::collection::vec((1u64..=40, 0u32..=500), 1..40)
    ) {
        let window_secs = 60;
        let base = Instant::now();
        let mut peak = ForeignPeak::new(Duration::from_secs(window_secs));
        let mut seen: Vec<(u64, u32)> = Vec::new();
        let mut elapsed = 0;
        for (gap, count) in observations {
            elapsed += gap;
            peak.observe(base + Duration::from_secs(elapsed), count);
            seen.push((elapsed, count));
            let held = seen
                .iter()
                .filter(|(at, _)| elapsed - at <= window_secs)
                .map(|(_, count)| *count)
                .max()
                .unwrap_or(0);
            prop_assert_eq!(peak.peak(), held);
        }
    }

    #[test]
    fn an_observation_never_moves_the_budget_and_the_peak_the_same_way(
        first in 0u32..=200,
        second in 0u32..=200,
    ) {
        let base = Instant::now();
        let mut budget = InstanceBudget::new(limits(400), 15, Duration::from_secs(60));
        budget.observe(base, first);
        let before = budget.current();
        let change = budget.observe(base + Duration::from_secs(1), second);
        let after = budget.current();
        match change {
            BudgetChange::Unchanged => {
                prop_assert_eq!(before.total(), after.total());
            }
            BudgetChange::Grew { from, to } => {
                prop_assert_eq!((from, to), (before.total(), after.total()));
                prop_assert!(to > from);
                prop_assert!(after.inputs().foreign_peak < before.inputs().foreign_peak);
            }
            BudgetChange::Shrank { from, to } => {
                prop_assert_eq!((from, to), (before.total(), after.total()));
                prop_assert!(to < from);
                prop_assert!(after.inputs().foreign_peak > before.inputs().foreign_peak);
            }
        }
    }
}
