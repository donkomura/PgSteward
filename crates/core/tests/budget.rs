use pgsteward_core::budget::{BudgetInputs, ServerLimits, TotalBudget};
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
