use std::num::NonZeroU32;

use pgsteward_sched::fair::WeightedMaxMinFair;
use pgsteward_sched::{Allocator, Claim, Grants};
use proptest::prelude::*;

fn fair() -> WeightedMaxMinFair {
    WeightedMaxMinFair::default()
}

fn claim(key: u32, min: u32, max: u32, weight: u32, demand: u32) -> Claim<u32> {
    Claim {
        key,
        min,
        max,
        weight: NonZeroU32::new(weight).unwrap(),
        demand,
        current: 0,
        may_release: true,
    }
}

fn open(key: u32, min: u32, weight: u32, demand: u32) -> Claim<u32> {
    claim(key, min, u32::MAX, weight, demand)
}

fn holding(entry: Claim<u32>, current: u32) -> Claim<u32> {
    Claim { current, ..entry }
}

fn not_yet_releasing(entry: Claim<u32>) -> Claim<u32> {
    Claim {
        may_release: false,
        ..entry
    }
}

fn carry(claims: &[Claim<u32>], grants: &Grants<u32>) -> Vec<Claim<u32>> {
    claims
        .iter()
        .map(|entry| holding(*entry, grants.get(&entry.key)))
        .collect()
}

#[test]
fn a_tenant_without_demand_is_granted_nothing_despite_its_minimum() {
    let grants = fair().allocate(150, &[open(1, 30, 1, 0)]);

    assert_eq!(grants.get(&1), 0);
    assert_eq!(grants.total(), 0);
    assert!(grants.guarantees_met());
}

#[test]
fn a_single_tenant_is_granted_what_it_asks_for() {
    let grants = fair().allocate(150, &[open(1, 0, 1, 40)]);

    assert_eq!(grants.get(&1), 40);
    assert_eq!(grants.total(), 40);
}

#[test]
fn the_guarantee_is_met_before_the_surplus_is_shared() {
    let grants = fair().allocate(10, &[open(1, 6, 1, 10), open(2, 0, 1, 10)]);

    assert_eq!(grants.get(&1), 8);
    assert_eq!(grants.get(&2), 2);
    assert_eq!(grants.total(), 10);
}

#[test]
fn the_surplus_follows_the_weights() {
    let grants = fair().allocate(9, &[open(1, 0, 1, 100), open(2, 0, 2, 100)]);

    assert_eq!(grants.get(&1), 3);
    assert_eq!(grants.get(&2), 6);
}

#[test]
fn the_surplus_left_by_a_capped_tenant_goes_to_the_others() {
    let grants = fair().allocate(10, &[claim(1, 0, 2, 1, 100), open(2, 0, 1, 100)]);

    assert_eq!(grants.get(&1), 2);
    assert_eq!(grants.get(&2), 8);
}

#[test]
fn demand_below_the_maximum_caps_the_grant() {
    let grants = fair().allocate(10, &[claim(1, 0, 8, 1, 3), open(2, 0, 1, 100)]);

    assert_eq!(grants.get(&1), 3);
    assert_eq!(grants.get(&2), 7);
}

#[test]
fn a_budget_smaller_than_the_guarantees_shrinks_them_by_weight() {
    let grants = fair().allocate(3, &[open(1, 4, 1, 10), open(2, 4, 2, 10)]);

    assert_eq!(grants.get(&1), 1);
    assert_eq!(grants.get(&2), 2);
    assert_eq!(grants.total(), 3);
    assert!(!grants.guarantees_met());
    assert_eq!(grants.unmet_guarantee(), 5);
}

#[test]
fn a_guarantee_beyond_the_demand_is_not_reported_as_unmet() {
    let grants = fair().allocate(10, &[open(1, 30, 1, 4)]);

    assert_eq!(grants.get(&1), 4);
    assert!(grants.guarantees_met());
}

#[test]
fn an_empty_budget_grants_nothing() {
    let grants = fair().allocate(0, &[open(1, 4, 1, 10)]);

    assert_eq!(grants.total(), 0);
    assert_eq!(grants.unmet_guarantee(), 4);
}

#[test]
fn thirty_thousand_tenants_share_a_budget_of_one_hundred() {
    let claims: Vec<Claim<u32>> = (0..30_000).map(|key| claim(key, 0, 20, 1, 5)).collect();

    let grants = fair().allocate(100, &claims);

    assert_eq!(grants.total(), 100);
    assert_eq!(
        grants.iter().filter(|&(_, granted)| granted > 0).count(),
        100
    );
}

#[test]
fn three_competing_tenants_do_not_starve_one_another() {
    let claims = [open(1, 0, 1, 100), open(2, 0, 1, 100), open(3, 0, 1, 100)];

    let grants = fair().allocate(150, &claims);

    assert_eq!(grants.get(&1), 50);
    assert_eq!(grants.get(&2), 50);
    assert_eq!(grants.get(&3), 50);
}

#[test]
fn a_tenant_that_stopped_asking_keeps_what_it_holds_while_nobody_else_needs_it() {
    let grants = fair().allocate(10, &[holding(open(1, 0, 1, 0), 5)]);

    assert_eq!(grants.get(&1), 4);
}

#[test]
fn what_is_no_longer_needed_is_given_up_a_little_at_a_time() {
    let mut claims = vec![holding(open(1, 0, 1, 0), 5)];
    let mut handed_back = Vec::new();

    for _ in 0..6 {
        let grants = fair().allocate(10, &claims);
        handed_back.push(grants.get(&1));
        claims = carry(&claims, &grants);
    }

    assert_eq!(handed_back, vec![4, 3, 2, 1, 0, 0]);
}

#[test]
fn demand_outranks_what_another_tenant_still_holds() {
    let claims = [holding(open(1, 0, 1, 0), 5), open(2, 0, 1, 5)];

    let grants = fair().allocate(5, &claims);

    assert_eq!(grants.get(&1), 0);
    assert_eq!(grants.get(&2), 5);
}

#[test]
fn what_is_held_is_kept_out_of_the_budget_the_others_leave() {
    let claims = [holding(open(1, 0, 1, 0), 5), open(2, 0, 1, 3)];

    let grants = fair().allocate(5, &claims);

    assert_eq!(grants.get(&1), 2);
    assert_eq!(grants.get(&2), 3);
}

#[test]
fn what_is_held_above_the_maximum_is_not_kept() {
    let grants = fair().allocate(10, &[holding(claim(1, 0, 2, 1, 0), 5)]);

    assert_eq!(grants.get(&1), 2);
}

#[test]
fn a_slot_does_not_move_when_the_tie_could_go_either_way() {
    let claims = [open(1, 0, 1, 1), holding(open(2, 0, 1, 1), 1)];

    let grants = fair().allocate(1, &claims);

    assert_eq!(grants.get(&1), 0);
    assert_eq!(grants.get(&2), 1);
}

#[test]
fn an_allocator_that_releases_nothing_keeps_every_idle_slot() {
    let grants = WeightedMaxMinFair::new(0).allocate(10, &[holding(open(1, 0, 1, 0), 5)]);

    assert_eq!(grants.get(&1), 5);
}

#[test]
fn an_allocator_that_releases_everything_keeps_no_idle_slot() {
    let grants = WeightedMaxMinFair::new(u32::MAX).allocate(10, &[holding(open(1, 0, 1, 0), 5)]);

    assert_eq!(grants.get(&1), 0);
}

#[test]
fn a_holder_that_may_not_release_yet_keeps_every_idle_slot() {
    let grants = fair().allocate(10, &[not_yet_releasing(holding(open(1, 0, 1, 0), 5))]);

    assert_eq!(grants.get(&1), 5);
}

#[test]
fn demand_outranks_a_holder_that_may_not_release_yet() {
    let claims = [
        not_yet_releasing(holding(open(1, 0, 1, 0), 5)),
        open(2, 0, 1, 5),
    ];

    let grants = fair().allocate(5, &claims);

    assert_eq!(grants.get(&1), 0);
    assert_eq!(grants.get(&2), 5);
}

#[test]
fn a_holder_that_may_not_release_yet_keeps_only_what_the_others_leave() {
    let claims = [
        not_yet_releasing(holding(open(1, 0, 1, 0), 5)),
        open(2, 0, 1, 3),
    ];

    let grants = fair().allocate(5, &claims);

    assert_eq!(grants.get(&1), 2);
    assert_eq!(grants.get(&2), 3);
}

fn arb_sized_claims(count: std::ops::Range<usize>) -> impl Strategy<Value = Vec<Claim<u32>>> {
    prop::collection::vec(
        (
            (0u32..=20, 0u32..=30, 1u32..=4, 0u32..=50, 0u32..=20),
            any::<bool>(),
        ),
        count,
    )
    .prop_map(|rows| {
        rows.into_iter()
            .enumerate()
            .map(
                |(index, ((min, span, weight, demand, current), may_release))| {
                    let key = u32::try_from(index).unwrap();
                    Claim {
                        may_release,
                        ..holding(
                            claim(key, min, min.saturating_add(span), weight, demand),
                            current,
                        )
                    }
                },
            )
            .collect()
    })
}

fn arb_claims() -> impl Strategy<Value = Vec<Claim<u32>>> {
    arb_sized_claims(0..8)
}

fn floor(entry: &Claim<u32>) -> u32 {
    entry.min.min(entry.demand.min(entry.max))
}

proptest! {
    #[test]
    fn the_grants_never_exceed_the_budget(
        budget in 0u32..=200,
        release in 0u32..=3,
        claims in arb_claims(),
    ) {
        let grants = WeightedMaxMinFair::new(release).allocate(budget, &claims);
        prop_assert!(grants.total() <= budget);
        prop_assert_eq!(grants.total(), grants.iter().map(|(_, granted)| granted).sum::<u32>());
    }

    #[test]
    fn no_tenant_is_granted_more_than_its_maximum(
        budget in 0u32..=200,
        release in 0u32..=3,
        claims in arb_claims(),
    ) {
        let grants = WeightedMaxMinFair::new(release).allocate(budget, &claims);
        for entry in &claims {
            prop_assert!(grants.get(&entry.key) <= entry.max);
        }
    }

    #[test]
    fn no_tenant_is_granted_more_than_it_asks_for_or_already_holds(
        budget in 0u32..=200,
        release in 0u32..=3,
        claims in arb_claims(),
    ) {
        let grants = WeightedMaxMinFair::new(release).allocate(budget, &claims);
        for entry in &claims {
            let kept = if entry.may_release {
                entry.current.saturating_sub(release)
            } else {
                entry.current
            };
            prop_assert!(grants.get(&entry.key) <= entry.demand.max(kept).min(entry.max));
        }
    }

    #[test]
    fn every_guarantee_is_met_when_the_budget_covers_them(
        claims in arb_claims(),
        release in 0u32..=3,
        spare in 0u32..=200,
    ) {
        let budget = claims.iter().map(floor).sum::<u32>() + spare;

        let grants = WeightedMaxMinFair::new(release).allocate(budget, &claims);
        for entry in &claims {
            prop_assert!(grants.get(&entry.key) >= floor(entry));
        }
        prop_assert!(grants.guarantees_met());
    }

    #[test]
    fn the_unmet_guarantee_is_what_the_budget_could_not_cover(
        budget in 0u32..=200,
        release in 0u32..=3,
        claims in arb_claims(),
    ) {
        let grants = WeightedMaxMinFair::new(release).allocate(budget, &claims);
        let unmet: u32 = claims
            .iter()
            .map(|entry| floor(entry).saturating_sub(grants.get(&entry.key)))
            .sum();
        prop_assert_eq!(grants.unmet_guarantee(), unmet);
        prop_assert_eq!(grants.guarantees_met(), unmet == 0);
    }

    #[test]
    fn the_order_of_the_claims_does_not_change_the_grants(
        budget in 0u32..=200,
        release in 0u32..=3,
        claims in arb_claims(),
        seed in any::<u64>(),
    ) {
        let mut shuffled = claims.clone();
        let mut state = seed | 1;
        for index in (1..shuffled.len()).rev() {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let pick = usize::try_from(state >> 33).unwrap() % (index + 1);
            shuffled.swap(index, pick);
        }

        let allocator = WeightedMaxMinFair::new(release);
        let grants = allocator.allocate(budget, &claims);
        let again = allocator.allocate(budget, &shuffled);
        for entry in &claims {
            prop_assert_eq!(grants.get(&entry.key), again.get(&entry.key));
        }
        prop_assert_eq!(grants.total(), again.total());
    }

    #[test]
    fn a_larger_budget_never_takes_a_grant_away(
        budget in 0u32..=200,
        extra in 0u32..=50,
        release in 0u32..=3,
        claims in arb_claims(),
    ) {
        let allocator = WeightedMaxMinFair::new(release);
        let before = allocator.allocate(budget, &claims);
        let after = allocator.allocate(budget + extra, &claims);
        for entry in &claims {
            prop_assert!(after.get(&entry.key) >= before.get(&entry.key));
        }
    }

    #[test]
    fn more_demand_never_takes_a_grant_away_from_the_tenant_that_asked(
        budget in 0u32..=200,
        release in 0u32..=3,
        claims in arb_sized_claims(1..8),
        extra in 1u32..=20,
        pick in any::<prop::sample::Index>(),
    ) {
        let chosen = pick.index(claims.len());

        let allocator = WeightedMaxMinFair::new(release);
        let before = allocator.allocate(budget, &claims);
        let mut grown = claims.clone();
        grown[chosen].demand = grown[chosen].demand.saturating_add(extra);
        let after = allocator.allocate(budget, &grown);

        prop_assert!(after.get(&claims[chosen].key) >= before.get(&claims[chosen].key));
    }

    #[test]
    fn a_round_that_changes_nothing_never_hands_out_a_slot_twice(
        budget in 0u32..=200,
        release in 0u32..=3,
        claims in arb_claims(),
    ) {
        let allocator = WeightedMaxMinFair::new(release);
        let first = allocator.allocate(budget, &claims);
        let second = allocator.allocate(budget, &carry(&claims, &first));

        for entry in &claims {
            let settled = first.get(&entry.key);
            prop_assert!(second.get(&entry.key) <= settled);
            if settled <= entry.demand.min(entry.max) {
                prop_assert_eq!(second.get(&entry.key), settled);
            }
        }
    }
}
