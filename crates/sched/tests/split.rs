use std::collections::BTreeMap;

use pgsteward_sched::split::{Share, split};
use proptest::prelude::*;

fn share(key: u32, demand: u32, current: u32) -> Share<u32> {
    Share {
        key,
        demand,
        current,
        may_release: true,
    }
}

fn retained(entry: Share<u32>) -> Share<u32> {
    Share {
        may_release: false,
        ..entry
    }
}

fn of(granted: &BTreeMap<u32, u32>, key: u32) -> u32 {
    granted.get(&key).copied().unwrap_or(0)
}

#[test]
fn a_single_proxy_is_given_the_whole_grant_up_to_its_demand() {
    let granted = split(5, &[share(1, 8, 0)]);

    assert_eq!(of(&granted, 1), 5);
}

#[test]
fn nothing_is_given_beyond_what_the_proxies_need() {
    let granted = split(10, &[share(1, 3, 0), share(2, 4, 0)]);

    assert_eq!(of(&granted, 1), 3);
    assert_eq!(of(&granted, 2), 4);
}

#[test]
fn a_proxy_keeps_its_current_grant_while_the_rest_goes_to_the_one_still_short() {
    let granted = split(6, &[share(1, 3, 3), share(2, 5, 0)]);

    assert_eq!(of(&granted, 1), 3);
    assert_eq!(of(&granted, 2), 3);
}

#[test]
fn the_rest_goes_first_to_the_proxy_furthest_below_its_demand() {
    let granted = split(4, &[share(1, 4, 0), share(2, 2, 0)]);

    assert_eq!(of(&granted, 1), 3);
    assert_eq!(of(&granted, 2), 1);
}

#[test]
fn a_shrinking_grant_is_taken_first_from_the_proxy_holding_the_most() {
    let granted = split(6, &[share(1, 5, 5), share(2, 5, 3)]);

    assert_eq!(of(&granted, 1), 3);
    assert_eq!(of(&granted, 2), 3);
}

#[test]
fn a_shrinking_grant_is_taken_first_from_what_is_held_beyond_demand() {
    let granted = split(5, &[share(1, 5, 5), retained(share(2, 0, 3))]);

    assert_eq!(of(&granted, 1), 5);
    assert_eq!(of(&granted, 2), 0);
}

#[test]
fn a_grant_shrinking_by_one_releases_an_idle_slot_rather_than_a_busy_one() {
    let granted = split(7, &[share(1, 5, 5), retained(share(2, 0, 3))]);

    assert_eq!(of(&granted, 1), 5);
    assert_eq!(of(&granted, 2), 2);
}

#[test]
fn a_proxy_within_its_release_delay_keeps_a_grant_above_its_demand() {
    let granted = split(5, &[retained(share(1, 0, 2)), share(2, 3, 0)]);

    assert_eq!(of(&granted, 1), 2);
    assert_eq!(of(&granted, 2), 3);
}

#[test]
fn a_proxy_past_its_release_delay_gives_up_what_it_does_not_use() {
    let granted = split(5, &[share(1, 0, 2), share(2, 5, 0)]);

    assert_eq!(of(&granted, 1), 0);
    assert_eq!(of(&granted, 2), 5);
}

fn shares() -> impl Strategy<Value = Vec<Share<u32>>> {
    prop::collection::vec((0u32..20, 0u32..20, any::<bool>()), 0..6).prop_map(|entries| {
        entries
            .into_iter()
            .enumerate()
            .map(|(key, (demand, current, may_release))| Share {
                key: u32::try_from(key).unwrap(),
                demand,
                current,
                may_release,
            })
            .collect()
    })
}

proptest! {
    #[test]
    fn everything_granted_is_handed_out_up_to_the_total_need(granted in 0u32..100, shares in shares()) {
        let split = split(granted, &shares);
        let need: u32 = shares.iter().map(Share::need).sum();

        prop_assert_eq!(split.values().sum::<u32>(), granted.min(need));
    }

    #[test]
    fn no_proxy_is_given_more_than_it_needs(granted in 0u32..100, shares in shares()) {
        let split = split(granted, &shares);

        for share in &shares {
            prop_assert!(of(&split, share.key) <= share.need());
        }
    }

    #[test]
    fn no_slot_moves_while_the_grant_covers_what_is_kept(granted in 0u32..100, shares in shares()) {
        let kept: u32 = shares.iter().map(|share| share.current.min(share.need())).sum();
        prop_assume!(granted >= kept);
        let split = split(granted, &shares);

        for share in &shares {
            prop_assert!(of(&split, share.key) >= share.current.min(share.need()));
        }
    }

    #[test]
    fn the_same_input_is_split_the_same_way(granted in 0u32..100, shares in shares()) {
        prop_assert_eq!(split(granted, &shares), split(granted, &shares));
    }
}
