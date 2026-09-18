use pgsteward_core::allocation::{
    AllocationTable, Desired, Entry, Holder, InstanceId, PreconditionError, ProxyId,
};
use pgsteward_core::tenant::TenantId;
use proptest::prelude::*;

fn instance(name: &str) -> InstanceId {
    InstanceId::new(name)
}

fn holder(user: &str, proxy: &str) -> Holder {
    Holder::new(TenantId::new(user, user), ProxyId::new(proxy))
}

#[test]
fn an_entry_within_the_budget_is_applied() {
    let mut table = AllocationTable::new();

    table
        .apply(
            &Entry::new().instance(
                instance("primary"),
                Desired::new(10)
                    .grant(holder("alice", "proxy-1"), 4)
                    .grant(holder("bob", "proxy-1"), 3),
            ),
        )
        .unwrap();

    assert_eq!(
        table.granted(&instance("primary"), &holder("alice", "proxy-1")),
        4
    );
    assert_eq!(
        table.granted(&instance("primary"), &holder("bob", "proxy-1")),
        3
    );
    assert_eq!(table.granted_total(&instance("primary")), 7);
    assert_eq!(table.budget(&instance("primary")), 10);
    assert_eq!(table.headroom(&instance("primary")), 3);
}

#[test]
fn an_entry_over_the_budget_is_rejected() {
    let mut table = AllocationTable::new();

    let error = table
        .apply(
            &Entry::new().instance(
                instance("primary"),
                Desired::new(10)
                    .grant(holder("alice", "proxy-1"), 6)
                    .grant(holder("bob", "proxy-1"), 5),
            ),
        )
        .unwrap_err();

    assert_eq!(
        error,
        PreconditionError::OverBudget {
            instance: instance("primary"),
            granted: 11,
            budget: 10,
        }
    );
    assert_eq!(table.granted_total(&instance("primary")), 0);
    assert_eq!(table.budget(&instance("primary")), 0);
}

#[test]
fn a_rejected_entry_moves_nothing_at_all() {
    let mut table = AllocationTable::new();
    table
        .apply(
            &Entry::new()
                .instance(
                    instance("primary"),
                    Desired::new(10).grant(holder("alice", "proxy-1"), 4),
                )
                .instance(
                    instance("replica"),
                    Desired::new(6).grant(holder("alice", "proxy-1"), 2),
                ),
        )
        .unwrap();

    table
        .apply(
            &Entry::new()
                .instance(
                    instance("primary"),
                    Desired::new(10).grant(holder("alice", "proxy-1"), 9),
                )
                .instance(
                    instance("replica"),
                    Desired::new(6).grant(holder("alice", "proxy-1"), 7),
                ),
        )
        .unwrap_err();

    assert_eq!(
        table.granted(&instance("primary"), &holder("alice", "proxy-1")),
        4
    );
    assert_eq!(
        table.granted(&instance("replica"), &holder("alice", "proxy-1")),
        2
    );
}

#[test]
fn each_instance_is_held_to_its_own_budget() {
    let mut table = AllocationTable::new();

    table
        .apply(
            &Entry::new()
                .instance(
                    instance("primary"),
                    Desired::new(10).grant(holder("alice", "proxy-1"), 10),
                )
                .instance(
                    instance("replica"),
                    Desired::new(6).grant(holder("alice", "proxy-1"), 6),
                ),
        )
        .unwrap();

    assert_eq!(table.granted_total(&instance("primary")), 10);
    assert_eq!(table.granted_total(&instance("replica")), 6);
}

#[test]
fn an_entry_replaces_the_grants_of_the_instance_it_names() {
    let mut table = AllocationTable::new();
    table
        .apply(
            &Entry::new()
                .instance(
                    instance("primary"),
                    Desired::new(10)
                        .grant(holder("alice", "proxy-1"), 4)
                        .grant(holder("bob", "proxy-1"), 3),
                )
                .instance(
                    instance("replica"),
                    Desired::new(6).grant(holder("alice", "proxy-1"), 2),
                ),
        )
        .unwrap();

    table
        .apply(&Entry::new().instance(
            instance("primary"),
            Desired::new(10).grant(holder("alice", "proxy-1"), 4),
        ))
        .unwrap();

    assert_eq!(
        table.granted(&instance("primary"), &holder("bob", "proxy-1")),
        0
    );
    assert_eq!(table.granted_total(&instance("primary")), 4);
    assert_eq!(
        table.granted(&instance("replica"), &holder("alice", "proxy-1")),
        2
    );
}

#[test]
fn a_grant_of_zero_holds_no_slot() {
    let mut table = AllocationTable::new();

    table
        .apply(
            &Entry::new().instance(
                instance("primary"),
                Desired::new(10)
                    .grant(holder("alice", "proxy-1"), 4)
                    .grant(holder("bob", "proxy-1"), 0),
            ),
        )
        .unwrap();

    assert_eq!(
        table.granted(&instance("primary"), &holder("bob", "proxy-1")),
        0
    );
    assert_eq!(table.holders(&instance("primary")).count(), 1);
}

#[test]
fn a_smaller_budget_arrives_with_the_grants_that_fit_it() {
    let mut table = AllocationTable::new();
    table
        .apply(&Entry::new().instance(
            instance("primary"),
            Desired::new(10).grant(holder("alice", "proxy-1"), 8),
        ))
        .unwrap();

    table
        .apply(&Entry::new().instance(
            instance("primary"),
            Desired::new(4).grant(holder("alice", "proxy-1"), 8),
        ))
        .unwrap_err();
    assert_eq!(table.budget(&instance("primary")), 10);

    table
        .apply(&Entry::new().instance(
            instance("primary"),
            Desired::new(4).grant(holder("alice", "proxy-1"), 4),
        ))
        .unwrap();

    assert_eq!(table.budget(&instance("primary")), 4);
    assert_eq!(table.granted_total(&instance("primary")), 4);
}

#[test]
fn a_proxy_reads_only_the_grants_it_was_given() {
    let mut table = AllocationTable::new();
    table
        .apply(
            &Entry::new()
                .instance(
                    instance("primary"),
                    Desired::new(10)
                        .grant(holder("alice", "proxy-1"), 4)
                        .grant(holder("alice", "proxy-2"), 3),
                )
                .instance(
                    instance("replica"),
                    Desired::new(6).grant(holder("bob", "proxy-1"), 2),
                ),
        )
        .unwrap();

    let grants: Vec<_> = table
        .grants_for(&ProxyId::new("proxy-1"))
        .map(|(instance, tenant, slots)| (instance.to_string(), tenant.to_string(), slots))
        .collect();

    assert_eq!(
        grants,
        vec![
            ("primary".to_owned(), "alice@alice".to_owned(), 4),
            ("replica".to_owned(), "bob@bob".to_owned(), 2),
        ]
    );
}

#[test]
fn an_instance_nobody_has_written_holds_nothing() {
    let table = AllocationTable::new();

    assert_eq!(table.budget(&instance("primary")), 0);
    assert_eq!(table.granted_total(&instance("primary")), 0);
    assert_eq!(table.headroom(&instance("primary")), 0);
    assert_eq!(
        table.granted(&instance("primary"), &holder("alice", "proxy-1")),
        0
    );
    assert_eq!(table.instances().count(), 0);
}

#[test]
fn applying_the_same_entry_again_moves_nothing() {
    let mut table = AllocationTable::new();
    let entry = Entry::new().instance(
        instance("primary"),
        Desired::new(10).grant(holder("alice", "proxy-1"), 4),
    );

    table.apply(&entry).unwrap();
    let before = table.clone();
    table.apply(&entry).unwrap();

    assert_eq!(table, before);
}

fn any_entry() -> impl Strategy<Value = Entry> {
    let names = prop::sample::select(vec!["primary", "replica"]);
    let holders = prop::collection::vec(
        (
            prop::sample::select(vec!["alice", "bob"]),
            prop::sample::select(vec!["proxy-1", "proxy-2"]),
            0u32..=6,
        ),
        0..4,
    );
    prop::collection::vec((names, 0u32..=10, holders), 0..3).prop_map(|instances| {
        instances
            .into_iter()
            .fold(Entry::new(), |entry, (name, budget, holders)| {
                let desired = holders
                    .into_iter()
                    .fold(Desired::new(budget), |desired, (user, proxy, slots)| {
                        desired.grant(holder(user, proxy), slots)
                    });
                entry.instance(instance(name), desired)
            })
    })
}

proptest! {
    #[test]
    fn no_instance_ever_holds_more_slots_than_its_budget(
        entries in prop::collection::vec(any_entry(), 1..12)
    ) {
        let mut table = AllocationTable::new();
        for entry in &entries {
            let _ = table.apply(entry);
            for name in table.instances() {
                prop_assert!(table.granted_total(name) <= table.budget(name));
            }
        }
    }
}
