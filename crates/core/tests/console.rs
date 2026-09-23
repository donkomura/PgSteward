use std::num::NonZeroU32;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pgsteward_core::allocation::{AllocationTable, Desired, Entry, Holder, InstanceId, ProxyId};
use pgsteward_core::budget::{BudgetInputs, ServerLimits, TotalBudget};
use pgsteward_core::console::{
    ConsoleView, InstanceSnapshot, PoolSnapshot, ResultSet, show_budget, show_instances, show_pools,
};
use pgsteward_core::grant::TenantPolicy;
use pgsteward_core::pool::PoolStats;
use pgsteward_core::tenant::TenantId;
use pgsteward_protocol::backend::Column;
use postgres_protocol::message::backend::Message;

const PROXY: &str = "10.0.0.1:6432";
const OTHER_PROXY: &str = "10.0.0.2:6432";

fn instance(name: &str) -> InstanceId {
    InstanceId::new(name)
}

fn tenant(user: &str) -> TenantId {
    TenantId::new(user, "app")
}

fn policy(min: u32, max: u32) -> TenantPolicy {
    TenantPolicy {
        min,
        max,
        weight: NonZeroU32::new(1).expect("a positive weight"),
    }
}

fn stats(idle: usize, in_use: usize, opening: usize, waiting: usize) -> PoolStats {
    PoolStats {
        slots: idle + in_use + opening,
        idle,
        in_use,
        opening,
        closing: 0,
        waiting,
    }
}

fn budget(max_connections: u32, foreign_peak: u32, margin: u32) -> TotalBudget {
    TotalBudget::derive(BudgetInputs {
        limits: ServerLimits {
            max_connections,
            superuser_reserved_connections: 3,
            reserved_connections: 2,
        },
        foreign_peak,
        margin,
    })
}

fn view(
    table: AllocationTable,
    pools: Vec<PoolSnapshot>,
    instances: Vec<InstanceSnapshot>,
) -> ConsoleView {
    ConsoleView {
        proxy: ProxyId::new(PROXY),
        pool_mode: "transaction".to_owned(),
        pools,
        instances,
        table,
    }
}

fn table(entry: &Entry) -> AllocationTable {
    let mut table = AllocationTable::new();
    table.apply(entry).expect("an entry within the budget");
    table
}

fn row(set: &ResultSet, at: usize) -> Vec<Option<&str>> {
    set.rows[at].iter().map(Option::as_deref).collect()
}

#[test]
fn a_pool_puts_the_desired_state_next_to_what_the_instance_holds() {
    let table = table(&Entry::new().instance(
        instance("primary"),
        Desired::new(50).grant(Holder::new(tenant("alice"), ProxyId::new(PROXY)), 7),
    ));
    let view = view(
        table,
        vec![PoolSnapshot {
            instance: instance("primary"),
            tenant: tenant("alice"),
            policy: Some(policy(2, 20)),
            stats: stats(1, 3, 1, 4),
        }],
        Vec::new(),
    );

    let set = show_pools(&view);

    assert_eq!(
        set.columns,
        vec![
            Column::text("database"),
            Column::text("user"),
            Column::text("instance"),
            Column::count("cl_waiting"),
            Column::count("sv_active"),
            Column::count("sv_idle"),
            Column::count("sv_login"),
            Column::text("pool_mode"),
            Column::count("granted"),
            Column::count("actual"),
            Column::count("min"),
            Column::count("max"),
            Column::count("demand"),
        ]
    );
    assert_eq!(
        row(&set, 0),
        vec![
            Some("app"),
            Some("alice"),
            Some("primary"),
            Some("4"),
            Some("3"),
            Some("1"),
            Some("1"),
            Some("transaction"),
            Some("7"),
            Some("4"),
            Some("2"),
            Some("20"),
            Some("8"),
        ]
    );
}

#[test]
fn a_pool_no_tenant_rule_covers_leaves_its_share_null() {
    let view = view(
        AllocationTable::new(),
        vec![PoolSnapshot {
            instance: instance("primary"),
            tenant: tenant("alice"),
            policy: None,
            stats: PoolStats::default(),
        }],
        Vec::new(),
    );

    let set = show_pools(&view);

    assert_eq!(row(&set, 0)[10..12], [None, None]);
    assert_eq!(row(&set, 0)[8], Some("0"));
}

#[test]
fn pools_come_out_in_one_order_whatever_order_they_were_given_in() {
    let view = view(
        AllocationTable::new(),
        vec![
            PoolSnapshot {
                instance: instance("replica"),
                tenant: tenant("alice"),
                policy: None,
                stats: PoolStats::default(),
            },
            PoolSnapshot {
                instance: instance("primary"),
                tenant: tenant("bob"),
                policy: None,
                stats: PoolStats::default(),
            },
            PoolSnapshot {
                instance: instance("primary"),
                tenant: tenant("alice"),
                policy: None,
                stats: PoolStats::default(),
            },
        ],
        Vec::new(),
    );

    let set = show_pools(&view);

    let named: Vec<_> = set
        .rows
        .iter()
        .map(|row| (row[2].clone().unwrap(), row[1].clone().unwrap()))
        .collect();
    assert_eq!(
        named,
        vec![
            ("primary".to_owned(), "alice".to_owned()),
            ("primary".to_owned(), "bob".to_owned()),
            ("replica".to_owned(), "alice".to_owned()),
        ]
    );
}

#[test]
fn the_budget_names_every_holder_of_every_instance() {
    let table = table(
        &Entry::new()
            .instance(
                instance("primary"),
                Desired::new(50)
                    .grant(Holder::new(tenant("alice"), ProxyId::new(PROXY)), 7)
                    .grant(Holder::new(tenant("bob"), ProxyId::new(PROXY)), 5),
            )
            .instance(
                instance("replica"),
                Desired::new(20).grant(Holder::new(tenant("alice"), ProxyId::new(PROXY)), 2),
            ),
    );
    let view = view(
        table,
        vec![PoolSnapshot {
            instance: instance("primary"),
            tenant: tenant("alice"),
            policy: Some(policy(2, 20)),
            stats: stats(2, 4, 0, 0),
        }],
        Vec::new(),
    );

    let set = show_budget(&view);

    assert_eq!(
        set.columns,
        vec![
            Column::text("instance"),
            Column::text("tenant"),
            Column::text("proxy"),
            Column::count("granted"),
            Column::count("actual"),
        ]
    );
    assert_eq!(
        row(&set, 0),
        vec![
            Some("primary"),
            Some("alice@app"),
            Some(PROXY),
            Some("7"),
            Some("6"),
        ]
    );
    assert_eq!(
        row(&set, 1),
        vec![
            Some("primary"),
            Some("bob@app"),
            Some(PROXY),
            Some("5"),
            Some("0"),
        ]
    );
    assert_eq!(
        row(&set, 3),
        vec![
            Some("replica"),
            Some("alice@app"),
            Some(PROXY),
            Some("2"),
            Some("0"),
        ]
    );
}

#[test]
fn the_budget_gives_the_slots_no_one_holds_a_row_of_their_own() {
    let table = table(&Entry::new().instance(
        instance("primary"),
        Desired::new(50).grant(Holder::new(tenant("alice"), ProxyId::new(PROXY)), 7),
    ));
    let view = view(table, Vec::new(), Vec::new());

    let set = show_budget(&view);

    assert_eq!(
        row(&set, 1),
        vec![Some("primary"), None, None, Some("43"), None]
    );
}

#[test]
fn the_budget_leaves_the_connections_of_another_proxy_unclaimed() {
    let table = table(&Entry::new().instance(
        instance("primary"),
        Desired::new(50).grant(Holder::new(tenant("alice"), ProxyId::new(OTHER_PROXY)), 7),
    ));
    let view = view(table, Vec::new(), Vec::new());

    let set = show_budget(&view);

    assert_eq!(
        row(&set, 0),
        vec![
            Some("primary"),
            Some("alice@app"),
            Some(OTHER_PROXY),
            Some("7"),
            None,
        ]
    );
}

#[test]
fn an_instance_explains_its_total_budget_by_the_parts_it_was_derived_from() {
    let table = table(
        &Entry::new().instance(
            instance("primary"),
            Desired::new(69)
                .grant(Holder::new(tenant("alice"), ProxyId::new(PROXY)), 7)
                .grant(Holder::new(tenant("bob"), ProxyId::new(PROXY)), 5),
        ),
    );
    let view = view(
        table,
        vec![PoolSnapshot {
            instance: instance("primary"),
            tenant: tenant("alice"),
            policy: Some(policy(2, 20)),
            stats: stats(2, 4, 0, 0),
        }],
        vec![InstanceSnapshot {
            instance: instance("primary"),
            budget: budget(100, 10, 16),
        }],
    );

    let set = show_instances(&view);

    assert_eq!(
        set.columns,
        vec![
            Column::text("instance"),
            Column::count("max_connections"),
            Column::count("reserved"),
            Column::count("foreign_peak"),
            Column::count("margin"),
            Column::count("shortfall"),
            Column::count("total_budget"),
            Column::count("granted"),
            Column::count("actual"),
            Column::count("tenants"),
        ]
    );
    assert_eq!(
        row(&set, 0),
        vec![
            Some("primary"),
            Some("100"),
            Some("5"),
            Some("10"),
            Some("16"),
            Some("0"),
            Some("69"),
            Some("12"),
            Some("6"),
            Some("2"),
        ]
    );
}

#[test]
fn an_instance_whose_deductions_outgrow_max_connections_reports_the_shortfall() {
    let view = view(
        AllocationTable::new(),
        Vec::new(),
        vec![InstanceSnapshot {
            instance: instance("primary"),
            budget: budget(10, 8, 5),
        }],
    );

    let set = show_instances(&view);

    assert_eq!(row(&set, 0)[5..7], [Some("8"), Some("0")]);
}

#[test]
fn a_result_set_is_written_as_its_columns_then_its_rows_then_a_command_tag() {
    let view = view(
        AllocationTable::new(),
        vec![PoolSnapshot {
            instance: instance("primary"),
            tenant: tenant("alice"),
            policy: None,
            stats: PoolStats::default(),
        }],
        Vec::new(),
    );
    let mut out = BytesMut::new();

    show_pools(&view).encode(&mut out);

    let mut messages = Vec::new();
    while let Some(message) = Message::parse(&mut out).expect("well-formed backend messages") {
        messages.push(message);
    }
    assert!(out.is_empty(), "bytes follow the result set");
    let mut messages = messages.into_iter();
    let Some(Message::RowDescription(description)) = messages.next() else {
        panic!("the result set opens with a row description");
    };
    assert_eq!(
        description.fields().count().expect("well-formed fields"),
        13
    );
    let Some(Message::DataRow(data)) = messages.next() else {
        panic!("the columns are followed by one row per pool");
    };
    let values: Vec<_> = data
        .ranges()
        .map(|range| {
            Ok(range.map(|range| {
                String::from_utf8(data.buffer()[range].to_vec()).expect("UTF-8 values")
            }))
        })
        .collect()
        .expect("well-formed row values");
    assert_eq!(values[10], None);
    assert_eq!(values[3], Some("0".to_owned()));
    let Some(Message::CommandComplete(complete)) = messages.next() else {
        panic!("the rows are followed by a command tag");
    };
    assert_eq!(complete.tag().expect("a UTF-8 tag"), "SHOW");
    assert!(messages.next().is_none(), "nothing follows the command tag");
}

#[test]
fn a_console_that_holds_nothing_answers_with_no_rows() {
    let view = view(AllocationTable::new(), Vec::new(), Vec::new());

    assert!(show_pools(&view).rows.is_empty());
    assert!(show_budget(&view).rows.is_empty());
    assert!(show_instances(&view).rows.is_empty());
}
