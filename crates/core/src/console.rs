use std::collections::BTreeSet;
use std::fmt;

use bytes::BytesMut;
use pgsteward_protocol::backend::{
    Column, encode_command_complete, encode_data_row, encode_row_description,
};

use crate::allocation::{AllocationTable, Holder, InstanceId, ProxyId};
use crate::budget::TotalBudget;
use crate::grant::TenantPolicy;
use crate::pool::PoolStats;
use crate::tenant::TenantId;

/// What every table the console writes reports as its command tag, the way
/// PostgreSQL answers a `SHOW`.
const TAG: &str = "SHOW";

const POOL_COLUMNS: [Column<'static>; 13] = [
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
];

const BUDGET_COLUMNS: [Column<'static>; 5] = [
    Column::text("instance"),
    Column::text("tenant"),
    Column::text("proxy"),
    Column::count("granted"),
    Column::count("actual"),
];

const INSTANCE_COLUMNS: [Column<'static>; 10] = [
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
];

/// One pool of this node, as it stood when the view was taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSnapshot {
    pub instance: InstanceId,
    pub tenant: TenantId,
    /// The share the cluster configuration gives this tenant, or `None` when
    /// no rule covers it.
    pub policy: Option<TenantPolicy>,
    pub stats: PoolStats,
}

/// One instance this node is configured with, and the total budget derived
/// for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceSnapshot {
    pub instance: InstanceId,
    pub budget: TotalBudget,
}

/// Everything the console reads to write one table.
///
/// It is taken at one moment and nothing in it is read again while the rows
/// are written, so a table never mixes two states of the node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsoleView {
    pub proxy: ProxyId,
    pub pool_mode: String,
    pub pools: Vec<PoolSnapshot>,
    pub instances: Vec<InstanceSnapshot>,
    pub table: AllocationTable,
}

impl ConsoleView {
    /// The connections this node holds on `instance` for `tenant`.
    fn actual_of(&self, instance: &InstanceId, tenant: &TenantId) -> usize {
        self.sum(|pool| &pool.instance == instance && &pool.tenant == tenant)
    }

    /// The connections this node holds on `instance`, over every tenant.
    fn actual_on(&self, instance: &InstanceId) -> usize {
        self.sum(|pool| &pool.instance == instance)
    }

    fn sum(&self, of_interest: impl Fn(&PoolSnapshot) -> bool) -> usize {
        self.pools
            .iter()
            .filter(|pool| of_interest(pool))
            .map(|pool| pool.stats.actual())
            .sum()
    }

    fn tenants_on(&self, instance: &InstanceId) -> usize {
        self.table
            .holders(instance)
            .map(|(holder, _)| holder.tenant())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

/// A table the console answers a `SHOW` with. Values travel in the text
/// format, and a `None` is a null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<Column<'static>>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl ResultSet {
    pub fn encode(&self, out: &mut BytesMut) {
        encode_row_description(&self.columns, out);
        for row in &self.rows {
            let values: Vec<Option<&str>> = row.iter().map(Option::as_deref).collect();
            encode_data_row(&values, out);
        }
        encode_command_complete(TAG, out);
    }
}

/// The pools this node holds: the slots each was granted beside the
/// connections it actually has, and the demand that asked for them.
#[must_use]
pub fn show_pools(view: &ConsoleView) -> ResultSet {
    let mut pools: Vec<&PoolSnapshot> = view.pools.iter().collect();
    pools.sort_by(|left, right| {
        (&left.instance, &left.tenant).cmp(&(&right.instance, &right.tenant))
    });
    let rows = pools
        .into_iter()
        .map(|pool| {
            let holder = Holder::new(pool.tenant.clone(), view.proxy.clone());
            Row::new()
                .value(pool.tenant.database())
                .value(pool.tenant.user())
                .value(&pool.instance)
                .value(pool.stats.waiting)
                .value(pool.stats.in_use)
                .value(pool.stats.idle)
                .value(pool.stats.opening)
                .value(&view.pool_mode)
                .value(view.table.granted(&pool.instance, &holder))
                .value(pool.stats.actual())
                .maybe(pool.policy.map(|policy| policy.min))
                .maybe(pool.policy.map(|policy| policy.max))
                .value(pool.stats.demand())
                .build()
        })
        .collect();
    ResultSet {
        columns: POOL_COLUMNS.to_vec(),
        rows,
    }
}

/// The whole allocation table, instance by instance: what each holder is
/// granted, and the slots that are granted to no one.
///
/// The actual connections are known for this node alone, so a grant held by
/// another proxy leaves that column null rather than claiming a zero.
#[must_use]
pub fn show_budget(view: &ConsoleView) -> ResultSet {
    let mut rows = Vec::new();
    for instance in view.table.instances() {
        for (holder, slots) in view.table.holders(instance) {
            let actual =
                (holder.proxy() == &view.proxy).then(|| view.actual_of(instance, holder.tenant()));
            rows.push(
                Row::new()
                    .value(instance)
                    .value(holder.tenant())
                    .value(holder.proxy())
                    .value(slots)
                    .maybe(actual)
                    .build(),
            );
        }
        rows.push(
            Row::new()
                .value(instance)
                .null()
                .null()
                .value(view.table.headroom(instance))
                .null()
                .build(),
        );
    }
    ResultSet {
        columns: BUDGET_COLUMNS.to_vec(),
        rows,
    }
}

/// Each instance's total budget together with the parts it was derived from,
/// so that why the budget is what it is takes one command to answer.
#[must_use]
pub fn show_instances(view: &ConsoleView) -> ResultSet {
    let mut instances: Vec<&InstanceSnapshot> = view.instances.iter().collect();
    instances.sort_by(|left, right| left.instance.cmp(&right.instance));
    let rows = instances
        .into_iter()
        .map(|snapshot| {
            let instance = &snapshot.instance;
            let inputs = snapshot.budget.inputs();
            Row::new()
                .value(instance)
                .value(inputs.limits.max_connections)
                .value(inputs.limits.reserved())
                .value(inputs.foreign_peak)
                .value(inputs.margin)
                .value(snapshot.budget.shortfall())
                .value(snapshot.budget.total())
                .value(view.table.granted_total(instance))
                .value(view.actual_on(instance))
                .value(view.tenants_on(instance))
                .build()
        })
        .collect();
    ResultSet {
        columns: INSTANCE_COLUMNS.to_vec(),
        rows,
    }
}

/// Collects one row's cells in the order its columns were described.
#[derive(Debug, Default)]
struct Row(Vec<Option<String>>);

impl Row {
    fn new() -> Self {
        Self::default()
    }

    fn value(mut self, value: impl fmt::Display) -> Self {
        self.0.push(Some(value.to_string()));
        self
    }

    /// A cell the console has no answer for, rather than a zero it would have
    /// to stand behind.
    fn maybe(mut self, value: Option<impl fmt::Display>) -> Self {
        self.0.push(value.map(|value| value.to_string()));
        self
    }

    fn null(mut self) -> Self {
        self.0.push(None);
        self
    }

    fn build(self) -> Vec<Option<String>> {
        self.0
    }
}
