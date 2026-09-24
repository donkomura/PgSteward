use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::io;

use bytes::BytesMut;
use pgsteward_protocol::admin::{AdminCommand, TenantSettings, parse};
use pgsteward_protocol::backend::{
    Column, ErrorResponse, encode_command_complete, encode_data_row, encode_empty_query_response,
    encode_error_response, encode_parameter_status, encode_ready_for_query, encode_row_description,
    sqlstate,
};
use pgsteward_protocol::framing::{FrameError, MAX_MESSAGE, decode_frame};
use pgsteward_protocol::frontend::{FrontendError, decode_query};
use pgsteward_protocol::message::{FrontendTag, MessageError, TransactionStatus};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::allocation::{AllocationTable, Holder, InstanceId, ProxyId};
use crate::budget::TotalBudget;
use crate::grant::TenantPolicy;
use crate::policy::{PolicyChange, SettingError};
use crate::pool::PoolStats;
use crate::session::ClientSession;
use crate::tenant::TenantId;

/// What every table the console writes reports as its command tag, the way
/// PostgreSQL answers a `SHOW`.
const TAG: &str = "SHOW";

/// What a written setting reports as its command tag, the way PostgreSQL
/// answers a `SET`.
const SET_TAG: &str = "SET";

/// The database a client names to reach the admin console instead of an
/// instance. It is reserved: a tenant rule that names it is never consulted,
/// so the console is reachable on every node without configuring it.
pub const DATABASE: &str = "pgsteward";

/// What this node answers today. The console's language is wider than this,
/// and every refusal points back at the part that is served.
const SERVED: &str =
    "This node serves SHOW POOLS, SHOW BUDGET, SHOW INSTANCES, SET TENANT and SET INSTANCE.";

/// What the console tells about itself before it reads a command.
///
/// A console session runs no query on any instance, so none of these values
/// comes from a server: they describe how the console itself writes its
/// answers.
const GREETING: [(&str, &str); 6] = [
    (
        "server_version",
        concat!(env!("CARGO_PKG_VERSION"), " (PgSteward)"),
    ),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("DateStyle", "ISO"),
    ("TimeZone", "UTC"),
    ("standard_conforming_strings", "on"),
];

const READ_CHUNK: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum ConsoleError {
    #[error("i/o error while serving the admin console: {0}")]
    Io(#[from] io::Error),
    #[error("malformed message: {0}")]
    Frame(#[from] FrameError),
    #[error(transparent)]
    Message(#[from] MessageError),
    #[error(transparent)]
    Frontend(#[from] FrontendError),
}

/// The node the console runs in, as far as the console sees it: one reading of
/// everything the tables report, and the cluster settings the commands write.
pub trait ConsoleNode: Send + Sync {
    /// One reading of the node. Nothing in it is read again while a table is
    /// written, so a table never mixes two states of the node.
    fn view(&self) -> ConsoleView;

    /// Writes one tenant rule of the cluster configuration.
    fn set_tenant(&self, tenant: &str, change: PolicyChange) -> Result<(), SettingError>;

    /// Writes the margin one instance's total budget is derived with.
    ///
    /// It answers once the margin is in force, which for a margin that shrinks
    /// the budget is after the connections above it are closed, so the console
    /// never reports a budget the instance is not yet within.
    fn set_instance(
        &self,
        instance: &str,
        margin: u32,
    ) -> impl Future<Output = Result<(), SettingError>> + Send;
}

/// Whether this client asked for the admin console rather than for an
/// instance. The database alone decides it, so an operator reaches the console
/// with whatever login the node already authenticates.
#[must_use]
pub fn is_console(tenant: &TenantId) -> bool {
    tenant.database() == DATABASE
}

/// Answers admin commands on an authenticated session until the client leaves.
///
/// The session holds no server connection and takes no slot: it reads `view`
/// once per command and writes the table from that one reading.
pub async fn serve_console<S, N>(session: ClientSession<S>, node: N) -> Result<(), ConsoleError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    N: ConsoleNode,
{
    let (mut stream, mut pending) = session.into_parts();
    let mut greeting = BytesMut::new();
    for (name, value) in GREETING {
        encode_parameter_status(name, value, &mut greeting);
    }
    encode_ready_for_query(TransactionStatus::Idle, &mut greeting);
    write(&mut stream, &greeting).await?;

    let mut discarding = false;
    loop {
        let mut out = BytesMut::new();
        let mut leaving = false;
        while let Some(frame) = decode_frame(&mut pending, MAX_MESSAGE)? {
            match FrontendTag::try_from(frame.tag)? {
                FrontendTag::Terminate => {
                    leaving = true;
                    break;
                }
                FrontendTag::Sync => {
                    discarding = false;
                    encode_ready_for_query(TransactionStatus::Idle, &mut out);
                }
                _ if discarding => {}
                FrontendTag::Query => {
                    answer(decode_query(&frame.body)?, &node, &mut out).await;
                    encode_ready_for_query(TransactionStatus::Idle, &mut out);
                }
                _ => {
                    encode_error_response(&simple_query_only(), &mut out);
                    discarding = true;
                }
            }
        }
        write(&mut stream, &out).await?;
        if leaving {
            return Ok(());
        }
        pending.reserve(READ_CHUNK);
        if stream.read_buf(&mut pending).await? == 0 {
            return Ok(());
        }
    }
}

/// Writes the table `sql` asks for, or says why it cannot be answered.
async fn answer<N: ConsoleNode>(sql: &str, node: &N, out: &mut BytesMut) {
    let command = match parse(sql) {
        Ok(command) => command,
        Err(error) => {
            encode_error_response(&error.response(), out);
            return;
        }
    };
    let unserved = match command {
        AdminCommand::Empty => {
            encode_empty_query_response(out);
            return;
        }
        AdminCommand::ShowPools => {
            show_pools(&node.view()).encode(out);
            return;
        }
        AdminCommand::ShowBudget => {
            show_budget(&node.view()).encode(out);
            return;
        }
        AdminCommand::ShowInstances => {
            show_instances(&node.view()).encode(out);
            return;
        }
        AdminCommand::SetTenant { tenant, settings } => {
            match node.set_tenant(&tenant, change_of(settings)) {
                Ok(()) => encode_command_complete(SET_TAG, out),
                Err(error) => encode_error_response(&refusal(&error), out),
            }
            return;
        }
        AdminCommand::SetInstance { instance, margin } => {
            match node.set_instance(&instance, margin).await {
                Ok(()) => encode_command_complete(SET_TAG, out),
                Err(error) => encode_error_response(&refusal(&error), out),
            }
            return;
        }
        AdminCommand::ShowClients => "SHOW CLIENTS",
        AdminCommand::ShowServers => "SHOW SERVERS",
        AdminCommand::ShowConfig => "SHOW CONFIG",
        AdminCommand::Reload => "RELOAD",
        AdminCommand::Pause => "PAUSE",
        AdminCommand::Resume => "RESUME",
    };
    encode_error_response(
        &ErrorResponse::error(
            sqlstate::FEATURE_NOT_SUPPORTED,
            format!(
                "`{unserved}` is part of the admin console but this node does not serve it yet"
            ),
        )
        .with_hint(SERVED),
        out,
    );
}

fn change_of(settings: TenantSettings) -> PolicyChange {
    PolicyChange {
        min: settings.min,
        max: settings.max,
        weight: settings.weight,
    }
}

/// What the console sends back when the node will not take a setting.
///
/// A name no rule is written as is an object that does not exist; a value the
/// allocation could not stand behind is a parameter out of range. Each refusal
/// points at where the operator can read the numbers it was weighed against.
fn refusal(error: &SettingError) -> ErrorResponse {
    let (code, hint) = match error {
        SettingError::NoSuchTenant { .. } => (
            sqlstate::UNDEFINED_OBJECT,
            "The name is a tenant rule as the cluster configuration writes it, \
             for example `app_web@reports`, `app_web` or `*`.",
        ),
        SettingError::NoSuchInstance { .. } => (
            sqlstate::UNDEFINED_OBJECT,
            "SHOW INSTANCES names the instances the cluster configuration holds.",
        ),
        SettingError::MinAboveMax { .. } => (
            sqlstate::INVALID_PARAMETER_VALUE,
            "Write min and max in one command to move both at once.",
        ),
        SettingError::ZeroWeight => (
            sqlstate::INVALID_PARAMETER_VALUE,
            "A weight is at least 1, which is the value a rule holds unless it says otherwise.",
        ),
        SettingError::AboveBudget { .. } => (
            sqlstate::INVALID_PARAMETER_VALUE,
            "SHOW INSTANCES explains how each total budget was derived.",
        ),
    };
    ErrorResponse::error(code, error.to_string()).with_hint(hint)
}

/// The console reads whole commands, so it has nowhere to put a parse that is
/// bound and executed later.
fn simple_query_only() -> ErrorResponse {
    ErrorResponse::error(
        sqlstate::FEATURE_NOT_SUPPORTED,
        "the admin console reads the simple query protocol only",
    )
    .with_hint("Send the command as a simple query.")
}

async fn write<S: AsyncWrite + Unpin>(stream: &mut S, out: &[u8]) -> io::Result<()> {
    if out.is_empty() {
        return Ok(());
    }
    stream.write_all(out).await?;
    stream.flush().await
}

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
