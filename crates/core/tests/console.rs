use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};

use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use pgsteward_core::allocation::{AllocationTable, Desired, Entry, Holder, InstanceId, ProxyId};
use pgsteward_core::auth::TrustAll;
use pgsteward_core::budget::{BudgetInputs, ServerLimits, TotalBudget};
use pgsteward_core::console::{
    ConsoleError, ConsoleNode, ConsoleView, DATABASE, InstanceSnapshot, PoolSnapshot, ResultSet,
    is_console, serve_console, show_budget, show_instances, show_pools,
};
use pgsteward_core::grant::TenantPolicy;
use pgsteward_core::policy::{PolicyChange, SettingError};
use pgsteward_core::pool::PoolStats;
use pgsteward_core::session::{Accepted, accept};
use pgsteward_core::tenant::TenantId;
use pgsteward_protocol::backend::Column;
use pgsteward_protocol::framing::{decode_frame, encode_frame};
use pgsteward_protocol::startup::{
    ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::message::backend::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::task::JoinHandle;

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

#[test]
fn the_console_is_named_by_the_database_alone() {
    assert!(is_console(&TenantId::new("alice", DATABASE)));
    assert!(is_console(&TenantId::new("postgres", DATABASE)));
    assert!(!is_console(&TenantId::new(DATABASE, "app")));
    assert!(!is_console(&TenantId::new("alice", "app")));
}

#[tokio::test]
async fn the_console_greets_a_client_before_it_reads_a_command() {
    let (mut client, _console) = console(Views::of(vec![empty_view()])).await;

    let greeting = client.read_greeting().await;

    let names: Vec<&str> = greeting.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "server_version",
            "server_encoding",
            "client_encoding",
            "DateStyle",
            "TimeZone",
            "standard_conforming_strings",
        ]
    );
    let version = &greeting[0].1;
    assert!(
        version.contains("PgSteward"),
        "the console names itself in server_version, got {version:?}"
    );
}

#[tokio::test]
async fn the_console_writes_the_table_the_command_names() {
    let (mut client, _console) = console(Views::of(vec![one_pool_view()])).await;
    client.read_greeting().await;

    client.query("SHOW POOLS").await;

    let Message::RowDescription(description) = client.read_message().await else {
        panic!("the answer opens with a row description");
    };
    assert_eq!(
        description.fields().count().expect("well-formed fields"),
        13
    );
    assert!(matches!(client.read_message().await, Message::DataRow(_)));
    let Message::CommandComplete(complete) = client.read_message().await else {
        panic!("the rows are followed by a command tag");
    };
    assert_eq!(complete.tag().expect("a UTF-8 tag"), "SHOW");
    client.read_ready().await;
}

#[tokio::test]
async fn every_command_reads_the_node_again() {
    let (mut client, _console) = console(Views::of(vec![empty_view(), one_pool_view()])).await;
    client.read_greeting().await;

    client.query("SHOW POOLS").await;
    assert_eq!(client.rows().await, 0);
    client.query("SHOW POOLS").await;

    assert_eq!(client.rows().await, 1);
}

#[tokio::test]
async fn the_three_tables_the_node_serves_are_all_answered() {
    let (mut client, _console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    for command in ["SHOW POOLS", "SHOW BUDGET", "SHOW INSTANCES"] {
        client.query(command).await;
        assert_eq!(client.rows().await, 0, "{command} answered with no table");
    }
}

#[tokio::test]
async fn a_text_that_holds_no_statement_gets_an_empty_query_response() {
    let (mut client, _console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    client.query(" ; ").await;

    assert!(matches!(
        client.read_message().await,
        Message::EmptyQueryResponse
    ));
    client.read_ready().await;
}

#[tokio::test]
async fn a_text_outside_the_console_language_is_refused_and_the_session_goes_on() {
    let (mut client, _console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    client.query("SELECT 1").await;

    let error = client.read_error().await;
    assert_eq!(error.code, "42601");
    assert!(
        error.hint.unwrap_or_default().contains("SHOW POOLS"),
        "the refusal names the commands the console has"
    );
    client.read_ready().await;
    client.query("SHOW POOLS").await;
    assert_eq!(client.rows().await, 0);
}

#[tokio::test]
async fn a_command_of_the_language_this_node_does_not_serve_yet_says_which_it_serves() {
    let (mut client, _console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    client.query("RELOAD").await;

    let error = client.read_error().await;
    assert_eq!(error.code, "0A000");
    assert!(
        error.hint.unwrap_or_default().contains("SHOW POOLS"),
        "the refusal names the commands this node serves"
    );
    client.read_ready().await;
    client.query("SHOW BUDGET").await;
    assert_eq!(client.rows().await, 0);
}

#[tokio::test]
async fn a_tenant_setting_reaches_the_node_with_the_name_as_it_was_written() {
    let (mut client, _console, written) = node(Views::repeating(empty_view()), None).await;
    client.read_greeting().await;

    client
        .query("SET TENANT \"app web@reports\" min = 2, weight = 3")
        .await;

    let Message::CommandComplete(complete) = client.read_message().await else {
        panic!("a setting is answered with a command tag");
    };
    assert_eq!(complete.tag().expect("a UTF-8 tag"), "SET");
    client.read_ready().await;
    assert_eq!(
        *written.lock().expect("the written settings lock"),
        vec![(
            "app web@reports".to_owned(),
            PolicyChange {
                min: Some(2),
                max: None,
                weight: Some(3),
            }
        )]
    );
}

#[tokio::test]
async fn a_tenant_setting_the_node_refuses_says_why_and_the_session_goes_on() {
    let (mut client, _console) = console_refusing(SettingError::AboveBudget {
        instance: instance("primary"),
        minimums: 45,
        budget: 40,
    })
    .await;
    client.read_greeting().await;

    client.query("SET TENANT app_web min = 45").await;

    let error = client.read_error().await;
    assert_eq!(error.code, "22023");
    assert!(
        error.message.contains("primary") && error.message.contains("40"),
        "the refusal names the instance and its total budget, got {:?}",
        error.message
    );
    assert!(
        error.hint.unwrap_or_default().contains("SHOW INSTANCES"),
        "the refusal points at the table that explains the budget"
    );
    client.read_ready().await;
    client.query("SHOW POOLS").await;
    assert_eq!(client.rows().await, 0);
}

#[tokio::test]
async fn a_name_no_rule_is_written_as_is_refused_as_an_undefined_object() {
    let (mut client, _console) = console_refusing(SettingError::NoSuchTenant {
        tenant: "app_web@orders".to_owned(),
    })
    .await;
    client.read_greeting().await;

    client.query("SET TENANT app_web@orders max = 5").await;

    let error = client.read_error().await;
    assert_eq!(error.code, "42704");
    assert!(error.message.contains("app_web@orders"));
    client.read_ready().await;
}

#[tokio::test]
async fn set_instance_is_of_the_language_this_node_does_not_serve_yet() {
    let (mut client, _console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    client.query("SET INSTANCE primary margin = 20").await;

    let error = client.read_error().await;
    assert_eq!(error.code, "0A000");
    client.read_ready().await;
}

#[tokio::test]
async fn the_console_reads_the_simple_query_protocol_only() {
    let (mut client, _console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    client.send(&parse_frame("SHOW POOLS")).await;
    let error = client.read_error().await;
    assert_eq!(error.code, "0A000");

    client.send(&frame(b'D', b"P\0")).await;
    client.send(&frame(b'E', b"\0\0\0\0\0")).await;
    client.send(&frame(b'S', &[])).await;

    client.read_ready().await;
    client.query("SHOW POOLS").await;
    assert_eq!(client.rows().await, 0);
}

#[tokio::test]
async fn terminate_ends_the_console_session() {
    let (mut client, console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    client.send(&frame(b'X', &[])).await;

    console
        .await
        .expect("the console task")
        .expect("a clean end");
    assert!(client.read_to_end().await.is_empty());
}

#[tokio::test]
async fn a_client_that_goes_away_ends_the_console_session() {
    let (mut client, console) = console(Views::repeating(empty_view())).await;
    client.read_greeting().await;

    drop(client);

    console
        .await
        .expect("the console task")
        .expect("a clean end");
}

const MAX_FRAME: usize = 1 << 20;
const DUPLEX_CAPACITY: usize = 64 * 1024;

fn empty_view() -> ConsoleView {
    view(AllocationTable::new(), Vec::new(), Vec::new())
}

fn one_pool_view() -> ConsoleView {
    view(
        AllocationTable::new(),
        vec![PoolSnapshot {
            instance: instance("primary"),
            tenant: tenant("alice"),
            policy: None,
            stats: PoolStats::default(),
        }],
        Vec::new(),
    )
}

/// The views the console is given, one per command. The last one stands for
/// every command after it, so a test names only the moments it cares about.
struct Views(Mutex<Vec<ConsoleView>>);

impl Views {
    fn of(views: Vec<ConsoleView>) -> Self {
        Self(Mutex::new(views))
    }

    fn repeating(view: ConsoleView) -> Self {
        Self::of(vec![view])
    }

    fn take(&self) -> ConsoleView {
        let mut views = self.0.lock().expect("the views lock");
        if views.len() > 1 {
            views.remove(0)
        } else {
            views[0].clone()
        }
    }
}

/// What a console session is given: the views it reads, and where the
/// settings it writes are kept.
struct Node {
    views: Views,
    written: Arc<Mutex<Vec<(String, PolicyChange)>>>,
    refusal: Option<SettingError>,
}

impl ConsoleNode for Node {
    fn view(&self) -> ConsoleView {
        self.views.take()
    }

    fn set_tenant(&self, tenant: &str, change: PolicyChange) -> Result<(), SettingError> {
        self.written
            .lock()
            .expect("the written settings lock")
            .push((tenant.to_owned(), change));
        match &self.refusal {
            Some(refusal) => Err(refusal.clone()),
            None => Ok(()),
        }
    }
}

async fn console(views: Views) -> (Client, JoinHandle<Result<(), ConsoleError>>) {
    let (client, console, _) = node(views, None).await;
    (client, console)
}

async fn console_refusing(refusal: SettingError) -> (Client, JoinHandle<Result<(), ConsoleError>>) {
    let (client, console, _) = node(Views::repeating(empty_view()), Some(refusal)).await;
    (client, console)
}

type Written = Arc<Mutex<Vec<(String, PolicyChange)>>>;

async fn node(
    views: Views,
    refusal: Option<SettingError>,
) -> (Client, JoinHandle<Result<(), ConsoleError>>, Written) {
    let (client_stream, session_stream) = duplex(DUPLEX_CAPACITY);
    let mut client = Client::new(client_stream);
    let accepting = tokio::spawn(accept(session_stream, TrustAll, None));

    let mut out = BytesMut::new();
    encode_startup(
        &StartupRequest::Startup(StartupMessage::new(
            ProtocolVersion::V3_0,
            vec![
                ("user".to_owned(), "admin".to_owned()),
                ("database".to_owned(), DATABASE.to_owned()),
            ],
        )),
        &mut out,
    );
    client.send(&out).await;

    let Accepted::Session(session) = accepting.await.unwrap().unwrap() else {
        panic!("expected an authenticated session");
    };
    assert!(is_console(session.tenant()));
    assert!(matches!(
        client.read_message().await,
        Message::AuthenticationOk
    ));
    let written: Written = Arc::new(Mutex::new(Vec::new()));
    let node = Node {
        views,
        written: Arc::clone(&written),
        refusal,
    };
    (client, tokio::spawn(serve_console(session, node)), written)
}

struct Client {
    stream: DuplexStream,
    buf: BytesMut,
}

struct Refusal {
    code: String,
    message: String,
    hint: Option<String>,
}

impl Client {
    fn new(stream: DuplexStream) -> Self {
        Self {
            stream,
            buf: BytesMut::new(),
        }
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    async fn query(&mut self, sql: &str) {
        let mut body = BytesMut::new();
        body.put_slice(sql.as_bytes());
        body.put_u8(0);
        let query = frame(b'Q', &body);
        self.send(&query).await;
    }

    async fn read_message(&mut self) -> Message {
        loop {
            if let Some(frame) = decode_frame(&mut self.buf, MAX_FRAME).unwrap() {
                let mut bytes = BytesMut::new();
                encode_frame(frame.tag, &frame.body, &mut bytes);
                return Message::parse(&mut bytes).unwrap().unwrap();
            }
            let read = self.stream.read_buf(&mut self.buf).await.unwrap();
            assert!(read > 0, "the console closed before answering");
        }
    }

    async fn read_greeting(&mut self) -> Vec<(String, String)> {
        let mut parameters = Vec::new();
        loop {
            match self.read_message().await {
                Message::ParameterStatus(body) => parameters.push((
                    body.name().unwrap().to_owned(),
                    body.value().unwrap().to_owned(),
                )),
                Message::ReadyForQuery(body) => {
                    assert_eq!(body.status(), b'I');
                    return parameters;
                }
                _ => panic!("unexpected message before ReadyForQuery"),
            }
        }
    }

    async fn read_ready(&mut self) {
        let Message::ReadyForQuery(body) = self.read_message().await else {
            panic!("expected a ReadyForQuery");
        };
        assert_eq!(body.status(), b'I');
    }

    async fn read_error(&mut self) -> Refusal {
        let Message::ErrorResponse(body) = self.read_message().await else {
            panic!("expected an ErrorResponse");
        };
        let mut code = None;
        let mut message = None;
        let mut hint = None;
        let mut fields = body.fields();
        while let Some(field) = fields.next().expect("well-formed error fields") {
            match field.type_() {
                b'C' => code = Some(text(field.value_bytes())),
                b'M' => message = Some(text(field.value_bytes())),
                b'H' => hint = Some(text(field.value_bytes())),
                _ => {}
            }
        }
        Refusal {
            code: code.expect("a SQLSTATE"),
            message: message.expect("a message"),
            hint,
        }
    }

    /// Reads one whole table and answers how many rows it held.
    async fn rows(&mut self) -> usize {
        let Message::RowDescription(_) = self.read_message().await else {
            panic!("the answer opens with a row description");
        };
        let mut rows = 0;
        loop {
            match self.read_message().await {
                Message::DataRow(_) => rows += 1,
                Message::CommandComplete(_) => {
                    self.read_ready().await;
                    return rows;
                }
                _ => panic!("unexpected message in a table"),
            }
        }
    }

    async fn read_to_end(&mut self) -> Vec<u8> {
        let mut rest = self.buf.to_vec();
        self.stream.read_to_end(&mut rest).await.unwrap();
        rest
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("a UTF-8 field")
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::new();
    encode_frame(tag, body, &mut out);
    out.to_vec()
}

fn parse_frame(sql: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_u8(0);
    body.put_slice(sql.as_bytes());
    body.put_u8(0);
    body.put_i16(0);
    frame(b'P', &body)
}
