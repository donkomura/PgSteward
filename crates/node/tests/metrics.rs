use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pgsteward_core::allocation::{AllocationTable, Desired, Entry, Holder, InstanceId, ProxyId};
use pgsteward_core::budget::{BudgetInputs, ServerLimits, TotalBudget};
use pgsteward_core::console::{ConsoleNode, ConsoleView, InstanceSnapshot, PoolSnapshot};
use pgsteward_core::grant::TenantPolicy;
use pgsteward_core::policy::{PolicyChange, SettingError};
use pgsteward_core::pool::PoolStats;
use pgsteward_core::rt::Net;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_core::tenant::TenantId;
use pgsteward_node::metrics::{exposition, serve_scrapes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const PROXY: &str = "10.0.0.1:6432";
const OTHER_PROXY: &str = "10.0.0.2:6432";
const INSTANCE: &str = "db-a";

fn instance() -> InstanceId {
    InstanceId::new(INSTANCE)
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

fn stats(idle: usize, in_use: usize, closing: usize, opening: usize, waiting: usize) -> PoolStats {
    PoolStats {
        slots: idle + in_use + opening,
        idle,
        in_use,
        opening,
        closing,
        waiting,
        opened: 0,
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

fn table(entry: &Entry) -> AllocationTable {
    let mut table = AllocationTable::new();
    table.apply(entry).expect("an entry within the budget");
    table
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

/// One node holding three connections for `app_web` out of the six slots it
/// was granted, on an instance whose budget leaves room for more.
fn one_pool() -> ConsoleView {
    let table = table(
        &Entry::new().instance(
            instance(),
            Desired::new(70)
                .grant(Holder::new(tenant("app_web"), ProxyId::new(PROXY)), 6)
                .grant(Holder::new(tenant("batch"), ProxyId::new(OTHER_PROXY)), 4),
        ),
    );
    view(
        table,
        vec![PoolSnapshot {
            instance: instance(),
            tenant: tenant("app_web"),
            policy: Some(policy(2, 8)),
            stats: PoolStats {
                opened: 17,
                ..stats(2, 3, 1, 1, 4)
            },
        }],
        vec![InstanceSnapshot {
            instance: instance(),
            budget: budget(100, 10, 15),
        }],
    )
}

/// Every sample of the exposition, keyed by its name and its labels in
/// alphabetical order so that a test does not pin the order they are written
/// in.
fn samples(text: &str) -> BTreeMap<String, u64> {
    text.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .map(|line| {
            let (series, value) = line.rsplit_once(' ').expect("a sample ends with its value");
            (normalize(series), value.parse().expect("a whole number"))
        })
        .collect()
}

fn normalize(series: &str) -> String {
    let Some((name, labels)) = series.split_once('{') else {
        return series.to_owned();
    };
    let mut labels: Vec<&str> = labels
        .trim_end_matches('}')
        .split(',')
        .filter(|label| !label.is_empty())
        .collect();
    labels.sort_unstable();
    format!("{name}{{{}}}", labels.join(","))
}

fn value(text: &str, series: &str) -> u64 {
    *samples(text)
        .get(&normalize(series))
        .unwrap_or_else(|| panic!("no sample named {series} in:\n{text}"))
}

fn named(text: &str, name: &str) -> BTreeMap<String, u64> {
    samples(text)
        .into_iter()
        .filter(|(series, _)| series.starts_with(&format!("{name}{{")) || series == name)
        .collect()
}

#[test]
fn granted_slots_and_server_connections_stand_side_by_side() {
    let text = exposition(&one_pool());

    assert_eq!(
        value(
            &text,
            r#"pgsteward_granted_slots{instance="db-a",database="app",user="app_web",proxy="10.0.0.1:6432"}"#
        ),
        6
    );
    assert_eq!(
        value(
            &text,
            r#"pgsteward_server_connections{instance="db-a",database="app",user="app_web",state="idle"}"#
        ),
        2
    );
    assert_eq!(
        value(
            &text,
            r#"pgsteward_server_connections{instance="db-a",database="app",user="app_web",state="active"}"#
        ),
        3
    );
    assert_eq!(
        value(
            &text,
            r#"pgsteward_server_connections{instance="db-a",database="app",user="app_web",state="closing"}"#
        ),
        1
    );
}

#[test]
fn the_connection_states_add_up_to_what_the_budget_counts() {
    let view = one_pool();
    let actual = view.pools[0].stats.actual() as u64;
    let text = exposition(&view);

    let counted: u64 = named(&text, "pgsteward_server_connections").values().sum();

    assert_eq!(counted, actual);
    assert_eq!(
        value(
            &text,
            r#"pgsteward_server_connections_opening{instance="db-a",database="app",user="app_web"}"#
        ),
        1
    );
}

#[test]
fn every_server_connection_the_node_opened_is_counted() {
    let text = exposition(&one_pool());

    assert_eq!(
        value(
            &text,
            r#"pgsteward_server_connections_opened_total{instance="db-a",database="app",user="app_web"}"#
        ),
        17
    );
    assert!(
        text.contains("# TYPE pgsteward_server_connections_opened counter"),
        "{text}"
    );
}

#[test]
fn a_grant_held_by_another_proxy_reports_no_connections() {
    let text = exposition(&one_pool());

    assert_eq!(
        value(
            &text,
            r#"pgsteward_granted_slots{instance="db-a",database="app",user="batch",proxy="10.0.0.2:6432"}"#
        ),
        4
    );
    assert!(
        named(&text, "pgsteward_server_connections")
            .keys()
            .all(|series| !series.contains(r#"user="batch""#)),
        "the connections of another proxy are unknown here, not zero:\n{text}"
    );
}

#[test]
fn the_waiting_clients_and_the_demand_they_make_are_reported() {
    let text = exposition(&one_pool());

    assert_eq!(
        value(
            &text,
            r#"pgsteward_clients_waiting{instance="db-a",database="app",user="app_web"}"#
        ),
        4
    );
    assert_eq!(
        value(
            &text,
            r#"pgsteward_demand_slots{instance="db-a",database="app",user="app_web"}"#
        ),
        8
    );
}

#[test]
fn a_tenant_no_rule_covers_reports_no_share() {
    let text = exposition(&view(
        table(&Entry::new().instance(instance(), Desired::new(70))),
        vec![PoolSnapshot {
            instance: instance(),
            tenant: tenant("app_web"),
            policy: None,
            stats: stats(1, 0, 0, 0, 0),
        }],
        Vec::new(),
    ));

    assert!(named(&text, "pgsteward_tenant_min_slots").is_empty());
    assert!(named(&text, "pgsteward_tenant_max_slots").is_empty());
    assert_eq!(
        value(
            &text,
            r#"pgsteward_server_connections{instance="db-a",database="app",user="app_web",state="idle"}"#
        ),
        1
    );
}

#[test]
fn the_share_a_rule_writes_is_reported() {
    let text = exposition(&one_pool());

    assert_eq!(
        value(
            &text,
            r#"pgsteward_tenant_min_slots{instance="db-a",database="app",user="app_web"}"#
        ),
        2
    );
    assert_eq!(
        value(
            &text,
            r#"pgsteward_tenant_max_slots{instance="db-a",database="app",user="app_web"}"#
        ),
        8
    );
}

#[test]
fn the_total_budget_comes_with_every_part_it_was_derived_from() {
    let text = exposition(&one_pool());

    assert_eq!(
        value(&text, r#"pgsteward_max_connections{instance="db-a"}"#),
        100
    );
    assert_eq!(
        value(&text, r#"pgsteward_reserved_connections{instance="db-a"}"#),
        5
    );
    assert_eq!(
        value(
            &text,
            r#"pgsteward_foreign_connections_peak{instance="db-a"}"#
        ),
        10
    );
    assert_eq!(
        value(&text, r#"pgsteward_budget_margin{instance="db-a"}"#),
        15
    );
    assert_eq!(
        value(&text, r#"pgsteward_budget_shortfall{instance="db-a"}"#),
        0
    );
    assert_eq!(
        value(&text, r#"pgsteward_total_budget_slots{instance="db-a"}"#),
        70
    );
}

#[test]
fn a_budget_the_deductions_outgrew_reports_its_shortfall() {
    let text = exposition(&view(
        table(&Entry::new().instance(instance(), Desired::new(0))),
        Vec::new(),
        vec![InstanceSnapshot {
            instance: instance(),
            budget: budget(20, 30, 15),
        }],
    ));

    assert_eq!(
        value(&text, r#"pgsteward_total_budget_slots{instance="db-a"}"#),
        0
    );
    assert_eq!(
        value(&text, r#"pgsteward_budget_shortfall{instance="db-a"}"#),
        30
    );
}

#[test]
fn the_granted_slots_and_the_unallocated_ones_make_up_the_budget() {
    let text = exposition(&one_pool());

    let granted: u64 = named(&text, "pgsteward_granted_slots").values().sum();
    let unallocated = value(&text, r#"pgsteward_unallocated_slots{instance="db-a"}"#);

    assert_eq!(unallocated, 60);
    assert_eq!(
        granted + unallocated,
        value(&text, r#"pgsteward_total_budget_slots{instance="db-a"}"#)
    );
}

#[test]
fn a_node_holding_nothing_writes_a_complete_exposition() {
    let text = exposition(&view(AllocationTable::new(), Vec::new(), Vec::new()));

    assert!(samples(&text).is_empty(), "{text}");
    assert!(text.ends_with("# EOF\n"), "{text}");
}

#[test]
fn every_name_says_which_system_it_belongs_to() {
    let text = exposition(&one_pool());

    assert!(
        samples(&text)
            .keys()
            .all(|series| series.starts_with("pgsteward_")),
        "{text}"
    );
}

/// A node that answers scrapes and counts how often it was read, so that a
/// test can tell one reading from two.
#[derive(Debug, Clone)]
struct ScrapedNode {
    readings: Arc<AtomicUsize>,
}

impl ScrapedNode {
    fn new() -> Self {
        Self {
            readings: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn readings(&self) -> usize {
        self.readings.load(Ordering::SeqCst)
    }
}

impl ConsoleNode for ScrapedNode {
    fn view(&self) -> ConsoleView {
        self.readings.fetch_add(1, Ordering::SeqCst);
        one_pool()
    }

    fn set_tenant(&self, _tenant: &str, _change: PolicyChange) -> Result<(), SettingError> {
        unreachable!("a scrape writes nothing")
    }

    fn set_instance(
        &self,
        _instance: &str,
        _margin: u32,
    ) -> impl Future<Output = Result<(), SettingError>> + Send {
        let refusal = Err(SettingError::NoSuchInstance {
            instance: InstanceId::new("a scrape writes nothing"),
        });
        async move { refusal }
    }
}

async fn scraping(node: ScrapedNode) -> SocketAddr {
    let rt = TokioRuntime::new();
    let listener = rt.bind("127.0.0.1:0").await.expect("a free port");
    let addr = listener.local_addr().expect("the port it was given");
    tokio::spawn(serve_scrapes(rt, listener, node));
    addr
}

/// Sends one request and reads everything the node answers with, up to the
/// close.
async fn request(addr: SocketAddr, target: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("the scrape address");
    stream
        .write_all(format!("GET {target} HTTP/1.1\r\nHost: node\r\n\r\n").as_bytes())
        .await
        .expect("a request the node reads");
    let mut answer = String::new();
    stream
        .read_to_string(&mut answer)
        .await
        .expect("an answer that ends with the close");
    answer
}

#[tokio::test]
async fn a_scrape_reads_the_metrics_of_the_node() {
    let addr = scraping(ScrapedNode::new()).await;

    let answer = request(addr, "/metrics").await;

    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
    assert!(
        answer.contains("Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8"),
        "{answer}"
    );
    let body = answer.split_once("\r\n\r\n").expect("a body").1;
    assert_eq!(body, exposition(&one_pool()));
}

#[tokio::test]
async fn every_scrape_reads_the_node_again() {
    let node = ScrapedNode::new();
    let addr = scraping(node.clone()).await;

    request(addr, "/metrics").await;
    request(addr, "/metrics").await;

    assert_eq!(node.readings(), 2);
}

#[tokio::test]
async fn a_path_that_is_not_the_scrape_reads_nothing() {
    let node = ScrapedNode::new();
    let addr = scraping(node.clone()).await;

    let answer = request(addr, "/healthz").await;

    assert!(answer.starts_with("HTTP/1.1 404 Not Found\r\n"), "{answer}");
    assert_eq!(node.readings(), 0);
}

#[tokio::test]
async fn a_request_that_is_not_a_read_is_refused() {
    let node = ScrapedNode::new();
    let addr = scraping(node.clone()).await;

    let mut stream = TcpStream::connect(addr).await.expect("the scrape address");
    stream
        .write_all(b"POST /metrics HTTP/1.1\r\nHost: node\r\n\r\n")
        .await
        .expect("a request the node reads");
    let mut answer = String::new();
    stream
        .read_to_string(&mut answer)
        .await
        .expect("an answer that ends with the close");

    assert!(
        answer.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
        "{answer}"
    );
    assert_eq!(node.readings(), 0);
}

#[tokio::test]
async fn one_scrape_does_not_hold_the_next_one_up() {
    let addr = scraping(ScrapedNode::new()).await;

    let held = TcpStream::connect(addr).await.expect("the scrape address");

    let answer = request(addr, "/metrics").await;
    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
    drop(held);
}
