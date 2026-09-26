use std::fmt;
use std::io;

use pgsteward_core::budget::TotalBudget;
use pgsteward_core::console::{ConsoleNode, ConsoleView, PoolSnapshot};
use pgsteward_core::rt::{Listener, Runtime};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{DescriptorEncoder, EncodeLabelSet, NoLabelSet};
use prometheus_client::metrics::MetricType;
use prometheus_client::registry::Registry;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// What every name this system publishes begins with, so that a scrape of a
/// host running several exporters says which one a series came from.
const PREFIX: &str = "pgsteward";

/// The states a server connection this node holds can be in. Their counts add
/// up to the number the connection cap is judged against, so a connection
/// being opened is not among them.
const IDLE: &str = "idle";
const ACTIVE: &str = "active";
const CLOSING: &str = "closing";

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct GrantLabels {
    instance: String,
    database: String,
    user: String,
    proxy: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct PoolLabels {
    instance: String,
    database: String,
    user: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct ConnectionLabels {
    instance: String,
    database: String,
    user: String,
    state: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct InstanceLabels {
    instance: String,
}

/// Writes every metric of this node in the `OpenMetrics` text format.
///
/// It is given one reading of the node and reads nothing else, so the desired
/// state and the actual connections in one scrape are of the same moment.
/// Taking them from two readings would show them apart while they agree, or
/// together while they do not, and it is exactly their agreement that the
/// connection cap rests on.
///
/// **What this node cannot know is left out rather than published as a zero.**
/// The connections behind another proxy's grant, and the share of a tenant no
/// rule covers, have no series here: a zero would be the different claim that
/// the number was measured and found to be none.
pub fn exposition(view: &ConsoleView) -> String {
    let mut registry = Registry::with_prefix(PREFIX);
    registry.register_collector(Box::new(NodeMetrics(view.clone())));
    let mut text = String::new();
    encode(&mut text, &registry).expect("writing into a String never fails");
    text
}

/// One reading of the node, written out series by series.
///
/// The series are written in the order the tables of the admin console list
/// their rows, so two scrapes of the same reading are the same text and a
/// diff of two scrapes shows what moved.
#[derive(Debug)]
struct NodeMetrics(ConsoleView);

impl Collector for NodeMetrics {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), fmt::Error> {
        encode_grants(&mut encoder, &self.0)?;
        encode_pools(&mut encoder, &self.0)?;
        encode_instances(&mut encoder, &self.0)
    }
}

/// The desired state, holder by holder, and the slots of each budget that are
/// granted to no one.
fn encode_grants(encoder: &mut DescriptorEncoder, view: &ConsoleView) -> Result<(), fmt::Error> {
    gauges(
        encoder,
        "granted_slots",
        "Connection slots the allocation table grants to a holder on an instance",
        view.table.instances().flat_map(|instance| {
            view.table.holders(instance).map(move |(holder, slots)| {
                (
                    GrantLabels {
                        instance: instance.to_string(),
                        database: holder.tenant().database().to_owned(),
                        user: holder.tenant().user().to_owned(),
                        proxy: holder.proxy().to_string(),
                    },
                    i64::from(slots),
                )
            })
        }),
    )?;
    gauges(
        encoder,
        "unallocated_slots",
        "Connection slots of an instance's total budget that are granted to no one",
        view.table.instances().map(|instance| {
            (
                InstanceLabels {
                    instance: instance.to_string(),
                },
                i64::from(view.table.headroom(instance)),
            )
        }),
    )
}

/// What this node actually holds against those grants, and what is waiting on
/// it.
fn encode_pools(encoder: &mut DescriptorEncoder, view: &ConsoleView) -> Result<(), fmt::Error> {
    let mut pools: Vec<&PoolSnapshot> = view.pools.iter().collect();
    pools.sort_by(|left, right| {
        (&left.instance, &left.tenant).cmp(&(&right.instance, &right.tenant))
    });

    gauges(
        encoder,
        "server_connections",
        "Server connections this node holds, by the state they are in",
        pools.iter().flat_map(|pool| {
            let labels = labels_of(pool);
            [
                (IDLE, pool.stats.idle),
                (ACTIVE, pool.stats.in_use),
                (CLOSING, pool.stats.closing),
            ]
            .map(|(state, count)| {
                (
                    ConnectionLabels {
                        instance: labels.instance.clone(),
                        database: labels.database.clone(),
                        user: labels.user.clone(),
                        state,
                    },
                    counted(count),
                )
            })
        }),
    )?;
    gauges(
        encoder,
        "server_connections_opening",
        "Server connections this node is opening and the instance does not hold yet",
        pools
            .iter()
            .map(|pool| (labels_of(pool), counted(pool.stats.opening))),
    )?;
    counters(
        encoder,
        "server_connections_opened",
        "Server connections this node has opened for a tenant since it started",
        pools
            .iter()
            .map(|pool| (labels_of(pool), pool.stats.opened)),
    )?;
    gauges(
        encoder,
        "clients_waiting",
        "Clients queued for a connection slot",
        pools
            .iter()
            .map(|pool| (labels_of(pool), counted(pool.stats.waiting))),
    )?;
    gauges(
        encoder,
        "demand_slots",
        "Connection slots this node reports it needs for a tenant",
        pools
            .iter()
            .map(|pool| (labels_of(pool), counted(pool.stats.demand()))),
    )?;
    gauges(
        encoder,
        "tenant_min_slots",
        "The slots a tenant is guaranteed by the rule the cluster configuration writes",
        pools.iter().filter_map(|pool| {
            pool.policy
                .map(|policy| (labels_of(pool), i64::from(policy.min)))
        }),
    )?;
    gauges(
        encoder,
        "tenant_max_slots",
        "The slots a tenant may not exceed by the rule the cluster configuration writes",
        pools.iter().filter_map(|pool| {
            pool.policy
                .map(|policy| (labels_of(pool), i64::from(policy.max)))
        }),
    )
}

fn encode_instances(encoder: &mut DescriptorEncoder, view: &ConsoleView) -> Result<(), fmt::Error> {
    let mut instances: Vec<_> = view.instances.iter().collect();
    instances.sort_by(|left, right| left.instance.cmp(&right.instance));
    for (name, help, of) in INSTANCE_METRICS {
        gauges(
            encoder,
            name,
            help,
            instances.iter().map(|snapshot| {
                (
                    InstanceLabels {
                        instance: snapshot.instance.to_string(),
                    },
                    i64::from(of(&snapshot.budget)),
                )
            }),
        )?;
    }
    Ok(())
}

fn labels_of(pool: &PoolSnapshot) -> PoolLabels {
    PoolLabels {
        instance: pool.instance.to_string(),
        database: pool.tenant.database().to_owned(),
        user: pool.tenant.user().to_owned(),
    }
}

/// Each instance's total budget together with the parts it was derived from,
/// in the order the derivation subtracts them, so that why a budget is what it
/// is can be read off a dashboard the same way `SHOW INSTANCES` reads.
type InstanceMetric = (&'static str, &'static str, fn(&TotalBudget) -> u32);

const INSTANCE_METRICS: [InstanceMetric; 6] = [
    (
        "max_connections",
        "The instance's max_connections setting",
        |budget| budget.inputs().limits.max_connections,
    ),
    (
        "reserved_connections",
        "Connections the instance reserves for superusers and for reserved roles",
        |budget| budget.inputs().limits.reserved(),
    ),
    (
        "foreign_connections_peak",
        "The highest number of connections not opened by this system seen over the window",
        |budget| budget.inputs().foreign_peak,
    ),
    (
        "budget_margin",
        "The margin held back from the instance's total budget",
        |budget| budget.inputs().margin,
    ),
    (
        "budget_shortfall",
        "By how much the deductions exceed max_connections, leaving the budget at zero",
        TotalBudget::shortfall,
    ),
    (
        "total_budget_slots",
        "Connections this system may hold on the instance, as derived from the instance",
        TotalBudget::total,
    ),
];

/// Writes one metric and every series it has, in the order they are given.
fn gauges<S: EncodeLabelSet>(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    series: impl IntoIterator<Item = (S, i64)>,
) -> Result<(), fmt::Error> {
    let mut metric = encoder.encode_descriptor(name, help, None, MetricType::Gauge)?;
    for (labels, value) in series {
        metric.encode_family(&labels)?.encode_gauge(&value)?;
    }
    Ok(())
}

fn counters<S: EncodeLabelSet>(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    series: impl IntoIterator<Item = (S, u64)>,
) -> Result<(), fmt::Error> {
    let mut metric = encoder.encode_descriptor(name, help, None, MetricType::Counter)?;
    for (labels, value) in series {
        metric
            .encode_family(&labels)?
            .encode_counter::<NoLabelSet, _, u64>(&value, None)?;
    }
    Ok(())
}

/// Counts travel as gauges, and nothing this node counts comes near the range
/// of the value a gauge carries.
fn counted(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// The one path a scrape reads. A node publishes nothing else over this
/// listener, so a request for anything else is answered without reading the
/// node at all.
const PATH: &str = "/metrics";

/// What the body is written in. The version is part of the type, so a scraper
/// that only speaks the older Prometheus text format can tell.
const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// How much of a request head this reads before giving up on it. A scrape
/// sends a request line and a handful of headers; anything longer is not one.
const HEAD_LIMIT: usize = 8 * 1024;

/// Answers metrics scrapes on `listener` until the task is dropped.
///
/// The node is read when a request arrives rather than when the connection
/// was accepted, and each answer closes its connection, so a scrape carries
/// the state the node is in at the moment it asks.
pub async fn serve_scrapes<R, N>(rt: R, listener: R::Listener, node: N)
where
    R: Runtime,
    N: ConsoleNode + Clone + 'static,
{
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let node = node.clone();
                rt.spawn(async move {
                    if let Err(error) = answer(stream, &node).await {
                        tracing::debug!(%error, "a metrics scrape ended with an error");
                    }
                });
            }
            Err(error) => tracing::warn!(%error, "cannot accept a metrics scrape"),
        }
    }
}

async fn answer<S, N>(mut stream: S, node: &N) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    N: ConsoleNode,
{
    let Some(head) = read_head(&mut stream).await? else {
        return Ok(());
    };
    let response = match request_line(&head) {
        Some(("GET", target)) if path_of(target) == PATH => {
            body(200, "OK", CONTENT_TYPE, &exposition(&node.view()))
        }
        Some(("GET", _)) => refusal(404, "Not Found", "This node publishes /metrics.\n"),
        Some(_) => refusal(405, "Method Not Allowed", "A scrape reads /metrics.\n"),
        None => refusal(400, "Bad Request", "Not an HTTP request.\n"),
    };
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    stream.shutdown().await
}

/// Reads up to the blank line that ends the request head, or `None` when the
/// scraper left without sending one.
async fn read_head<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<Option<String>> {
    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while head.len() < HEAD_LIMIT {
        if stream.read(&mut byte).await? == 0 {
            return Ok(None);
        }
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return Ok(Some(String::from_utf8_lossy(&head).into_owned()));
        }
    }
    Ok(Some(String::from_utf8_lossy(&head).into_owned()))
}

fn request_line(head: &str) -> Option<(&str, &str)> {
    let line = head.lines().next()?;
    let mut words = line.split_whitespace();
    let method = words.next()?;
    let target = words.next()?;
    Some((method, target))
}

/// The path of a request target, with any query string left off: a scraper
/// that appends parameters is still asking for the same one path.
fn path_of(target: &str) -> &str {
    target.split(['?', '#']).next().unwrap_or(target)
}

/// Every answer closes the connection it was written on. A scrape is one
/// request, and keeping the socket would hold a reading of the node open
/// against the next one.
fn body(code: u16, reason: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {code} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {length}\r\n\
         Connection: close\r\n\
         \r\n\
         {body}",
        length = body.len()
    )
}

fn refusal(code: u16, reason: &str, message: &str) -> String {
    body(code, reason, "text/plain; charset=utf-8", message)
}
