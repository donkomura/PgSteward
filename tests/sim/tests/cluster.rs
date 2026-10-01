use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::budget::ServerLimits;
use pgsteward_core::rt::turmoil_rt::TurmoilRuntime;
use pgsteward_harness::cluster::{ClusterSpec, DB, SimCluster, connect_to_node};
use pgsteward_harness::wire_client::WireClient;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::ServeOptions;

const PENCIL_VERIFIER: &str = "SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$\
WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=";
const TRANSACTIONS: usize = 3;
const SEEDS: u64 = 5;
const LIMITS: ServerLimits = ServerLimits {
    max_connections: 40,
    superuser_reserved_connections: 3,
    reserved_connections: 0,
};

fn spec(nodes: usize) -> ClusterSpec {
    ClusterSpec {
        nodes,
        limits: LIMITS,
        node: NodeConfig::parse(&format!(
            r#"
[node]
role = "proxy"
listen = "0.0.0.0:6432"
coordinator = "coordinator:7432"
max_client_connections = 100

[monitor]
user = "postgres"

[client."alice"]
verifier = "{PENCIL_VERIFIER}"
"#
        ))
        .unwrap(),
        cluster: ClusterConfig::parse(
            r#"
[cluster]
pool_mode = "transaction"
grant_ttl = "10s"
arbitration_interval = "10ms"

[[instance]]
name = "db"

[tenant."*"]
instances = ["db"]
"#,
        )
        .unwrap(),
        options: ServeOptions {
            margin: 0,
            observe_interval: Duration::from_millis(100),
            ..ServeOptions::default()
        },
    }
}

async fn run_transactions(rt: TurmoilRuntime, node: usize) -> turmoil::Result {
    let stream = connect_to_node(&rt, node).await?;
    let mut client = WireClient::login(stream, "alice", "postgres", Some("pencil")).await?;
    for _ in 0..TRANSACTIONS {
        assert_eq!(client.query("BEGIN").await?.1, b'T');
        assert_eq!(client.query("SELECT 1").await?.1, b'T');
        assert_eq!(client.query("COMMIT").await?.1, b'I');
    }
    Ok(())
}

fn record_when_done(
    cluster: &mut SimCluster<'_>,
    name: &str,
    node: usize,
    done: &Arc<Mutex<Vec<(String, Duration)>>>,
) {
    let done = Arc::clone(done);
    let label = name.to_owned();
    cluster.client(name, async move {
        run_transactions(TurmoilRuntime::new(), node).await?;
        done.lock()
            .unwrap()
            .push((label, turmoil::sim_elapsed().unwrap()));
        Ok(())
    });
}

fn record_outcome(
    cluster: &mut SimCluster<'_>,
    name: &str,
    node: usize,
    served: &Arc<Mutex<Vec<(String, bool)>>>,
) {
    let served = Arc::clone(served);
    let label = name.to_owned();
    cluster.client(name, async move {
        let outcome = run_transactions(TurmoilRuntime::new(), node).await;
        served.lock().unwrap().push((label, outcome.is_ok()));
        Ok(())
    });
}

#[test]
fn every_node_of_a_cluster_serves_its_own_clients() {
    let mut cluster = SimCluster::start(0, Duration::from_secs(60), &spec(3));
    for node in 0..3 {
        cluster.client(&format!("app-{node}"), async move {
            run_transactions(TurmoilRuntime::new(), node).await
        });
    }

    cluster.run().unwrap();

    assert!(
        cluster.stats().accepted() > 3,
        "each node must open server connections of its own beyond its observer"
    );
}

#[test]
fn killing_a_node_closes_its_server_connections_and_leaves_the_rest() {
    let mut cluster = SimCluster::start(0, Duration::from_secs(60), &spec(2));
    for node in 0..2 {
        cluster.client(&format!("app-{node}"), async move {
            run_transactions(TurmoilRuntime::new(), node).await
        });
    }
    cluster.run().unwrap();
    let before = cluster.stats().live();

    cluster.kill(0);
    cluster.run_for(Duration::from_secs(1)).unwrap();
    let after = cluster.stats().live();

    assert!(
        after < before,
        "the killed node's server connections must close: {before} before, {after} after"
    );
    assert!(
        after > 0,
        "the surviving node's server connections must stay open"
    );
}

#[test]
fn a_restarted_node_serves_clients_again() {
    let mut cluster = SimCluster::start(0, Duration::from_secs(60), &spec(1));
    cluster.client("before", async move {
        run_transactions(TurmoilRuntime::new(), 0).await
    });
    cluster.run().unwrap();

    cluster.kill(0);
    cluster.run_for(Duration::from_secs(1)).unwrap();
    assert_eq!(
        cluster.stats().live(),
        0,
        "a killed node must hold no server connection"
    );

    cluster.restart(0);
    cluster.client("after", async move {
        run_transactions(TurmoilRuntime::new(), 0).await
    });
    cluster.run().unwrap();
    assert!(cluster.stats().live() > 0);
}

#[test]
fn a_node_cut_off_from_the_database_serves_nobody_until_repaired() {
    let mut cluster = SimCluster::start(0, Duration::from_secs(60), &spec(2));
    for node in 0..2 {
        cluster.client(&format!("warm-{node}"), async move {
            run_transactions(TurmoilRuntime::new(), node).await
        });
    }
    cluster.run().unwrap();

    let node = SimCluster::node(0);
    cluster.partition(&node, DB);
    let served: Arc<Mutex<Vec<(String, bool)>>> = Arc::new(Mutex::new(Vec::new()));
    record_outcome(&mut cluster, "cut-0", 0, &served);
    record_outcome(&mut cluster, "cut-1", 1, &served);
    cluster.run_for(Duration::from_secs(5)).unwrap();
    assert!(
        !served.lock().unwrap().contains(&("cut-0".to_owned(), true)),
        "the node cut off from the database must not serve: {:?}",
        served.lock().unwrap()
    );
    assert!(
        served.lock().unwrap().contains(&("cut-1".to_owned(), true)),
        "the node that still reaches the database must serve: {:?}",
        served.lock().unwrap()
    );

    cluster.repair(&node, DB);
    record_outcome(&mut cluster, "repaired-0", 0, &served);
    cluster.run().unwrap();
    assert!(
        served
            .lock()
            .unwrap()
            .contains(&("repaired-0".to_owned(), true)),
        "the node must serve again once the link is repaired: {:?}",
        served.lock().unwrap()
    );
}

#[test]
fn a_slow_link_to_the_database_slows_only_the_node_behind_it() {
    let mut cluster = SimCluster::start(0, Duration::from_secs(60), &spec(2));
    for node in 0..2 {
        cluster.client(&format!("warm-{node}"), async move {
            run_transactions(TurmoilRuntime::new(), node).await
        });
    }
    cluster.run().unwrap();

    cluster.delay(&SimCluster::node(0), DB, Duration::from_millis(200));
    let started = cluster.elapsed();
    let done = Arc::new(Mutex::new(Vec::new()));
    record_when_done(&mut cluster, "slow", 0, &done);
    record_when_done(&mut cluster, "fast", 1, &done);
    cluster.run().unwrap();

    let done = done.lock().unwrap();
    let took = |name: &str| {
        done.iter()
            .find(|(label, _)| label == name)
            .map(|(_, at)| at.checked_sub(started).unwrap())
            .unwrap()
    };
    let round_trips = u32::try_from(TRANSACTIONS * 3).unwrap();
    assert!(
        took("slow") >= Duration::from_millis(400) * round_trips,
        "every statement on the slow node must cross the slow link twice: {:?}",
        took("slow")
    );
    assert!(took("fast") < Duration::from_millis(400) * round_trips);
}

fn trace(seed: u64) -> Vec<(String, Duration)> {
    let mut cluster = SimCluster::start(seed, Duration::from_secs(60), &spec(2));
    let done = Arc::new(Mutex::new(Vec::new()));
    for client in 0..6 {
        record_when_done(&mut cluster, &format!("app-{client}"), client % 2, &done);
    }
    cluster.run().unwrap();
    cluster.kill(1);
    cluster.restart(1);
    for client in 6..10 {
        record_when_done(&mut cluster, &format!("app-{client}"), client % 2, &done);
    }
    cluster.run().unwrap();
    Arc::try_unwrap(done).unwrap().into_inner().unwrap()
}

#[test]
fn the_same_seed_runs_the_cluster_the_same_way() {
    let traces: Vec<_> = (0..SEEDS).map(trace).collect();
    for (seed, first) in (0..SEEDS).zip(&traces) {
        assert_eq!(
            &trace(seed),
            first,
            "seed {seed}: the same seed must finish the same clients at the same instants"
        );
    }
    assert!(
        traces.windows(2).any(|pair| pair[0] != pair[1]),
        "different seeds must be able to run the cluster differently"
    );
}
