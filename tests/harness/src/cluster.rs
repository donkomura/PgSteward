use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::io;
use std::rc::Rc;
use std::time::Duration;

use pgsteward_core::budget::ServerLimits;
use pgsteward_core::rt::turmoil_rt::TurmoilRuntime;
use pgsteward_core::rt::{Clock, ClockRate, Net};
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use turmoil::Sim;

use crate::fake_postgres::{FakePostgres, FakePostgresStats};
use crate::faults::{Change, Fault, Scenario};

pub const DB: &str = "db";
const DB_PORT: u16 = 5432;
const NODE_PORT: u16 = 6432;
const LISTEN_RETRY: Duration = Duration::from_millis(10);
const MAX_LATENCY: Duration = Duration::from_millis(100);
const PROBE_RETRY: Duration = Duration::from_millis(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct ClusterSpec {
    pub nodes: usize,
    pub limits: ServerLimits,
    pub node: NodeConfig,
    pub cluster: ClusterConfig,
    pub options: ServeOptions,
    /// The rate of each node's clock by index. A node past the end of the list
    /// runs on the exact clock.
    pub clocks: Vec<ClockRate>,
}

/// One fake PostgreSQL and `nodes` nodes pointed at it, inside one turmoil
/// simulation whose randomness all comes from the seed.
pub struct SimCluster<'a> {
    sim: Sim<'a>,
    stats: FakePostgresStats,
    nodes: usize,
    probes: usize,
}

impl fmt::Debug for SimCluster<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimCluster")
            .field("elapsed", &self.sim.elapsed())
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl SimCluster<'_> {
    pub fn start(seed: u64, duration: Duration, spec: &ClusterSpec) -> Self {
        let mut sim = turmoil::Builder::new()
            .rng_seed(seed)
            .simulation_duration(duration)
            .min_message_latency(Duration::ZERO)
            .max_message_latency(MAX_LATENCY)
            .build();
        let stats = FakePostgresStats::default();
        start_db(&mut sim, stats.clone(), spec.limits);
        for index in 0..spec.nodes {
            start_node(&mut sim, index, spec);
        }
        Self {
            sim,
            stats,
            nodes: spec.nodes,
            probes: 0,
        }
    }

    pub fn node(index: usize) -> String {
        format!("node-{index}")
    }

    pub fn stats(&self) -> &FakePostgresStats {
        &self.stats
    }

    pub fn elapsed(&self) -> Duration {
        self.sim.elapsed()
    }

    pub fn client<F>(&mut self, name: &str, client: F)
    where
        F: Future<Output = turmoil::Result> + 'static,
    {
        self.sim.client(name, client);
    }

    pub fn kill(&mut self, index: usize) {
        self.sim.crash(Self::node(index));
    }

    pub fn restart(&mut self, index: usize) {
        self.sim.bounce(Self::node(index));
    }

    pub fn partition(&self, a: &str, b: &str) {
        self.sim.partition(a, b);
    }

    pub fn repair(&self, a: &str, b: &str) {
        self.sim.repair(a, b);
    }

    pub fn delay(&self, a: &str, b: &str, latency: Duration) {
        self.sim.set_link_latency(a, b, latency);
    }

    /// Applies each change of the scenario at its offset from now, and returns
    /// once the last fault has healed.
    pub fn play(&mut self, scenario: &Scenario) -> turmoil::Result {
        let start = self.sim.elapsed();
        for (at, node, change) in scenario.changes() {
            self.run_until(start + at)?;
            self.apply(node, change);
        }
        Ok(())
    }

    fn apply(&mut self, node: usize, change: Change) {
        let host = Self::node(node);
        match change {
            Change::Begin(Fault::Crash) => self.kill(node),
            Change::End(Fault::Crash) => self.restart(node),
            Change::Begin(Fault::CutOff) => self.partition(&host, DB),
            Change::End(Fault::CutOff) => self.repair(&host, DB),
            Change::Begin(Fault::Slow(latency)) => self.delay(&host, DB, latency),
            Change::End(Fault::Slow(_)) => {
                self.sim.set_link_latency(host.as_str(), DB, Duration::ZERO);
                self.sim
                    .set_link_max_message_latency(host.as_str(), DB, MAX_LATENCY);
            }
        }
    }

    /// Measures, for each node, the simulated time from now until `probe`
    /// first succeeds against it. A probe that has not finished within
    /// `PROBE_TIMEOUT` is abandoned and tried again, since turmoil does not
    /// resend what a partition dropped. Fails if the simulation ends first.
    pub fn recovery<P, F>(&mut self, probe: P) -> turmoil::Result<Vec<Duration>>
    where
        P: Fn(TurmoilRuntime, usize) -> F + Clone + 'static,
        F: Future<Output = io::Result<()>> + 'static,
    {
        let from = self.sim.elapsed();
        let recovered = Rc::new(RefCell::new(vec![None; self.nodes]));
        for node in 0..self.nodes {
            let probe = probe.clone();
            let recovered = Rc::clone(&recovered);
            self.probes += 1;
            self.sim
                .client(format!("probe-{}", self.probes), async move {
                    let rt = TurmoilRuntime::new();
                    loop {
                        let served = tokio::select! {
                            outcome = probe(rt, node) => outcome.is_ok(),
                            () = rt.sleep(PROBE_TIMEOUT) => false,
                        };
                        if served {
                            break;
                        }
                        rt.sleep(PROBE_RETRY).await;
                    }
                    recovered.borrow_mut()[node] = turmoil::sim_elapsed();
                    Ok(())
                });
        }
        self.sim.run()?;
        let recovered = recovered.borrow();
        Ok(recovered
            .iter()
            .map(|at| {
                at.expect("every probe has finished once the simulation has run")
                    .checked_sub(from)
                    .expect("a probe finishes after it starts")
            })
            .collect())
    }

    /// Runs until every client added so far has finished.
    pub fn run(&mut self) -> turmoil::Result {
        self.sim.run()
    }

    /// Runs for `span` of simulated time, whether or not the clients finish.
    pub fn run_for(&mut self, span: Duration) -> turmoil::Result {
        self.run_until(self.sim.elapsed() + span)
    }

    fn run_until(&mut self, until: Duration) -> turmoil::Result {
        while self.sim.elapsed() < until {
            self.sim.step()?;
        }
        Ok(())
    }
}

/// A node listens only once it has derived the total budget, so a client that
/// starts with the cluster is refused until then.
pub async fn connect_to_node(
    rt: &TurmoilRuntime,
    index: usize,
) -> io::Result<<TurmoilRuntime as Net>::Stream> {
    let addr = format!("{}:{NODE_PORT}", SimCluster::node(index));
    loop {
        match rt.connect(&addr).await {
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                rt.sleep(LISTEN_RETRY).await;
            }
            connected => return connected,
        }
    }
}

fn start_db(sim: &mut Sim<'_>, stats: FakePostgresStats, limits: ServerLimits) {
    sim.host(DB, move || {
        let stats = stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start_with_limits(&rt, &format!("0.0.0.0:{DB_PORT}"), stats, limits)
                .await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn start_node(sim: &mut Sim<'_>, index: usize, spec: &ClusterSpec) {
    let spec = spec.clone();
    let clock = spec.clocks.get(index).copied().unwrap_or(ClockRate::EXACT);
    sim.host(SimCluster::node(index), move || {
        let spec = spec.clone();
        async move {
            let rt = TurmoilRuntime::with_rate(clock);
            let serving = serve(rt, spec.node, spec.cluster, spec.options).await?;
            std::future::pending::<()>().await;
            drop(serving);
            Ok(())
        }
    });
}
