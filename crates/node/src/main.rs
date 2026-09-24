use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_node::config::{ClusterConfig, NodeConfig};
use pgsteward_node::serve::{ServeOptions, serve};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "A PostgreSQL connection pooler that keeps every instance within its total budget"
)]
struct Args {
    #[arg(long, value_name = "FILE")]
    config: PathBuf,
    #[arg(long, value_name = "FILE")]
    cluster: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let node = NodeConfig::parse(&read(&args.config)?)
        .with_context(|| format!("reading {}", args.config.display()))?;
    let cluster = ClusterConfig::parse(&read(&args.cluster)?)
        .with_context(|| format!("reading {}", args.cluster.display()))?;
    let serving = serve(TokioRuntime::new(), node, cluster, ServeOptions::default()).await?;
    tracing::info!(addr = %serving.local_addr(), "serving");
    stop_signal().await?;
    let stopped = serving.shutdown().await;
    tracing::info!(closed = stopped.closed, held = stopped.held, "stopped");
    Ok(())
}

/// Waits for the signal an orchestrator sends to take this node out of
/// service, so that the grants go back before the process ends rather than
/// being left to expire.
#[cfg(unix)]
async fn stop_signal() -> anyhow::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = terminate.recv() => Ok(()),
        result = tokio::signal::ctrl_c() => result.map_err(anyhow::Error::from),
    }
}

#[cfg(not(unix))]
async fn stop_signal() -> anyhow::Result<()> {
    tokio::signal::ctrl_c().await.map_err(anyhow::Error::from)
}

fn read(path: &PathBuf) -> anyhow::Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))
}
