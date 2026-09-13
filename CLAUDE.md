# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Writing rules

- **Everything written into this repository is in English**: commit messages, PR titles and descriptions, documentation, error messages, log messages, and identifiers.
- **Never refer to plan steps by their labels (`M1`, `M1-02`, `M2-05`) in commit messages or PR descriptions.** Call them by name, for example "framing and the ReadyForQuery state machine" or "time and I/O abstraction". The labels are for use inside the design documents only.
- Use the vocabulary from the design document glossary (chapter 9)

## Design documents

- `docs/design-doc.md` is the canonical source. Read chapter 8 for the control model, 14.1 for configuration, and chapter 9 for the glossary. **It is internal and must never be published**: the path is gitignored, it exists only on the local machine, and it must not be committed, pushed, quoted at length in PRs, or copied into any published artifact.
- `docs/plan.md` is the implementation plan. Each step is one small change with tests written first.
- The comparison with existing poolers (`comparison.md`) and the research notes live outside the repository in `~/projects/distributed-connection-pooler/`.

When they disagree, `docs/design-doc.md` wins.

## Commands

```bash
cargo test --workspace --all-features --exclude pgsteward-integration-tests   # unit, property, and simulation tests
cargo test -p pgsteward-sim-tests --all-features                              # turmoil simulation only
cargo test -p pgsteward-integration-tests                                     # real PostgreSQL 16; needs Docker (testcontainers starts it)
cargo test -p pgsteward-protocol --test ready flush_without_sync              # a single test: crate, test file, test name
cargo clippy --workspace --all-features --all-targets                         # pedantic is on; CI runs with -D warnings
cargo fmt --all
cargo deny check
```

The turmoil implementation in `pgsteward-core` sits behind the `turmoil` feature, so pass `--all-features` whenever simulation code is involved.

## Architecture

### Control model

**The coordinator computes the desired state; proxies realize it.** The coordinator computes an allocation table (instance × tenant × proxy → number of connection slots) and proxies converge their actual server connections to what they were granted. A proxy never grows or shrinks its slots on its own. The only unilateral action a proxy takes is the self-fence (closing every connection), and it only ever moves in the shrinking direction.

The top-priority invariant is "for each instance, the number of actual connections on the database never exceeds that instance's total budget". A single violation is a failure, and it outranks availability and throughput.

Keep this role boundary even in the single-node stage. If the pool is given "grow" or "release" decisions, distribution later means a rewrite.

### Crate layers

Dependencies flow strictly downward.

| crate | responsibility | must not contain |
|---|---|---|
| `pgsteward-protocol` | wire protocol v3 framing, message types, ReadyForQuery state machine | I/O, clocks |
| `pgsteward-sched` | allocation algorithm (total budget + demand + current grants → desired state) | I/O, clocks |
| `pgsteward-core` | sessions, server connections, pool, the in-process degenerate form of the allocation table and allocator, `rt` | config files |
| `pgsteward-node` | the `pgsteward` binary: config, role selection, admin console | — |
| `tests/harness` | fake PostgreSQL, `pg_stat_activity` observer, the shared connection-cap assertion | dependencies from production code |

Every package name carries the `pgsteward-` prefix (`core` would collide with Rust's built-in crate).

### Time and I/O only through `rt`

Do not call `tokio::spawn`, `tokio::time`, `tokio::net`, or `std::time::Instant` outside `pgsteward_core::rt`. Pass around a value implementing the `Clock` / `Spawner` / `Net` traits (`TokioRuntime` or `TurmoilRuntime`). These are values rather than global functions because turmoil runs several hosts inside one process.

turmoil hosts run on a paused tokio runtime, so `tokio::time::Instant` is already deterministic there; only the network types need swapping.

### Connection-cap verification

Only the counting is abstracted (`ObserveConnections`); `CapMonitor` runs the same check against a real PostgreSQL (`pg_stat_activity`) and against the simulation (connections accepted by the fake PostgreSQL). It polls for the whole test, and `assert_never_exceeded` fails on a single excess sample. It also fails if any observation errored, because "zero violations" proves nothing for an unobserved interval.

### Two configuration layers, and the total budget is not a setting

Node-local config (role, listener, coordinator entry point, client-connection cap, TLS) and cluster config (instances, tenant min / max / weight). The total budget is derived from the database's `max_connections` and the observed foreign connections; it has no configuration key. Every struct is `deny_unknown_fields`, so `budget` and `pool_size` are rejected.

### ReadyForQuery state machine

`ReadyTracker::may_release()` is the single decision of whether a server connection may go back to the pool. It is true only when all three hold: no outstanding ReadyForQuery (incremented by Query / Sync / FunctionCall, decremented by ReadyForQuery), no open extended-query window (Parse through Sync; Flush does not close it), and the transaction status is idle.

## Workflow

- Move into a worktree with `git wt <branch>` before changing anything.
- For each step, write the tests first, confirm they fail because nothing is implemented, then implement. Do not change the tests while implementing.
- Do not write code comments.
- **Before every push, run the same checks CI runs, locally, and push only if all of them pass.** CI's clippy uses the current stable toolchain, so run `rustup update stable` first if the local toolchain is behind.

```bash
rustup update stable
cargo fmt --all --check
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-features --all-targets
cargo deny check
cargo test --workspace --all-features --exclude pgsteward-sim-tests --exclude pgsteward-integration-tests
cargo test -p pgsteward-sim-tests --all-features
cargo test -p pgsteward-integration-tests        # needs Docker
```

`cargo audit` also runs in CI; run it too when it is installed locally.
