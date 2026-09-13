use std::time::Duration;

use pgsteward_core::rt::{Clock, Net, turmoil_rt::TurmoilRuntime};
use pgsteward_harness::cap::CapMonitor;
use pgsteward_harness::fake_postgres::{FakePostgres, FakePostgresStats};

fn run_with_connections(opened: usize, cap: usize) -> pgsteward_harness::cap::CapReport {
    let stats = FakePostgresStats::default();
    let mut sim = turmoil::Builder::new().build();

    let server_stats = stats.clone();
    sim.host("db", move || {
        let stats = server_stats.clone();
        async move {
            let rt = TurmoilRuntime::new();
            FakePostgres::start(&rt, "0.0.0.0:5432", stats).await?;
            std::future::pending::<()>().await;
            Ok(())
        }
    });

    let client_stats = stats.clone();
    sim.client("app", async move {
        let rt = TurmoilRuntime::new();
        let monitor = CapMonitor::start(&rt, client_stats, cap, Duration::from_millis(1));
        let mut held = Vec::new();
        for _ in 0..opened {
            held.push(rt.connect("db:5432").await?);
        }
        rt.sleep(Duration::from_millis(50)).await;
        drop(held);
        rt.sleep(Duration::from_millis(50)).await;
        let report = monitor.stop().await;
        REPORT.with(|slot| *slot.borrow_mut() = Some(report));
        Ok(())
    });

    sim.run().unwrap();
    REPORT.with(|slot| slot.borrow_mut().take()).unwrap()
}

thread_local! {
    static REPORT: std::cell::RefCell<Option<pgsteward_harness::cap::CapReport>> = const { std::cell::RefCell::new(None) };
}

#[test]
fn fake_postgres_counts_live_connections_and_releases_them() {
    let report = run_with_connections(3, 3);
    assert_eq!(report.peak(), 3);
    report.assert_never_exceeded();
}

#[test]
fn fake_postgres_reports_cap_violation() {
    let report = run_with_connections(4, 3);
    assert_eq!(report.peak(), 4);
    assert!(!report.violations().is_empty());
}
