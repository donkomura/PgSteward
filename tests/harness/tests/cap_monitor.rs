use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::rt::tokio_rt::TokioRuntime;
use pgsteward_harness::cap::{CapMonitor, ObserveConnections, ObserveError};

#[derive(Clone)]
struct Scripted(Arc<Mutex<Vec<usize>>>);

impl ObserveConnections for Scripted {
    async fn observe(&self) -> Result<usize, ObserveError> {
        let mut samples = self.0.lock().unwrap();
        Ok(if samples.len() > 1 {
            samples.remove(0)
        } else {
            samples[0]
        })
    }
}

#[tokio::test(start_paused = true)]
async fn records_peak_without_violation_when_under_cap() {
    let rt = TokioRuntime::new();
    let observer = Scripted(Arc::new(Mutex::new(vec![1, 3, 2])));
    let monitor = CapMonitor::start(&rt, observer, 3, Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let report = monitor.stop().await;
    assert_eq!(report.peak(), 3);
    assert!(report.violations().is_empty());
    report.assert_never_exceeded();
}

#[tokio::test(start_paused = true)]
async fn records_violation_when_cap_exceeded_once() {
    let rt = TokioRuntime::new();
    let observer = Scripted(Arc::new(Mutex::new(vec![1, 4, 1])));
    let monitor = CapMonitor::start(&rt, observer, 3, Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let report = monitor.stop().await;
    assert_eq!(report.peak(), 4);
    assert_eq!(report.violations().len(), 1);
    assert_eq!(report.violations()[0].observed, 4);
    assert_eq!(report.violations()[0].cap, 3);
}

#[tokio::test(start_paused = true)]
#[should_panic(expected = "connection cap exceeded")]
async fn assert_never_exceeded_panics_on_violation() {
    let rt = TokioRuntime::new();
    let observer = Scripted(Arc::new(Mutex::new(vec![5])));
    let monitor = CapMonitor::start(&rt, observer, 3, Duration::from_millis(10));
    tokio::time::sleep(Duration::from_millis(50)).await;
    monitor.stop().await.assert_never_exceeded();
}
