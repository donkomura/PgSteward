use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pgsteward_core::rt::{Instant, JoinHandle, Runtime};

#[derive(Debug, thiserror::Error)]
#[error("failed to observe connection count: {0}")]
pub struct ObserveError(pub String);

pub trait ObserveConnections: Send + Sync + 'static {
    fn observe(&self) -> impl Future<Output = Result<usize, ObserveError>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapViolation {
    pub observed: usize,
    pub cap: usize,
    pub at: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct CapReport {
    peak: usize,
    samples: usize,
    violations: Vec<CapViolation>,
    observe_errors: Vec<String>,
}

impl CapReport {
    #[must_use]
    pub fn peak(&self) -> usize {
        self.peak
    }

    #[must_use]
    pub fn samples(&self) -> usize {
        self.samples
    }

    #[must_use]
    pub fn violations(&self) -> &[CapViolation] {
        &self.violations
    }

    #[must_use]
    pub fn observe_errors(&self) -> &[String] {
        &self.observe_errors
    }

    pub fn assert_never_exceeded(&self) {
        assert!(
            self.violations.is_empty(),
            "connection cap exceeded {} time(s) over {} sample(s): {}",
            self.violations.len(),
            self.samples,
            self
        );
        assert!(
            self.observe_errors.is_empty(),
            "connection count could not be observed: {:?}",
            self.observe_errors
        );
    }

    fn record(&mut self, observed: usize, cap: usize, at: Instant) {
        self.samples += 1;
        self.peak = self.peak.max(observed);
        if observed > cap {
            self.violations.push(CapViolation { observed, cap, at });
        }
    }
}

impl fmt::Display for CapReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "peak={} samples={}", self.peak, self.samples)?;
        for v in &self.violations {
            write!(
                f,
                " [observed {} > cap {} at {:?}]",
                v.observed, v.cap, v.at
            )?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct CapMonitor {
    report: Arc<Mutex<CapReport>>,
    task: JoinHandle<()>,
}

impl CapMonitor {
    pub fn start<R, O>(rt: &R, observer: O, cap: usize, interval: Duration) -> Self
    where
        R: Runtime,
        O: ObserveConnections,
    {
        let report = Arc::new(Mutex::new(CapReport::default()));
        let shared = Arc::clone(&report);
        let clock = rt.clone();
        let task = rt.spawn(async move {
            loop {
                match observer.observe().await {
                    Ok(count) => shared.lock().expect("cap report lock poisoned").record(
                        count,
                        cap,
                        clock.now(),
                    ),
                    Err(err) => shared
                        .lock()
                        .expect("cap report lock poisoned")
                        .observe_errors
                        .push(err.to_string()),
                }
                clock.sleep(interval).await;
            }
        });
        Self { report, task }
    }

    #[must_use]
    pub fn snapshot(&self) -> CapReport {
        self.report
            .lock()
            .expect("cap report lock poisoned")
            .clone()
    }

    pub async fn stop(self) -> CapReport {
        let Self { report, task } = self;
        task.abort();
        let _ = task.await;
        report.lock().expect("cap report lock poisoned").clone()
    }
}
