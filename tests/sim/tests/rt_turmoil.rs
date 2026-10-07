use std::num::NonZeroU32;
use std::time::Duration;

use pgsteward_core::rt::turmoil_rt::TurmoilRuntime;
use pgsteward_core::rt::{Clock, ClockRate, Net, Spawner};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn clock_is_deterministic_under_simulation() {
    let mut sim = turmoil::Builder::new().build();
    sim.client("app", async {
        let rt = TurmoilRuntime::new();
        let before = rt.now();
        rt.sleep(Duration::from_secs(3)).await;
        assert_eq!(rt.now().duration_since(before), Duration::from_secs(3));
        Ok(())
    });
    sim.run().unwrap();
}

fn rate(per_million: u32) -> ClockRate {
    ClockRate::per_million(NonZeroU32::new(per_million).unwrap())
}

fn sleep_on(rate: ClockRate, duration: Duration) -> (Duration, Duration) {
    let mut sim = turmoil::Builder::new().build();
    let measured = std::rc::Rc::new(std::cell::Cell::new((Duration::ZERO, Duration::ZERO)));
    let out = std::rc::Rc::clone(&measured);
    sim.client("app", async move {
        let rt = TurmoilRuntime::with_rate(rate);
        let local = rt.now();
        let simulated = turmoil::sim_elapsed().unwrap();
        rt.sleep(duration).await;
        out.set((
            rt.now().duration_since(local),
            turmoil::sim_elapsed()
                .unwrap()
                .checked_sub(simulated)
                .unwrap(),
        ));
        Ok(())
    });
    sim.run().unwrap();
    measured.get()
}

#[test]
fn a_fast_clock_sleeps_through_its_duration_in_less_simulated_time() {
    let (local, simulated) = sleep_on(rate(2_000_000), Duration::from_secs(4));
    assert_eq!(local, Duration::from_secs(4));
    assert_eq!(simulated, Duration::from_secs(2));
}

#[test]
fn a_slow_clock_sleeps_through_its_duration_in_more_simulated_time() {
    let (local, simulated) = sleep_on(rate(500_000), Duration::from_secs(1));
    assert_eq!(local, Duration::from_secs(1));
    assert_eq!(simulated, Duration::from_secs(2));
}

#[test]
fn a_skewed_clock_never_wakes_before_its_own_duration() {
    let (local, _) = sleep_on(rate(3_000_000), Duration::from_secs(1));
    assert!(
        local >= Duration::from_secs(1),
        "the sleep must last its whole duration on the clock that measures it: {local:?}"
    );
}

#[test]
fn the_exact_rate_keeps_the_simulated_clock() {
    let (local, simulated) = sleep_on(ClockRate::EXACT, Duration::from_millis(1500));
    assert_eq!(local, Duration::from_millis(1500));
    assert_eq!(simulated, Duration::from_millis(1500));
}

#[test]
fn net_resolves_host_names_inside_simulation() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("db", || async {
        let rt = TurmoilRuntime::new();
        let listener = rt.bind("0.0.0.0:5432").await?;
        loop {
            let (mut stream, _) = listener.accept().await?;
            rt.spawn(async move {
                let mut buf = [0u8; 4];
                if stream.read_exact(&mut buf).await.is_ok() {
                    let _ = stream.write_all(&buf).await;
                }
            });
        }
    });

    sim.client("app", async {
        let rt = TurmoilRuntime::new();
        let mut stream = rt.connect("db:5432").await?;
        stream.write_all(b"ping").await?;
        let mut echoed = [0u8; 4];
        stream.read_exact(&mut echoed).await?;
        assert_eq!(&echoed, b"ping");
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn a_worker_runs_inside_the_simulation() {
    let mut sim = turmoil::Builder::new().build();
    sim.client("app", async {
        let rt = TurmoilRuntime::new();
        let (report, reported) = tokio::sync::oneshot::channel();
        rt.spawn_worker("worker".to_owned(), async move {
            TurmoilRuntime::new().sleep(Duration::from_secs(1)).await;
            let _ = report.send(());
        })?;
        let before = rt.now();
        reported.await?;
        assert_eq!(rt.now().duration_since(before), Duration::from_secs(1));
        Ok(())
    });
    sim.run().unwrap();
}
