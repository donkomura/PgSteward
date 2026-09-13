use std::time::Duration;

use pgsteward_core::rt::{Clock, Net, Spawner, turmoil_rt::TurmoilRuntime};
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
