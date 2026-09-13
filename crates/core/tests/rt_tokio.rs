use std::time::Duration;

use pgsteward_core::rt::{Clock, Net, Spawner, tokio_rt::TokioRuntime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn clock_sleep_advances_now() {
    let rt = TokioRuntime::new();
    let before = rt.now();
    rt.sleep(Duration::from_millis(20)).await;
    assert!(rt.now().duration_since(before) >= Duration::from_millis(20));
}

#[tokio::test]
async fn spawner_runs_task_to_completion() {
    let rt = TokioRuntime::new();
    let handle = rt.spawn(async { 21 * 2 });
    assert_eq!(handle.await.unwrap(), 42);
}

#[tokio::test]
async fn net_bind_connect_roundtrip() {
    let rt = TokioRuntime::new();
    let listener = rt.bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = rt.spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        stream.write_all(&buf).await.unwrap();
    });

    let mut client = rt.connect(&addr.to_string()).await.unwrap();
    client.write_all(b"ping").await.unwrap();
    let mut echoed = [0u8; 4];
    client.read_exact(&mut echoed).await.unwrap();
    assert_eq!(&echoed, b"ping");
    server.await.unwrap();
}
