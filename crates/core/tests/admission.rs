use std::time::Duration;

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use pgsteward_core::admission::{Admitted, ClientLimit};
use pgsteward_core::auth::{AuthMethod, Credentials, TrustAll};
use pgsteward_core::scram::{DEFAULT_ITERATIONS, ScramVerifier};
use pgsteward_core::session::{AcceptError, Accepted};
use pgsteward_core::tenant::TenantId;
use pgsteward_core::tls::MaybeTls;
use pgsteward_protocol::framing::{decode_frame, encode_frame};
use pgsteward_protocol::startup::{
    ProtocolVersion, StartupMessage, StartupRequest, encode_startup,
};
use postgres_protocol::message::backend::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::time::timeout;

const MAX_FRAME: usize = 1 << 20;
const DUPLEX_CAPACITY: usize = 64 * 1024;

struct Client {
    stream: DuplexStream,
}

impl Client {
    async fn arrive(user: &str) -> (Self, DuplexStream) {
        let (mut stream, proxy) = duplex(DUPLEX_CAPACITY);
        let mut out = BytesMut::new();
        encode_startup(
            &StartupRequest::Startup(StartupMessage::new(
                ProtocolVersion::V3_0,
                vec![
                    ("user".to_owned(), user.to_owned()),
                    ("database".to_owned(), "shop".to_owned()),
                ],
            )),
            &mut out,
        );
        stream.write_all(&out).await.unwrap();
        stream.flush().await.unwrap();
        (Self { stream }, proxy)
    }

    async fn read_message(&mut self) -> Message {
        let mut buf = BytesMut::new();
        let frame = loop {
            if let Some(frame) = decode_frame(&mut buf, MAX_FRAME).unwrap() {
                break frame;
            }
            assert!(
                self.stream.read_buf(&mut buf).await.unwrap() > 0,
                "the proxy closed without answering"
            );
        };
        let mut bytes = BytesMut::new();
        encode_frame(frame.tag, &frame.body, &mut bytes);
        Message::parse(&mut bytes).unwrap().unwrap()
    }

    async fn read_refusal(&mut self) -> (String, String) {
        let Message::ErrorResponse(body) = self.read_message().await else {
            panic!("expected an ErrorResponse");
        };
        let mut fields = body.fields();
        let mut code = String::new();
        let mut message = String::new();
        while let Ok(Some(field)) = fields.next() {
            let value = String::from_utf8_lossy(field.value_bytes()).into_owned();
            match field.type_() {
                b'C' => code = value,
                b'M' => message = value,
                _ => {}
            }
        }
        (code, message)
    }
}

async fn admit(
    limit: &ClientLimit,
    proxy: DuplexStream,
) -> Result<(Admitted, Accepted<MaybeTls<DuplexStream>>), AcceptError> {
    limit.accept(proxy, TrustAll, None).await
}

#[tokio::test]
async fn a_node_accepts_up_to_its_client_connection_limit() {
    let limit = ClientLimit::new(2);

    let (_first, first_proxy) = Client::arrive("app_web").await;
    let (_second, second_proxy) = Client::arrive("app_web").await;
    let held = vec![
        admit(&limit, first_proxy).await.unwrap(),
        admit(&limit, second_proxy).await.unwrap(),
    ];
    assert_eq!(limit.live(), 2);

    let (mut third, third_proxy) = Client::arrive("app_web").await;
    let error = admit(&limit, third_proxy).await.unwrap_err();
    assert!(
        matches!(error, AcceptError::TooManyClients { max: 2 }),
        "expected a refusal naming the limit, got {error:?}"
    );

    let (code, message) = third.read_refusal().await;
    assert_eq!(code, "53300");
    assert!(
        message.contains('2'),
        "the refusal must tell the client what the limit is: {message}"
    );
    assert_eq!(limit.live(), 2, "a refused client takes no place");
    drop(held);
}

#[tokio::test]
async fn a_finished_client_connection_gives_its_place_back() {
    let limit = ClientLimit::new(1);

    let (_first, first_proxy) = Client::arrive("app_web").await;
    let admitted = admit(&limit, first_proxy).await.unwrap();
    assert_eq!(limit.live(), 1);
    drop(admitted);
    assert_eq!(limit.live(), 0);

    let (_second, second_proxy) = Client::arrive("app_web").await;
    let (_admitted, accepted) = admit(&limit, second_proxy).await.unwrap();
    assert!(matches!(accepted, Accepted::Session(_)));
}

#[tokio::test]
async fn a_client_over_the_limit_is_refused_without_being_authenticated() {
    let limit = ClientLimit::new(1);
    let _held = limit.admit().expect("the only place is free");

    let (mut client, proxy) = Client::arrive("app_web").await;
    let error = limit
        .accept(proxy, OneVerifier::for_user("app_web", "secret"), None)
        .await
        .unwrap_err();
    assert!(matches!(error, AcceptError::TooManyClients { max: 1 }));

    let (code, _) = client.read_refusal().await;
    assert_eq!(
        code, "53300",
        "the refusal must come instead of a password request"
    );
}

#[tokio::test]
async fn a_refused_client_reads_the_refusal_before_the_node_closes() {
    let limit = ClientLimit::new(1);

    let (_first, first_proxy) = Client::arrive("app_web").await;
    let _admitted = admit(&limit, first_proxy).await.unwrap();

    let (mut second, second_proxy) = Client::arrive("app_web").await;
    admit(&limit, second_proxy).await.unwrap_err();

    assert_eq!(second.read_refusal().await.0, "53300");
    assert_eq!(
        second.stream.read_buf(&mut BytesMut::new()).await.unwrap(),
        0,
        "the node closes after the refusal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clients_arriving_at_once_never_pass_the_limit() {
    const MAX: usize = 8;
    const ARRIVALS: usize = 64;

    let limit = ClientLimit::new(MAX);
    let mut arrivals = Vec::with_capacity(ARRIVALS);
    for _ in 0..ARRIVALS {
        let limit = limit.clone();
        arrivals.push(tokio::spawn(async move { limit.admit() }));
    }

    let mut admitted = Vec::new();
    for arrival in arrivals {
        if let Some(place) = arrival.await.unwrap() {
            admitted.push(place);
        }
    }
    assert_eq!(admitted.len(), MAX);
    assert_eq!(limit.live(), MAX);

    admitted.clear();
    assert_eq!(limit.live(), 0);
}

#[tokio::test]
async fn a_node_that_holds_no_client_is_drained_at_once() {
    let limit = ClientLimit::new(4);

    timeout(Duration::from_secs(1), limit.drained())
        .await
        .expect("nothing is here to wait for");
}

#[tokio::test]
async fn draining_waits_for_the_client_that_is_still_here() {
    let limit = ClientLimit::new(4);
    let place = limit.admit().expect("a place under the limit");
    let drained = tokio::spawn({
        let limit = limit.clone();
        async move { limit.drained().await }
    });
    tokio::task::yield_now().await;
    assert!(!drained.is_finished(), "one client still holds a place");

    drop(place);

    timeout(Duration::from_secs(1), drained)
        .await
        .expect("the last client left")
        .unwrap();
}

#[derive(Debug, Clone)]
struct OneVerifier {
    user: String,
    password: String,
}

impl OneVerifier {
    fn for_user(user: &str, password: &str) -> Self {
        Self {
            user: user.to_owned(),
            password: password.to_owned(),
        }
    }
}

impl Credentials for OneVerifier {
    fn method(&self, tenant: &TenantId) -> Option<AuthMethod> {
        (tenant.user() == self.user).then(|| {
            AuthMethod::ScramSha256(ScramVerifier::from_password(
                &self.password,
                b"pgsteward-salt-1",
                DEFAULT_ITERATIONS,
            ))
        })
    }
}
