use std::collections::BTreeMap;
use std::future::{Future, ready};
use std::time::Duration;

use pgsteward_core::budget::ServerLimits;
use pgsteward_core::inspect::{
    InspectError, count_foreign_connections, read_server_limits, read_timeout_settings,
};
use pgsteward_core::server::{ApplicationName, QueryError, Row, SimpleQuery};

const UNRECOGNIZED_PARAMETER: &str = "42704";

#[derive(Debug, Default)]
struct FakeServer {
    settings: BTreeMap<String, Result<String, (String, String)>>,
    client_backends: Vec<String>,
    asked: Vec<String>,
}

impl FakeServer {
    fn with_settings<'a>(settings: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        Self {
            settings: settings
                .into_iter()
                .map(|(name, value)| (name.to_owned(), Ok(value.to_owned())))
                .collect(),
            ..Self::default()
        }
    }

    fn refusing(mut self, setting: &str, code: &str, message: &str) -> Self {
        self.settings.insert(
            setting.to_owned(),
            Err((code.to_owned(), message.to_owned())),
        );
        self
    }

    fn with_client_backends<'a>(names: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            client_backends: names.into_iter().map(str::to_owned).collect(),
            ..Self::default()
        }
    }

    fn asked(&self) -> &[String] {
        &self.asked
    }

    fn show(&self, setting: &str) -> Result<Vec<Row>, QueryError> {
        match self.settings.get(setting) {
            Some(Ok(value)) => Ok(vec![vec![Some(value.clone())]]),
            Some(Err((code, message))) => Err(QueryError::Server {
                code: code.clone(),
                message: message.clone(),
            }),
            None => Err(QueryError::Server {
                code: UNRECOGNIZED_PARAMETER.to_owned(),
                message: format!("unrecognized configuration parameter \"{setting}\""),
            }),
        }
    }

    fn count_foreign(&self) -> Vec<Row> {
        let count = self
            .client_backends
            .iter()
            .filter(|name| !name.starts_with(ApplicationName::PREFIX))
            .count();
        vec![vec![Some(count.to_string())]]
    }
}

impl SimpleQuery for FakeServer {
    fn simple_query(&mut self, sql: &str) -> impl Future<Output = Result<Vec<Row>, QueryError>> {
        self.asked.push(sql.to_owned());
        ready(match sql.strip_prefix("SHOW ") {
            Some(setting) => self.show(setting),
            None => Ok(self.count_foreign()),
        })
    }
}

#[tokio::test]
async fn the_server_limits_are_read_from_max_connections_and_both_reservations() {
    let mut server = FakeServer::with_settings([
        ("max_connections", "200"),
        ("superuser_reserved_connections", "3"),
        ("reserved_connections", "5"),
    ]);

    let limits = read_server_limits(&mut server).await.unwrap();

    assert_eq!(
        limits,
        ServerLimits {
            max_connections: 200,
            superuser_reserved_connections: 3,
            reserved_connections: 5,
        }
    );
    assert_eq!(limits.reserved(), 8);
}

#[tokio::test]
async fn a_server_without_reserved_connections_reserves_nothing_for_it() {
    let mut server = FakeServer::with_settings([
        ("max_connections", "100"),
        ("superuser_reserved_connections", "3"),
    ]);

    let limits = read_server_limits(&mut server).await.unwrap();

    assert_eq!(limits.reserved_connections, 0);
    assert_eq!(limits.reserved(), 3);
}

#[tokio::test]
async fn a_server_without_max_connections_is_an_error() {
    let mut server = FakeServer::with_settings([("superuser_reserved_connections", "3")]);

    let err = read_server_limits(&mut server).await.unwrap_err();

    match err {
        InspectError::Missing(setting) => assert_eq!(setting, "max_connections"),
        other => panic!("expected the setting to be reported as missing, got {other:?}"),
    }
}

#[tokio::test]
async fn a_refused_show_is_reported_with_its_sqlstate() {
    let mut server = FakeServer::with_settings([("max_connections", "100")]).refusing(
        "superuser_reserved_connections",
        "42501",
        "permission denied",
    );

    let err = read_server_limits(&mut server).await.unwrap_err();

    match err {
        InspectError::Query(QueryError::Server { code, .. }) => assert_eq!(code, "42501"),
        other => panic!("expected the server error to be reported, got {other:?}"),
    }
}

#[tokio::test]
async fn a_max_connections_that_is_not_a_number_is_an_error() {
    let mut server = FakeServer::with_settings([("max_connections", "many")]);

    let err = read_server_limits(&mut server).await.unwrap_err();

    match err {
        InspectError::Unreadable { what, value } => {
            assert_eq!(what, "max_connections");
            assert_eq!(value, "many");
        }
        other => panic!("expected an unreadable value, got {other:?}"),
    }
}

#[tokio::test]
async fn the_timeout_settings_are_read_in_the_units_the_server_displays() {
    let mut server =
        FakeServer::with_settings([("tcp_keepalives_idle", "45s"), ("tcp_user_timeout", "20s")]);

    let timeouts = read_timeout_settings(&mut server).await.unwrap();

    assert_eq!(timeouts.tcp_keepalives_idle, Duration::from_secs(45));
    assert_eq!(timeouts.tcp_user_timeout, Duration::from_secs(20));
}

#[tokio::test]
async fn a_timeout_the_server_scales_to_another_unit_keeps_its_length() {
    let mut server = FakeServer::with_settings([
        ("tcp_keepalives_idle", "1min"),
        ("tcp_user_timeout", "500ms"),
    ]);

    let timeouts = read_timeout_settings(&mut server).await.unwrap();

    assert_eq!(timeouts.tcp_keepalives_idle, Duration::from_secs(60));
    assert_eq!(timeouts.tcp_user_timeout, Duration::from_millis(500));
}

#[tokio::test]
async fn a_timeout_of_zero_is_read_as_disabled() {
    let mut server =
        FakeServer::with_settings([("tcp_keepalives_idle", "0"), ("tcp_user_timeout", "0")]);

    let timeouts = read_timeout_settings(&mut server).await.unwrap();

    assert_eq!(timeouts.tcp_keepalives_idle, Duration::ZERO);
    assert_eq!(timeouts.tcp_user_timeout, Duration::ZERO);
}

#[tokio::test]
async fn a_timeout_in_an_unknown_unit_is_an_error() {
    let mut server = FakeServer::with_settings([
        ("tcp_keepalives_idle", "45s"),
        ("tcp_user_timeout", "20 fortnights"),
    ]);

    let err = read_timeout_settings(&mut server).await.unwrap_err();

    match err {
        InspectError::Unreadable { what, value } => {
            assert_eq!(what, "tcp_user_timeout");
            assert_eq!(value, "20 fortnights");
        }
        other => panic!("expected an unreadable value, got {other:?}"),
    }
}

#[tokio::test]
async fn the_foreign_connections_are_the_client_backends_this_system_did_not_open() {
    let mut server = FakeServer::with_client_backends([
        "pgsteward-node-1",
        "pgsteward-node-2",
        "psql",
        "monitoring",
        "",
    ]);

    let foreign = count_foreign_connections(&mut server).await.unwrap();

    assert_eq!(foreign, 3);
    let sql = server.asked().last().unwrap();
    assert!(sql.contains("pg_stat_activity"), "{sql}");
    assert!(sql.contains("client backend"), "{sql}");
    assert!(sql.contains(ApplicationName::PREFIX), "{sql}");
}

#[tokio::test]
async fn a_missing_count_of_the_foreign_connections_is_an_error() {
    struct Babbling;

    impl SimpleQuery for Babbling {
        fn simple_query(
            &mut self,
            _sql: &str,
        ) -> impl Future<Output = Result<Vec<Row>, QueryError>> {
            ready(Ok(vec![vec![None]]))
        }
    }

    let err = count_foreign_connections(&mut Babbling).await.unwrap_err();

    assert!(matches!(err, InspectError::Missing(_)), "{err:?}");
}
