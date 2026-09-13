use std::time::Duration;

use crate::budget::ServerLimits;
use crate::server::{ApplicationName, QueryError, Row, SimpleQuery};

// A server answers SHOW for a setting it does not have with SQLSTATE 42704
// (undefined_object). reserved_connections is one of those below PostgreSQL 16,
// so only this code means "this server has no such setting"; every other server
// error is passed on.
const SQLSTATE_UNDEFINED_OBJECT: &str = "42704";
const FOREIGN_CONNECTIONS: &str = "the number of foreign connections";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeoutSettings {
    pub tcp_keepalives_idle: Duration,
    pub tcp_user_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum InspectError {
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error("the server returned no value for {0}")]
    Missing(&'static str),
    #[error("cannot read {what} from the value {value:?} the server returned")]
    Unreadable { what: &'static str, value: String },
}

pub async fn read_server_limits<Q: SimpleQuery>(
    server: &mut Q,
) -> Result<ServerLimits, InspectError> {
    let max_connections = parse_count("max_connections", &show(server, "max_connections").await?)?;
    let superuser_reserved_connections = parse_count(
        "superuser_reserved_connections",
        &show(server, "superuser_reserved_connections").await?,
    )?;
    let reserved_connections = match show(server, "reserved_connections").await {
        Ok(value) => parse_count("reserved_connections", &value)?,
        Err(InspectError::Missing(_)) => 0,
        Err(error) => return Err(error),
    };
    Ok(ServerLimits {
        max_connections,
        superuser_reserved_connections,
        reserved_connections,
    })
}

pub async fn read_timeout_settings<Q: SimpleQuery>(
    server: &mut Q,
) -> Result<TimeoutSettings, InspectError> {
    let tcp_keepalives_idle = parse_duration(
        "tcp_keepalives_idle",
        &show(server, "tcp_keepalives_idle").await?,
        Duration::from_secs(1),
    )?;
    let tcp_user_timeout = parse_duration(
        "tcp_user_timeout",
        &show(server, "tcp_user_timeout").await?,
        Duration::from_millis(1),
    )?;
    Ok(TimeoutSettings {
        tcp_keepalives_idle,
        tcp_user_timeout,
    })
}

/// Counts the client backends this system did not open. The total budget is
/// derived by subtracting the peak of this count, because how many connections
/// an administrator's psql or a monitoring agent takes is in no configuration.
pub async fn count_foreign_connections<Q: SimpleQuery>(
    server: &mut Q,
) -> Result<u32, InspectError> {
    let prefix = ApplicationName::PREFIX;
    let sql = format!(
        "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND left(application_name, {}) IS DISTINCT FROM '{prefix}'",
        prefix.len()
    );
    let rows = server.simple_query(&sql).await?;
    let count = first_value(rows).ok_or(InspectError::Missing(FOREIGN_CONNECTIONS))?;
    parse_count(FOREIGN_CONNECTIONS, &count)
}

async fn show<Q: SimpleQuery>(
    server: &mut Q,
    setting: &'static str,
) -> Result<String, InspectError> {
    let rows = match server.simple_query(&format!("SHOW {setting}")).await {
        Ok(rows) => rows,
        Err(QueryError::Server { code, .. }) if code == SQLSTATE_UNDEFINED_OBJECT => {
            return Err(InspectError::Missing(setting));
        }
        Err(error) => return Err(error.into()),
    };
    first_value(rows).ok_or(InspectError::Missing(setting))
}

fn first_value(rows: Vec<Row>) -> Option<String> {
    rows.into_iter().next()?.into_iter().next().flatten()
}

fn parse_count(what: &'static str, value: &str) -> Result<u32, InspectError> {
    value.parse().map_err(|_| InspectError::Unreadable {
        what,
        value: value.to_owned(),
    })
}

fn parse_duration(
    what: &'static str,
    value: &str,
    base: Duration,
) -> Result<Duration, InspectError> {
    let unreadable = || InspectError::Unreadable {
        what,
        value: value.to_owned(),
    };
    let digits = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    let (amount, unit) = value.split_at(digits);
    let amount: u32 = amount.parse().map_err(|_| unreadable())?;
    let unit = match unit {
        "" => base,
        "us" => Duration::from_micros(1),
        "ms" => Duration::from_millis(1),
        "s" => Duration::from_secs(1),
        "min" => Duration::from_secs(60),
        "h" => Duration::from_secs(60 * 60),
        "d" => Duration::from_secs(24 * 60 * 60),
        _ => return Err(unreadable()),
    };
    unit.checked_mul(amount).ok_or_else(unreadable)
}
