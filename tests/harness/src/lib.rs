pub mod cap;
pub mod fake_postgres;
#[cfg(feature = "postgres")]
pub mod pg_stat_activity;

use std::sync::Arc;

use pgsteward_core::tls::{ServerTls, SslMode};

/// How tests open server connections: they ask for encryption and carry on in
/// the clear, which is what both the fake PostgreSQL and a PostgreSQL built
/// without a certificate answer.
#[must_use]
pub fn server_tls() -> Arc<ServerTls> {
    Arc::new(ServerTls::new(SslMode::Prefer, None).expect("prefer reads no certificate"))
}
