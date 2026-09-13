use tokio_postgres::Client;

use crate::cap::{ObserveConnections, ObserveError};

#[derive(Debug)]
pub struct PgStatActivity {
    client: Client,
    application_name_prefix: String,
}

impl PgStatActivity {
    #[must_use]
    pub fn new(client: Client, application_name_prefix: impl Into<String>) -> Self {
        Self {
            client,
            application_name_prefix: application_name_prefix.into(),
        }
    }
}

impl ObserveConnections for PgStatActivity {
    async fn observe(&self) -> Result<usize, ObserveError> {
        let row = self
            .client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend' AND left(application_name, length($1::text)) = $1::text",
                &[&self.application_name_prefix],
            )
            .await
            .map_err(|e| ObserveError(e.to_string()))?;
        let count: i64 = row.get(0);
        usize::try_from(count).map_err(|e| ObserveError(e.to_string()))
    }
}
