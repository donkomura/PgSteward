use std::fmt;

use pgsteward_protocol::startup::StartupMessage;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId {
    user: String,
    database: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TenantResolveError {
    #[error("no user name in the startup message; a tenant is a user and a database")]
    MissingUser,
}

impl TenantId {
    pub fn new(user: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            database: database.into(),
        }
    }

    pub fn from_startup(startup: &StartupMessage) -> Result<Self, TenantResolveError> {
        let user = startup.user().ok_or(TenantResolveError::MissingUser)?;
        let database = startup.database().unwrap_or(user);
        Ok(Self::new(user, database))
    }

    #[must_use]
    pub fn user(&self) -> &str {
        &self.user
    }

    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.user, self.database)
    }
}
