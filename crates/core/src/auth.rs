use std::collections::BTreeMap;

use crate::scram::ScramVerifier;
use crate::tenant::TenantId;

#[derive(Debug, Clone)]
pub enum AuthMethod {
    Trust,
    ScramSha256(ScramVerifier),
}

pub trait Credentials {
    fn method(&self, tenant: &TenantId) -> Option<AuthMethod>;
}

impl<C: Credentials + ?Sized> Credentials for &C {
    fn method(&self, tenant: &TenantId) -> Option<AuthMethod> {
        (*self).method(tenant)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TrustAll;

impl Credentials for TrustAll {
    fn method(&self, _tenant: &TenantId) -> Option<AuthMethod> {
        Some(AuthMethod::Trust)
    }
}

/// The users this node can authenticate, looked up by user name because a
/// tenant is a user and a database while a verifier belongs to the user alone.
/// A user the table does not name has no method: the caller still plays the
/// exchange out with a mock verifier, so that asking cannot tell whether a user
/// exists.
#[derive(Debug, Clone, Default)]
pub struct ClientCredentials {
    users: BTreeMap<String, AuthMethod>,
}

impl ClientCredentials {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, user: impl Into<String>, method: AuthMethod) {
        self.users.insert(user.into(), method);
    }
}

impl Credentials for ClientCredentials {
    fn method(&self, tenant: &TenantId) -> Option<AuthMethod> {
        self.users.get(tenant.user()).cloned()
    }
}
