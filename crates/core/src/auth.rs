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
