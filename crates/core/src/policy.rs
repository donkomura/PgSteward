use std::collections::BTreeMap;
use std::num::NonZeroU32;

use crate::allocation::InstanceId;
use crate::grant::TenantPolicy;
use crate::tenant::TenantId;

const WILDCARD: &str = "*";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRule {
    pub instances: Vec<InstanceId>,
    pub policy: TenantPolicy,
}

/// The cluster's tenant rules, keyed as the cluster config writes them.
///
/// A tenant is resolved by the most specific key that names it: `user@database`
/// first, then `user`, then `*`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Policies {
    weights: BTreeMap<InstanceId, NonZeroU32>,
    rules: BTreeMap<String, TenantRule>,
}

impl Policies {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn instance(mut self, instance: InstanceId, weight: NonZeroU32) -> Self {
        self.weights.insert(instance, weight);
        self
    }

    #[must_use]
    pub fn tenant(mut self, key: impl Into<String>, rule: TenantRule) -> Self {
        self.rules.insert(key.into(), rule);
        self
    }

    #[must_use]
    pub fn resolve(&self, tenant: &TenantId) -> Option<&TenantRule> {
        self.rules
            .get(&tenant.to_string())
            .or_else(|| self.rules.get(tenant.user()))
            .or_else(|| self.rules.get(WILDCARD))
    }

    #[must_use]
    pub fn policy(&self, instance: &InstanceId, tenant: &TenantId) -> Option<TenantPolicy> {
        self.resolve(tenant)
            .filter(|rule| rule.instances.contains(instance))
            .map(|rule| rule.policy)
    }

    /// Picks the instance a new client connection of `tenant` goes to, by the
    /// weights of the instances its rule lists. `roll` is given the sum of those
    /// weights and returns a number below it.
    pub fn route(&self, tenant: &TenantId, roll: impl FnOnce(u32) -> u32) -> Option<&InstanceId> {
        let rule = self.resolve(tenant)?;
        let weight = |instance: &InstanceId| self.weights.get(instance).map_or(1, |w| w.get());
        let total: u32 = rule.instances.iter().map(weight).sum();
        if total == 0 {
            return None;
        }
        let mut left = roll(total) % total;
        for instance in &rule.instances {
            let weight = weight(instance);
            if left < weight {
                return Some(instance);
            }
            left -= weight;
        }
        None
    }
}
