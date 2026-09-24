use std::collections::BTreeMap;
use std::num::NonZeroU32;

use crate::allocation::InstanceId;
use crate::grant::TenantPolicy;
use crate::tenant::TenantId;

const WILDCARD: &str = "*";

/// The parts of a tenant's share a setting writes. What it leaves out keeps
/// the value the cluster configuration already holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PolicyChange {
    pub min: Option<u32>,
    pub max: Option<u32>,
    pub weight: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettingError {
    #[error("the cluster configuration writes no tenant rule named `{tenant}`")]
    NoSuchTenant { tenant: String },
    #[error("the cluster configuration holds no instance named `{instance}`")]
    NoSuchInstance { instance: InstanceId },
    #[error("a minimum of {min} is above the maximum of {max}")]
    MinAboveMax { min: u32, max: u32 },
    #[error("a weight of 0 would leave the tenant out of every allocation")]
    ZeroWeight,
    #[error(
        "the minimums on instance `{instance}` would total {minimums}, above its total budget of {budget}"
    )]
    AboveBudget {
        instance: InstanceId,
        minimums: u32,
        budget: u32,
    },
}

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
    pub fn rule(&self, tenant: &str) -> Option<&TenantRule> {
        self.rules.get(tenant)
    }

    /// The rules with `change` written into the one named `tenant`.
    ///
    /// The name is a rule as the cluster configuration writes it, not a tenant
    /// the rule covers: a setting moves a rule, and the fallback from
    /// `user@database` to `user` to `*` would otherwise write the value into a
    /// rule the operator did not name. `budget` answers an instance's total
    /// budget, or nothing while it has not been derived yet.
    pub fn change_tenant(
        &self,
        tenant: &str,
        change: PolicyChange,
        budget: impl Fn(&InstanceId) -> Option<u32>,
    ) -> Result<Self, SettingError> {
        let rule = self
            .rule(tenant)
            .ok_or_else(|| SettingError::NoSuchTenant {
                tenant: tenant.to_owned(),
            })?;
        let policy = TenantPolicy {
            min: change.min.unwrap_or(rule.policy.min),
            max: change.max.unwrap_or(rule.policy.max),
            weight: match change.weight {
                Some(weight) => NonZeroU32::new(weight).ok_or(SettingError::ZeroWeight)?,
                None => rule.policy.weight,
            },
        };
        if policy.min > policy.max {
            return Err(SettingError::MinAboveMax {
                min: policy.min,
                max: policy.max,
            });
        }
        let instances = rule.instances.clone();
        let mut changed = self.clone();
        changed
            .rules
            .get_mut(tenant)
            .expect("the rule was found above")
            .policy = policy;
        for instance in instances {
            let minimums = changed.minimums_on(&instance);
            if let Some(budget) = budget(&instance)
                && minimums > budget
            {
                return Err(SettingError::AboveBudget {
                    instance,
                    minimums,
                    budget,
                });
            }
        }
        Ok(changed)
    }

    /// What every rule on `instance` reserves together.
    #[must_use]
    pub fn minimums_on(&self, instance: &InstanceId) -> u32 {
        self.rules
            .values()
            .filter(|rule| rule.instances.contains(instance))
            .map(|rule| rule.policy.min)
            .fold(0, u32::saturating_add)
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
