use std::collections::BTreeMap;
use std::fmt;

use crate::tenant::TenantId;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstanceId(String);

impl InstanceId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProxyId(String);

impl ProxyId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProxyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Holder {
    tenant: TenantId,
    proxy: ProxyId,
}

impl Holder {
    #[must_use]
    pub fn new(tenant: TenantId, proxy: ProxyId) -> Self {
        Self { tenant, proxy }
    }

    #[must_use]
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    #[must_use]
    pub fn proxy(&self) -> &ProxyId {
        &self.proxy
    }
}

/// One instance's desired state: the total budget it is held to, and the slots
/// each holder is granted out of it.
///
/// A holder left out is granted nothing, so an entry carrying this replaces
/// what the instance held rather than adding to it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Desired {
    budget: u32,
    granted: BTreeMap<Holder, u32>,
    total: u64,
}

impl Desired {
    #[must_use]
    pub fn new(budget: u32) -> Self {
        Self {
            budget,
            granted: BTreeMap::new(),
            total: 0,
        }
    }

    #[must_use]
    pub fn grant(mut self, holder: Holder, slots: u32) -> Self {
        let previous = if slots == 0 {
            self.granted.remove(&holder)
        } else {
            self.granted.insert(holder, slots)
        };
        self.total = self.total - u64::from(previous.unwrap_or(0)) + u64::from(slots);
        self
    }

    #[must_use]
    pub fn budget(&self) -> u32 {
        self.budget
    }

    #[must_use]
    pub fn granted(&self, holder: &Holder) -> u32 {
        self.granted.get(holder).copied().unwrap_or(0)
    }

    #[must_use]
    pub fn granted_total(&self) -> u64 {
        self.total
    }

    pub fn holders(&self) -> impl Iterator<Item = (&Holder, u32)> {
        self.granted.iter().map(|(holder, slots)| (holder, *slots))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entry {
    instances: BTreeMap<InstanceId, Desired>,
}

impl Entry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn instance(mut self, instance: InstanceId, desired: Desired) -> Self {
        self.instances.insert(instance, desired);
        self
    }

    pub fn iter(&self) -> impl Iterator<Item = (&InstanceId, &Desired)> {
        self.instances.iter()
    }
}

impl<'a> IntoIterator for &'a Entry {
    type Item = (&'a InstanceId, &'a Desired);
    type IntoIter = std::collections::btree_map::Iter<'a, InstanceId, Desired>;

    fn into_iter(self) -> Self::IntoIter {
        self.instances.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PreconditionError {
    #[error("instance `{instance}` would hold {granted} slots against a total budget of {budget}")]
    OverBudget {
        instance: InstanceId,
        granted: u64,
        budget: u32,
    },
}

/// The desired state of the whole system: instance × tenant × proxy → slots.
///
/// Every write goes through [`AllocationTable::apply`], which holds each
/// instance to "the slots granted never exceed the total budget". An entry that
/// would break it for any instance is rejected whole, so a budget that shrinks
/// has to arrive together with the grants that fit it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AllocationTable {
    instances: BTreeMap<InstanceId, Desired>,
}

impl AllocationTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, entry: &Entry) -> Result<(), PreconditionError> {
        for (instance, desired) in entry {
            if desired.granted_total() > u64::from(desired.budget()) {
                return Err(PreconditionError::OverBudget {
                    instance: instance.clone(),
                    granted: desired.granted_total(),
                    budget: desired.budget(),
                });
            }
        }
        for (instance, desired) in entry {
            self.instances.insert(instance.clone(), desired.clone());
        }
        Ok(())
    }

    #[must_use]
    pub fn budget(&self, instance: &InstanceId) -> u32 {
        self.instances.get(instance).map_or(0, Desired::budget)
    }

    #[must_use]
    pub fn granted(&self, instance: &InstanceId, holder: &Holder) -> u32 {
        self.instances
            .get(instance)
            .map_or(0, |desired| desired.granted(holder))
    }

    #[must_use]
    pub fn granted_total(&self, instance: &InstanceId) -> u32 {
        let total = self
            .instances
            .get(instance)
            .map_or(0, Desired::granted_total);
        u32::try_from(total).unwrap_or(u32::MAX)
    }

    #[must_use]
    pub fn headroom(&self, instance: &InstanceId) -> u32 {
        self.budget(instance)
            .saturating_sub(self.granted_total(instance))
    }

    pub fn instances(&self) -> impl Iterator<Item = &InstanceId> {
        self.instances.keys()
    }

    pub fn holders(&self, instance: &InstanceId) -> impl Iterator<Item = (&Holder, u32)> {
        self.instances
            .get(instance)
            .into_iter()
            .flat_map(Desired::holders)
    }

    pub fn grants_for<'a>(
        &'a self,
        proxy: &'a ProxyId,
    ) -> impl Iterator<Item = (&'a InstanceId, &'a TenantId, u32)> {
        self.instances.iter().flat_map(move |(instance, desired)| {
            desired
                .holders()
                .filter(move |(holder, _)| holder.proxy() == proxy)
                .map(move |(holder, slots)| (instance, holder.tenant(), slots))
        })
    }
}
